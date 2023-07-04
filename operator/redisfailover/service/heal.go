package service

import (
	"errors"
	"sort"
	"strconv"

	redisfailoverv1 "github.com/spotahome/redis-operator/api/redisfailover/v1"
	"github.com/spotahome/redis-operator/log"
	"github.com/spotahome/redis-operator/service/k8s"
	"github.com/spotahome/redis-operator/service/redis"
	v1 "k8s.io/api/core/v1"
)

// RedisFailoverHeal defines the interface able to fix the problems on the redis failovers
type RedisFailoverHeal interface {
	MakeMaster(ip string, rFailover *redisfailoverv1.RedisFailover) error
	SetOldestAsMaster(rFailover *redisfailoverv1.RedisFailover) error
	SetMasterOnAll(masterIP string, rFailover *redisfailoverv1.RedisFailover) error
	SetExternalMasterOnAll(masterIP string, masterPort string, rFailover *redisfailoverv1.RedisFailover) error
	NewSentinelMonitor(ip string, monitor string, rFailover *redisfailoverv1.RedisFailover) error
	NewSentinelMonitorWithPort(ip string, monitor string, port string, rFailover *redisfailoverv1.RedisFailover) error
	RestoreSentinel(ip string) error
	SetSentinelCustomConfig(ip string, rFailover *redisfailoverv1.RedisFailover) error
	SetRedisCustomConfig(ip string, rFailover *redisfailoverv1.RedisFailover) error
	DeletePod(podName string, rFailover *redisfailoverv1.RedisFailover) error
}

// RedisFailoverHealer is our implementation of RedisFailoverCheck interface
type RedisFailoverHealer struct {
	k8sService  k8s.Services
	redisClient redis.Client
	logger      log.Logger
}

// NewRedisFailoverHealer creates an object of the RedisFailoverChecker struct
func NewRedisFailoverHealer(k8sService k8s.Services, redisClient redis.Client, logger log.Logger) *RedisFailoverHealer {
	logger = logger.With("service", "redis.healer")
	return &RedisFailoverHealer{
		k8sService:  k8sService,
		redisClient: redisClient,
		logger:      logger,
	}
}

func (r *RedisFailoverHealer) setMasterLabelIfNecessary(namespace string, pod v1.Pod) error {
	for labelKey, labelValue := range pod.ObjectMeta.Labels {
		if labelKey == redisRoleLabelKey && labelValue == redisRoleLabelMaster {
			return nil
		}
	}
	return r.k8sService.UpdatePodLabels(namespace, pod.ObjectMeta.Name, generateRedisMasterRoleLabel())
}

func (r *RedisFailoverHealer) setSlaveLabelIfNecessary(namespace string, pod v1.Pod) error {
	for labelKey, labelValue := range pod.ObjectMeta.Labels {
		if labelKey == redisRoleLabelKey && labelValue == redisRoleLabelSlave {
			return nil
		}
	}
	return r.k8sService.UpdatePodLabels(namespace, pod.ObjectMeta.Name, generateRedisSlaveRoleLabel())
}

func (r *RedisFailoverHealer) MakeMaster(ip string, rf *redisfailoverv1.RedisFailover) error {
	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	port := getRedisPort(rf.Spec.Redis.Port)
	connectionOptions := redis.NewRedisConnectionOptions(ip, port, password)
	err = r.redisClient.MakeMaster(connectionOptions)
	if err != nil {
		return err
	}

	rps, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return err
	}
	for _, rp := range rps.Items {
		if rp.Status.PodIP == ip {
			return r.setMasterLabelIfNecessary(rf.Namespace, rp)
		}
	}
	return nil
}

// SetOldestAsMaster puts all redis to the same master, choosen by order of appearance
func (r *RedisFailoverHealer) SetOldestAsMaster(rf *redisfailoverv1.RedisFailover) error {
	ssp, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return err
	}
	if len(ssp.Items) < 1 {
		return errors.New("number of redis pods are 0")
	}

	// Order the pods so we start by the oldest one
	sort.Slice(ssp.Items, func(i, j int) bool {
		return ssp.Items[i].CreationTimestamp.Before(&ssp.Items[j].CreationTimestamp)
	})

	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	port := getRedisPort(rf.Spec.Redis.Port)
	newMasterIP := ""
	for _, pod := range ssp.Items {
		if newMasterIP == "" {
			newMasterIP = pod.Status.PodIP
			r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Infof("New master is %s with ip %s", pod.Name, newMasterIP)

			connectionOptions := redis.NewRedisConnectionOptions(newMasterIP, port, password)
			if err := r.redisClient.MakeMaster(connectionOptions); err != nil {
				newMasterIP = ""
				r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Errorf("Make new master failed, master ip: %s, error: %v", pod.Status.PodIP, err)
				continue
			}

			err = r.setMasterLabelIfNecessary(rf.Namespace, pod)
			if err != nil {
				return err
			}

			newMasterIP = pod.Status.PodIP
		} else {
			r.logger.Infof("Making pod %s slave of %s", pod.Name, newMasterIP)

			connectionOptions := redis.NewRedisConnectionOptions(pod.Status.PodIP, port, password)
			if err := r.redisClient.MakeSlaveOfWithPort(connectionOptions, newMasterIP, port); err != nil {
				r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Errorf("Make slave failed, slave pod ip: %s, master ip: %s, error: %v", pod.Status.PodIP, newMasterIP, err)
			}

			err = r.setSlaveLabelIfNecessary(rf.Namespace, pod)
			if err != nil {
				return err
			}
		}
	}
	if newMasterIP == "" {
		return errors.New("SetOldestAsMaster- unable to set master")
	} else {
		return nil
	}
}

// SetMasterOnAll puts all redis nodes as a slave of a given master
func (r *RedisFailoverHealer) SetMasterOnAll(masterIP string, rf *redisfailoverv1.RedisFailover) error {
	ssp, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return err
	}

	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	port := getRedisPort(rf.Spec.Redis.Port)
	for _, pod := range ssp.Items {
		//During this configuration process if there is a new master selected , bailout
		connectionOptions := redis.NewRedisConnectionOptions(masterIP, port, password)
		isMaster, err := r.redisClient.IsMaster(connectionOptions)
		if err != nil || !isMaster {
			r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Errorf("check master failed maybe this node is not ready(ip changed), or sentinel made a switch: %s", masterIP)
			return err
		} else {
			if pod.Status.PodIP == masterIP {
				continue
			}
			r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Infof("Making pod %s slave of %s", pod.Name, masterIP)
			connectionOptions = redis.NewRedisConnectionOptions(pod.Status.PodIP, port, password)
			if err := r.redisClient.MakeSlaveOfWithPort(connectionOptions, masterIP, port); err != nil {
				r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Errorf("Make slave failed, slave ip: %s, master ip: %s, error: %v", pod.Status.PodIP, masterIP, err)
				return err
			}

			err = r.setSlaveLabelIfNecessary(rf.Namespace, pod)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// SetExternalMasterOnAll puts all redis nodes as a slave of a given master outside of
// the current RedisFailover instance
func (r *RedisFailoverHealer) SetExternalMasterOnAll(masterIP, masterPort string, rf *redisfailoverv1.RedisFailover) error {
	ssp, err := r.k8sService.GetStatefulSetPods(rf.Namespace, GetRedisName(rf))
	if err != nil {
		return err
	}

	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	for _, pod := range ssp.Items {
		r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Infof("Making pod %s slave of %s:%s", pod.Name, masterIP, masterPort)
		connectionOptions := redis.NewRedisConnectionOptions(pod.Status.PodIP, masterPort, password)
		if err := r.redisClient.MakeSlaveOfWithPort(connectionOptions, masterIP, masterPort); err != nil {
			return err
		}

	}
	return nil
}

// NewSentinelMonitor changes the master that Sentinel has to monitor
func (r *RedisFailoverHealer) NewSentinelMonitor(ip string, monitor string, rf *redisfailoverv1.RedisFailover) error {
	quorum := strconv.Itoa(int(getQuorum(rf)))

	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	port := getRedisPort(rf.Spec.Redis.Port)
	connectionOptions := redis.NewSentinelConnectionOptions(ip)
	return r.redisClient.MonitorRedisWithPort(connectionOptions, monitor, port, quorum, password)
}

// NewSentinelMonitorWithPort changes the master that Sentinel has to monitor by the provided IP and Port
func (r *RedisFailoverHealer) NewSentinelMonitorWithPort(ip string, monitor string, monitorPort string, rf *redisfailoverv1.RedisFailover) error {
	quorum := strconv.Itoa(int(getQuorum(rf)))

	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	connectionOptions := redis.NewSentinelConnectionOptions(ip)
	return r.redisClient.MonitorRedisWithPort(connectionOptions, monitor, monitorPort, quorum, password)
}

// RestoreSentinel clear the number of sentinels on memory
func (r *RedisFailoverHealer) RestoreSentinel(ip string) error {
	r.logger.Debugf("Restoring sentinel %s", ip)
	connectionOptions := redis.NewSentinelConnectionOptions(ip)
	return r.redisClient.ResetSentinel(connectionOptions)
}

// SetSentinelCustomConfig will call sentinel to set the configuration given in config
func (r *RedisFailoverHealer) SetSentinelCustomConfig(ip string, rf *redisfailoverv1.RedisFailover) error {
	r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Debugf("Setting the custom config on sentinel %s...", ip)
	connectionOptions := redis.NewSentinelConnectionOptions(ip)
	return r.redisClient.SetCustomSentinelConfig(connectionOptions, rf.Spec.Sentinel.CustomConfig)
}

// SetRedisCustomConfig will call redis to set the configuration given in config
func (r *RedisFailoverHealer) SetRedisCustomConfig(ip string, rf *redisfailoverv1.RedisFailover) error {
	r.logger.WithField("redisfailover", rf.ObjectMeta.Name).WithField("namespace", rf.ObjectMeta.Namespace).Debugf("Setting the custom config on redis %s...", ip)

	password, err := k8s.GetRedisPassword(r.k8sService, rf)
	if err != nil {
		return err
	}

	port := getRedisPort(rf.Spec.Redis.Port)
	connectionOptions := redis.NewRedisConnectionOptions(ip, port, password)
	return r.redisClient.SetCustomRedisConfig(connectionOptions, rf.Spec.Redis.CustomConfig)
}

// DeletePod delete a failing pod so kubernetes relaunch it again
func (r *RedisFailoverHealer) DeletePod(podName string, rFailover *redisfailoverv1.RedisFailover) error {
	r.logger.WithField("redisfailover", rFailover.ObjectMeta.Name).WithField("namespace", rFailover.ObjectMeta.Namespace).Infof("Deleting pods %s...", podName)
	return r.k8sService.DeletePod(rFailover.Namespace, podName)
}
