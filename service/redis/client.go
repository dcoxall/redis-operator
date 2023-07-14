package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"regexp"
	"strings"

	rediscli "github.com/redis/go-redis/v9"
	"github.com/spotahome/redis-operator/log"
	"github.com/spotahome/redis-operator/metrics"
)

type RedisConnectionOptions interface {
	GetRedisClient() *rediscli.Client
	GetIP() string
}

type SentinelConnectionOptions interface {
	GetSentinelClient() *rediscli.SentinelClient
	GetIP() string
}

type connectionOpts struct {
	ip         string
	port       string
	password   string
	tlsConfig  *tls.Config
}

func NewRedisConnectionOptions(ip, port, password string, tlsConfig *tls.Config) RedisConnectionOptions {
	return &connectionOpts{
		ip:        ip,
		port:      port,
		password:  password,
		tlsConfig: tlsConfig,
	}
}

func NewSentinelConnectionOptions(ip string, tlsConfig *tls.Config) SentinelConnectionOptions {
	return &connectionOpts{
		ip:        ip,
		port:      sentinelPort,
		tlsConfig: tlsConfig,
	}
}

func (connOpts *connectionOpts) GetSentinelClient() *rediscli.SentinelClient {
	options := &rediscli.Options{
		Addr:      net.JoinHostPort(connOpts.ip, sentinelPort),
		TLSConfig: connOpts.tlsConfig,
		Password:  "",
		DB:        0,
	}
	return rediscli.NewSentinelClient(options)
}

func (connOpts *connectionOpts) GetRedisClient() *rediscli.Client {
	options := &rediscli.Options{
		Addr:      net.JoinHostPort(connOpts.ip, connOpts.port),
		TLSConfig: connOpts.tlsConfig,
		Password:  connOpts.password,
		DB:        0,
	}
	return rediscli.NewClient(options)
}

func (connOpts *connectionOpts) GetIP() string {
	return connOpts.ip
}

// Client defines the functions neccesary to connect to redis and sentinel to get or set what we need
type Client interface {
	GetNumberSentinelsInMemory(connOpts SentinelConnectionOptions) (int32, error)
	GetNumberSentinelSlavesInMemory(connOpts SentinelConnectionOptions) (int32, error)
	ResetSentinel(connOpts SentinelConnectionOptions) error
	GetSlaveOf(connOpts RedisConnectionOptions) (string, error)
	IsMaster(connOpts RedisConnectionOptions) (bool, error)
	MonitorRedis(connOpts SentinelConnectionOptions, monitor, quorum, password string) error
	MonitorRedisWithPort(connOpts SentinelConnectionOptions, monitor, port, quorum, password string) error
	MakeMaster(connOpts RedisConnectionOptions) error
	MakeSlaveOf(connOpts RedisConnectionOptions, masterIP string) error
	MakeSlaveOfWithPort(connOpts RedisConnectionOptions, masterIP, masterPort string) error
	GetSentinelMonitor(connOpts SentinelConnectionOptions) (string, string, error)
	SetCustomSentinelConfig(connOpts SentinelConnectionOptions, configs []string) error
	SetCustomRedisConfig(connOpts RedisConnectionOptions, configs []string) error
	SlaveIsReady(connOpts RedisConnectionOptions) (bool, error)
	SentinelCheckQuorum(connOpts SentinelConnectionOptions) error
}

type client struct {
	metricsRecorder metrics.Recorder
}

// New returns a redis client
func New(metricsRecorder metrics.Recorder) Client {
	return &client{
		metricsRecorder: metricsRecorder,
	}
}

const (
	sentinelsNumberREString = "sentinels=([0-9]+)"
	slaveNumberREString     = "slaves=([0-9]+)"
	sentinelStatusREString  = "status=([a-z]+)"
	redisMasterHostREString = "master_host:([0-9.]+)"
	redisRoleMaster         = "role:master"
	redisSyncing            = "master_sync_in_progress:1"
	redisMasterSillPending  = "master_host:127.0.0.1"
	redisLinkUp             = "master_link_status:up"
	redisPort               = "6379"
	sentinelPort            = "26379"
	masterName              = "mymaster"
)

var (
	sentinelNumberRE  = regexp.MustCompile(sentinelsNumberREString)
	sentinelStatusRE  = regexp.MustCompile(sentinelStatusREString)
	slaveNumberRE     = regexp.MustCompile(slaveNumberREString)
	redisMasterHostRE = regexp.MustCompile(redisMasterHostREString)
)

// GetNumberSentinelsInMemory return the number of sentinels that the requested sentinel has
func (c *client) GetNumberSentinelsInMemory(connOpts SentinelConnectionOptions) (int32, error) {
	ip := connOpts.GetIP()
	rClient := connOpts.GetSentinelClient()
	defer rClient.Close()

	sentinelMap, err := rClient.Sentinels(context.TODO(), masterName).Result()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.GET_NUM_SENTINELS_IN_MEM, metrics.FAIL, getRedisError(err))
		return 0, err
	}
	return int32(len(sentinelMap)), nil
}

// GetNumberSentinelsInMemory return the number of slaves that the requested sentinel has
func (c *client) GetNumberSentinelSlavesInMemory(connOpts SentinelConnectionOptions) (int32, error) {
	ip := connOpts.GetIP()
	rClient := connOpts.GetSentinelClient()
	defer rClient.Close()

	replicas, err := rClient.Replicas(context.TODO(), masterName).Result()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.GET_NUM_REDIS_SLAVES_IN_MEM, metrics.FAIL, getRedisError(err))
		return 0, err
	}

	var availableReplicas int32
	for _, node := range replicas {
		isDown := false
		if flags, ok := node["flags"]; ok {
			for _, flag := range strings.Split(flags, ",") {
				switch flag {
				case "s_down", "o_down", "disconnected":
					isDown = true
				}
			}
		}
		if !isDown && node["ip"] != "" && node["port"] != "" {
			availableReplicas++
		}
	}

	return availableReplicas, nil
}

// ResetSentinel sends a sentinel reset * for the given sentinel
func (c *client) ResetSentinel(connOpts SentinelConnectionOptions) error {
	ip := connOpts.GetIP()
	rClient := connOpts.GetSentinelClient()
	defer rClient.Close()

	err := rClient.Reset(context.TODO(), "*").Err()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.RESET_SENTINEL, metrics.FAIL, getRedisError(err))
		return err
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.RESET_SENTINEL, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return nil
}

// GetSlaveOf returns the master of the given redis, or nil if it's master
func (c *client) GetSlaveOf(connOpts RedisConnectionOptions) (string, error) {
	ip := connOpts.GetIP()
	rClient := connOpts.GetRedisClient()
	defer rClient.Close()
	info, err := rClient.Info(context.TODO(), "replication").Result()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.GET_SLAVE_OF, metrics.FAIL, getRedisError(err))
		log.Errorf("error while getting masterIP : Failed to get info replication while querying redis instance %v", ip)
		return "", err
	}
	match := redisMasterHostRE.FindStringSubmatch(info)
	if len(match) == 0 {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.GET_SLAVE_OF, metrics.SUCCESS, metrics.NOT_APPLICABLE)
		return "", nil
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.GET_SLAVE_OF, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return match[1], nil
}

func (c *client) IsMaster(connOpts RedisConnectionOptions) (bool, error) {
	ip := connOpts.GetIP()
	rClient := connOpts.GetRedisClient()
	defer rClient.Close()
	info, err := rClient.Info(context.TODO(), "replication").Result()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.IS_MASTER, metrics.FAIL, getRedisError(err))
		return false, err
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.IS_MASTER, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return strings.Contains(info, redisRoleMaster), nil
}

func (c *client) MonitorRedis(connOpts SentinelConnectionOptions, monitor, quorum, password string) error {
	return c.MonitorRedisWithPort(connOpts, monitor, redisPort, quorum, password)
}

func (c *client) MonitorRedisWithPort(connOpts SentinelConnectionOptions, monitor, port, quorum, password string) error {
	ip := connOpts.GetIP()
	rClient := connOpts.GetSentinelClient()
	defer rClient.Close()
	rClient.Remove(context.TODO(), masterName).Result()
	// We'll continue even if it fails, the priority is to have the redises monitored
	err := rClient.Monitor(context.TODO(), masterName, monitor, port, quorum).Err()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MONITOR_REDIS_WITH_PORT, metrics.FAIL, getRedisError(err))
		return err
	}

	if password != "" {
		err := rClient.Set(context.TODO(), masterName, "auth-pass", password).Err()
		if err != nil {
			c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MONITOR_REDIS_WITH_PORT, metrics.FAIL, getRedisError(err))
			return err
		}
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MONITOR_REDIS_WITH_PORT, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return nil
}

func (c *client) MakeMaster(connOpts RedisConnectionOptions) error {
	ip := connOpts.GetIP()
	rClient := connOpts.GetRedisClient()
	defer rClient.Close()
	if res := rClient.SlaveOf(context.TODO(), "NO", "ONE"); res.Err() != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MAKE_MASTER, metrics.FAIL, getRedisError(res.Err()))
		return res.Err()
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MAKE_MASTER, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return nil
}

func (c *client) MakeSlaveOf(connOpts RedisConnectionOptions, masterIP string) error {
	return c.MakeSlaveOfWithPort(connOpts, masterIP, redisPort)
}

func (c *client) MakeSlaveOfWithPort(connOpts RedisConnectionOptions, masterIP, masterPort string) error {
	rClient := connOpts.GetRedisClient()
	defer rClient.Close()
	ip, _, _ := net.SplitHostPort(rClient.Options().Addr)
	if res := rClient.SlaveOf(context.TODO(), masterIP, masterPort); res.Err() != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MAKE_SLAVE_OF, metrics.FAIL, getRedisError(res.Err()))
		return res.Err()
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.MAKE_SLAVE_OF, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return nil
}

func (c *client) GetSentinelMonitor(connOpts SentinelConnectionOptions) (string, string, error) {
	ip := connOpts.GetIP()
	rClient := connOpts.GetSentinelClient()
	defer rClient.Close()
	res, err := rClient.Master(context.TODO(), masterName).Result()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.GET_SENTINEL_MONITOR, metrics.FAIL, getRedisError(err))
		return "", "", err
	}
	masterIP := res["ip"]
	masterPort := res["port"]
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.GET_SENTINEL_MONITOR, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return masterIP, masterPort, nil
}

func (c *client) SetCustomSentinelConfig(connOpts SentinelConnectionOptions, configs []string) error {
	ip := connOpts.GetIP()
	rClient := connOpts.GetSentinelClient()
	defer rClient.Close()

	for _, config := range configs {
		param, value, err := c.getConfigParameters(config)
		if err != nil {
			return err
		}
		if err := c.applySentinelConfig(param, value, rClient, ip); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) SentinelCheckQuorum(connOpts SentinelConnectionOptions) error {
	ip := connOpts.GetIP()
	rClient := connOpts.GetSentinelClient()
	defer rClient.Close()

	res, err := rClient.CkQuorum(context.TODO(), masterName).Result()
	if err != nil {
		log.Warnf("Unable to get result for CKQUORUM comand")
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.CHECK_SENTINEL_QUORUM, metrics.FAIL, getRedisError(err))
		return err
	}
	log.Debugf("SentinelCheckQuorum cmd result: %s", res)
	s := strings.Split(res, " ")
	status := s[0]
	quorum := s[1]

	if status == "" {
		log.Errorf("quorum command result unexpected output")
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.CHECK_SENTINEL_QUORUM, metrics.FAIL, "quorum command result unexpected output")
		return fmt.Errorf("quorum command result unexpected output")
	}
	if status == "(error)" && quorum == "NOQUORUM" {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.CHECK_SENTINEL_QUORUM, metrics.SUCCESS, "NOQUORUM")
		return fmt.Errorf("quorum Not available")

	} else if status == "OK" {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.CHECK_SENTINEL_QUORUM, metrics.SUCCESS, "QUORUM")
		return nil
	} else {
		log.Errorf("quorum command status unexpected !!!")
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.CHECK_SENTINEL_QUORUM, metrics.FAIL, "quorum command status unexpected output")
		return fmt.Errorf("quorum status unexpected %s", status)
	}
}

func (c *client) SetCustomRedisConfig(connOpts RedisConnectionOptions, configs []string) error {
	ip := connOpts.GetIP()
	rClient := connOpts.GetRedisClient()
	defer rClient.Close()

	for _, config := range configs {
		param, value, err := c.getConfigParameters(config)
		if err != nil {
			return err
		}
		// If the configuration is an empty line , it will result in an incorrect configSet, which will not run properly down the line.
		// `config set save ""` should support
		if strings.TrimSpace(param) == "" {
			continue
		}
		if err := c.applyRedisConfig(param, value, rClient, ip); err != nil {
			return err
		}
	}
	return nil
}

func (c *client) applyRedisConfig(parameter string, value string, rClient *rediscli.Client, ip string) error {
	result := rClient.ConfigSet(context.TODO(), parameter, value)
	if nil != result.Err() {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.APPLY_REDIS_CONFIG, metrics.FAIL, getRedisError(result.Err()))
		return result.Err()
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.APPLY_REDIS_CONFIG, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return result.Err()
}

func (c *client) applySentinelConfig(parameter string, value string, rClient *rediscli.SentinelClient, ip string) error {
	err := rClient.Set(context.TODO(), masterName, parameter, value).Err()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.APPLY_SENTINEL_CONFIG, metrics.FAIL, getRedisError(err))
		return err
	}
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_SENTINEL, ip, metrics.APPLY_SENTINEL_CONFIG, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return nil
}

func (c *client) getConfigParameters(config string) (parameter string, value string, err error) {
	s := strings.Split(config, " ")
	if len(s) < 2 {
		return "", "", fmt.Errorf("configuration '%s' malformed", config)
	}
	if len(s) == 2 && s[1] == `""` {
		return s[0], "", nil
	}
	return s[0], strings.Join(s[1:], " "), nil
}

func (c *client) SlaveIsReady(connOpts RedisConnectionOptions) (bool, error) {
	ip := connOpts.GetIP()
	rClient := connOpts.GetRedisClient()
	defer rClient.Close()
	info, err := rClient.Info(context.TODO(), "replication").Result()
	if err != nil {
		c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.SLAVE_IS_READY, metrics.FAIL, getRedisError(err))
		return false, err
	}

	ok := !strings.Contains(info, redisSyncing) &&
		!strings.Contains(info, redisMasterSillPending) &&
		strings.Contains(info, redisLinkUp)
	c.metricsRecorder.RecordRedisOperation(metrics.KIND_REDIS, ip, metrics.SLAVE_IS_READY, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	return ok, nil
}

func getRedisError(err error) string {
	if strings.Contains(err.Error(), "NOAUTH") {
		return metrics.NOAUTH
	} else if strings.Contains(err.Error(), "WRONGPASS") {
		return metrics.WRONG_PASSWORD_USED
	} else if strings.Contains(err.Error(), "NOPERM") {
		return metrics.NOPERM
	} else if strings.Contains(err.Error(), "i/o timeout") {
		return metrics.IO_TIMEOUT
	} else if strings.Contains(err.Error(), "connection refused") {
		return metrics.CONNECTION_REFUSED
	} else {
		return "MISC"
	}
}
