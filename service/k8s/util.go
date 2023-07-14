package k8s

import (
	"fmt"
	"crypto/tls"
	"crypto/x509"

	redisfailoverv1 "github.com/spotahome/redis-operator/api/redisfailover/v1"
	"github.com/spotahome/redis-operator/metrics"
	"k8s.io/apimachinery/pkg/api/errors"
)

// GetRedisPassword retreives password from kubernetes secret or, if
// unspecified, returns a blank string
func GetRedisPassword(s Services, rf *redisfailoverv1.RedisFailover) (string, error) {

	if rf.Spec.Auth.SecretPath == "" {
		// no auth settings specified, return blank password
		return "", nil
	}

	secret, err := s.GetSecret(rf.ObjectMeta.Namespace, rf.Spec.Auth.SecretPath)
	if err != nil {
		return "", err
	}

	if password, ok := secret.Data["password"]; ok {
		return string(password), nil
	}

	return "", fmt.Errorf("secret \"%s\" does not have a password field", rf.Spec.Auth.SecretPath)
}

func GetRedisTLSConfig(s Services, rf *redisfailoverv1.RedisFailover) (*tls.Config, error) {
	config := tls.Config{}

	// TLS is not explicitly configured
	if rf.Spec.TLS.SecretName == "" {
		return nil, nil
	}

	certs, err := s.GetSecret(rf.ObjectMeta.Namespace, rf.Spec.TLS.SecretName)
	if err != nil {
		return nil, err
	}

	if cacert, ok := certs.Data["ca.crt"]; ok {
		rootCAPool := x509.NewCertPool()
		if added := rootCAPool.AppendCertsFromPEM(cacert); !added {
			return nil, fmt.Errorf("Unable to append certificate authority to pool")
		}
		config.RootCAs = rootCAPool
	}

	tlscrt, crtOK := certs.Data["tls.crt"]
	tlskey, keyOK := certs.Data["tls.key"]

	if crtOK && keyOK {
		clientCert, err := tls.X509KeyPair(tlscrt, tlskey)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{clientCert}
	}

	return &config, nil
}

func recordMetrics(namespace string, kind string, object string, operation string, err error, metricsRecorder metrics.Recorder) {
	if nil == err {
		metricsRecorder.RecordK8sOperation(namespace, kind, object, operation, metrics.SUCCESS, metrics.NOT_APPLICABLE)
	} else if errors.IsForbidden(err) {
		metricsRecorder.RecordK8sOperation(namespace, kind, object, operation, metrics.FAIL, metrics.K8S_FORBIDDEN_ERR)
	} else if errors.IsUnauthorized(err) {
		metricsRecorder.RecordK8sOperation(namespace, kind, object, operation, metrics.FAIL, metrics.K8S_UNAUTH)
	} else if errors.IsNotFound(err) {
		metricsRecorder.RecordK8sOperation(namespace, kind, object, operation, metrics.FAIL, metrics.K8S_NOT_FOUND)
	} else {
		metricsRecorder.RecordK8sOperation(namespace, kind, object, operation, metrics.FAIL, metrics.K8S_MISC)
	}
}
