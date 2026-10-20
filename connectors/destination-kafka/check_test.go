package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func TestCheckConnectsWithoutTestTopic(t *testing.T) {
	c := startCluster(t)
	status := runCheck(t, Destination{}, plainConfig(c, ""))
	assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
}

func TestCheckProducesToTestTopic(t *testing.T) {
	c := startCluster(t)
	status := runCheck(t, Destination{}, plainConfig(c, `"test_topic":"check.topic"`))
	require.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
	msgs := consume(t, bootstrap(c), "check.topic", 1)
	assert.Equal(t, checkStream, msgs[0].Value.Stream)
	assert.Equal(t, msgs[0].Value.ID, msgs[0].Key)
}

func TestCheckUnreachableBroker(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	status := runCheck(t, Destination{}, fmt.Sprintf(`{"bootstrap_servers":%q,"topic_pattern":"t"}`, addr))
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "connect to "+addr)
}

func TestCheckTestTopicProduceFails(t *testing.T) {
	c := startCluster(t)
	failProduces(c, 29) // TOPIC_AUTHORIZATION_FAILED
	status := runCheck(t, Destination{}, plainConfig(c, `"test_topic":"check"`))
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "produce to test topic check")
}

func TestCheckInvalidConfig(t *testing.T) {
	status := runCheck(t, Destination{}, `{"bootstrap_servers":"","topic_pattern":"t"}`)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "bootstrap_servers is empty")
}

func saslConfig(c *kfake.Cluster, protocol, mechanism, credentials string) string {
	return plainConfig(c, fmt.Sprintf(`"test_topic":"check","protocol":{"security_protocol":%q,"sasl_mechanism":%q,%s}`,
		protocol, mechanism, credentials))
}

func TestCheckSASLPlaintext(t *testing.T) {
	for _, mech := range []string{mechanismPlain, mechanismSCRAMSHA256, mechanismSCRAMSHA512} {
		t.Run(mech, func(t *testing.T) {
			c := startCluster(t, kfake.EnableSASL(), kfake.Superuser(mech, "synclet", "s3cret"))

			status := runCheck(t, Destination{}, saslConfig(c, protocolSASLPlaintext, mech, `"sasl_username":"synclet","sasl_password":"s3cret"`))
			assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)

			status = runCheck(t, Destination{}, saslConfig(c, protocolSASLPlaintext, mech,
				`"sasl_jaas_config":"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"synclet\" password=\"s3cret\";"`))
			assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)

			status = runCheck(t, Destination{}, saslConfig(c, protocolSASLPlaintext, mech, `"sasl_username":"synclet","sasl_password":"wrong"`))
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
		})
	}
}

func TestCheckPlaintextAgainstSASLClusterFails(t *testing.T) {
	c := startCluster(t, kfake.EnableSASL(), kfake.Superuser(mechanismPlain, "synclet", "s3cret"))
	status := runCheck(t, Destination{}, plainConfig(c, `"test_topic":"check"`))
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
}

func TestSASLSSLCheckAndWrite(t *testing.T) {
	serverTLS, roots := selfSignedTLS(t)
	c := startCluster(t, kfake.TLS(serverTLS), kfake.EnableSASL(), kfake.Superuser(mechanismSCRAMSHA512, "synclet", "s3cret"))
	d := Destination{rootCAs: roots}
	cfg := saslConfig(c, protocolSASLSSL, mechanismSCRAMSHA512, `"sasl_username":"synclet","sasl_password":"s3cret"`)

	status := runCheck(t, d, cfg)
	require.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)

	out := runWrite(t, d, cfg, catalog(t, "app.users:id"), rec("app", "users", `{"id":1}`), state("users", 1))
	require.NoError(t, out.Err)
	require.Len(t, out.States(), 1)
	msgs := consume(t, bootstrap(c), "app_users", 1,
		kgo.DialTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{User: "synclet", Pass: "s3cret"}.AsSha512Mechanism()))
	assert.Equal(t, "1", msgs[0].Key)

	status = runCheck(t, Destination{}, cfg)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status, "the system roots do not trust the test certificate")

	status = runCheck(t, d, saslConfig(c, protocolSASLPlaintext, mechanismSCRAMSHA512, `"sasl_username":"synclet","sasl_password":"s3cret"`))
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status, "SASL_PLAINTEXT does not speak TLS")
}

func selfSignedTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kfake"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}},
		MinVersion:   tls.VersionTLS12,
	}, roots
}
