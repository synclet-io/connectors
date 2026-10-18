package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"golang.org/x/crypto/ssh"
)

const (
	pgImage    = "postgres:16-alpine"
	pgDatabase = "synclet"
	pgUser     = "synclet"
	pgPassword = "synclet-test"
)

var (
	containersMu sync.Mutex
	containers   []testcontainers.Container
	sharedOnce   sync.Once
	sharedCfg    Config
	sharedErr    error
)

func TestMain(m *testing.M) {
	code := m.Run()
	containersMu.Lock()
	for _, c := range containers {
		_ = testcontainers.TerminateContainer(c)
	}
	containersMu.Unlock()
	os.Exit(code)
}

func needDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts Postgres in Docker; skipped under -short")
	}
}

// runPostgres starts a postgres:16 container loaded with testdata/fixture.sql
// and returns a configuration that reaches it without SSL.
func runPostgres(opts ...testcontainers.ContainerCustomizer) (Config, error) {
	ctx := context.Background()
	opts = append([]testcontainers.ContainerCustomizer{
		postgres.WithInitScripts(filepath.Join("testdata", "fixture.sql")),
		postgres.WithDatabase(pgDatabase),
		postgres.WithUsername(pgUser),
		postgres.WithPassword(pgPassword),
		postgres.BasicWaitStrategies(),
	}, opts...)
	ctr, err := postgres.Run(ctx, pgImage, opts...)
	if ctr != nil {
		containersMu.Lock()
		containers = append(containers, ctr)
		containersMu.Unlock()
	}
	if err != nil {
		return Config{}, err
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return Config{}, err
	}
	port, err := ctr.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return Config{}, err
	}
	return Config{
		Host:     host,
		Port:     int(port.Num()),
		Database: pgDatabase,
		Username: pgUser,
		Password: pgPassword,
		SSLMode:  SSLMode{Mode: sslDisable},
	}, nil
}

// sharedPostgres is one container every test that does not need its own
// server setup reads from.
func sharedPostgres(t *testing.T) Config {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedCfg, sharedErr = runPostgres() })
	require.NoError(t, sharedErr)
	return sharedCfg
}

func execSQL(t *testing.T, cfg Config, sql string) {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	_, err = d.Exec(ctx, sql)
	require.NoError(t, err)
}

func discoverStream(t *testing.T, cfg Config, namespace, name string) synclet.Stream {
	t.Helper()
	cat, err := Source{}.Discover(t.Context(), cfg)
	require.NoError(t, err)
	for _, s := range cat.Streams {
		if s.Namespace == namespace && s.Name == name {
			return s
		}
	}
	t.Fatalf("discover: no stream %s.%s", namespace, name)
	return synclet.Stream{}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func runRead(t *testing.T, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{
		Command: synclet.CommandRead,
		Config:  mustJSON(t, cfg),
		Catalog: mustJSON(t, cat),
		State:   state,
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	return out
}

// testCA is a certificate authority generated for one test.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM string
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "synclet test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{cert: cert, key: key, certPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

// serverCert issues a server certificate for the host name localhost and
// returns it and its key as PEM.
func (ca *testCA) serverCert(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

// sshServer is an in-process SSH server that serves direct-tcpip channels,
// dialing the address routes maps a requested address to.
type sshServer struct {
	addr     string
	password string
	key      ssh.PublicKey
	routes   map[string]string
}

func startSSHServer(t *testing.T, password string, key ssh.PublicKey, routes map[string]string) *sshServer {
	t.Helper()
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	hostKey, err := ssh.NewSignerFromKey(hostPriv)
	require.NoError(t, err)
	s := &sshServer{password: password, key: key, routes: routes}
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, p []byte) (*ssh.Permissions, error) {
			if s.password != "" && string(p) == s.password {
				return nil, nil
			}
			return nil, os.ErrPermission
		},
		PublicKeyCallback: func(_ ssh.ConnMetadata, k ssh.PublicKey) (*ssh.Permissions, error) {
			if s.key != nil && string(k.Marshal()) == string(s.key.Marshal()) {
				return nil, nil
			}
			return nil, os.ErrPermission
		},
	}
	cfg.AddHostKey(hostKey)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	s.addr = l.Addr().String()
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			go s.serve(nc, cfg)
		}
	}()
	return s
}

func (s *sshServer) hostPort(t *testing.T) (string, int) {
	t.Helper()
	host, port, err := net.SplitHostPort(s.addr)
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	return host, p
}

func (s *sshServer) serve(nc net.Conn, cfg *ssh.ServerConfig) {
	conn, chans, reqs, err := ssh.NewServerConn(nc, cfg)
	if err != nil {
		_ = nc.Close()
		return
	}
	defer func() { _ = conn.Close() }()
	go ssh.DiscardRequests(reqs)
	for nch := range chans {
		if nch.ChannelType() != "direct-tcpip" {
			_ = nch.Reject(ssh.UnknownChannelType, "only direct-tcpip")
			continue
		}
		go s.forward(nch)
	}
}

func (s *sshServer) forward(nch ssh.NewChannel) {
	var req struct {
		Host     string
		Port     uint32
		OrigHost string
		OrigPort uint32
	}
	if err := ssh.Unmarshal(nch.ExtraData(), &req); err != nil {
		_ = nch.Reject(ssh.ConnectionFailed, err.Error())
		return
	}
	target := net.JoinHostPort(req.Host, strconv.Itoa(int(req.Port)))
	if r, ok := s.routes[target]; ok {
		target = r
	}
	up, err := net.Dial("tcp", target)
	if err != nil {
		_ = nch.Reject(ssh.ConnectionFailed, err.Error())
		return
	}
	ch, chReqs, err := nch.Accept()
	if err != nil {
		_ = up.Close()
		return
	}
	go ssh.DiscardRequests(chReqs)
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(ch, up); _ = ch.CloseWrite(); done <- struct{}{} }()
	go func() { _, _ = io.Copy(up, ch); done <- struct{}{} }()
	<-done
	_ = ch.Close()
	_ = up.Close()
}
