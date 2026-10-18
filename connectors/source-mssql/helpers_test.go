package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/ssh"
)

const (
	msImage      = "mcr.microsoft.com/mssql/server:2022-latest"
	msSAPassword = "Synclet-SA-Test1!"
	msDatabase   = "synclet"
	msUser       = "synclet"
	msPassword   = "Synclet-Test1!"
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
		t.Skip("starts SQL Server in Docker; skipped under -short")
	}
}

// runMSSQL starts a SQL Server container loaded with testdata/fixture.sql
// and returns a configuration that reaches it as the synclet login.
func runMSSQL() (Config, error) {
	ctx := context.Background()
	ctr, err := mssql.Run(ctx, msImage,
		mssql.WithAcceptEULA(),
		mssql.WithPassword(msSAPassword),
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithWaitStrategyAndDeadline(5*time.Minute,
			wait.ForListeningPort("1433/tcp"),
			wait.ForLog("Recovery is complete."),
		),
	)
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
	port, err := ctr.MappedPort(ctx, "1433/tcp")
	if err != nil {
		return Config{}, err
	}
	sa := Config{Host: host, Port: int(port.Num()), Database: "master", Username: "sa", Password: msSAPassword}
	if err := loadFixture(ctx, sa, filepath.Join("testdata", "fixture.sql")); err != nil {
		return Config{}, err
	}
	cfg := sa
	cfg.Database, cfg.Username, cfg.Password = msDatabase, msUser, msPassword
	return cfg, nil
}

// loadFixture runs the batches of a script, split at GO lines, as cfg's
// login once the server accepts it.
func loadFixture(ctx context.Context, cfg Config, path string) error {
	var d *db
	var err error
	for deadline := time.Now().Add(2 * time.Minute); ; {
		if d, err = connect(ctx, cfg); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("connect as sa: %w", err)
		}
		time.Sleep(2 * time.Second)
	}
	defer d.close()
	conn, err := d.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var batch strings.Builder
	run := func() error {
		if strings.TrimSpace(batch.String()) == "" {
			return nil
		}
		_, err := conn.ExecContext(ctx, batch.String())
		batch.Reset()
		return err
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.EqualFold(strings.TrimSpace(sc.Text()), "GO") {
			if err := run(); err != nil {
				return err
			}
			continue
		}
		batch.WriteString(sc.Text())
		batch.WriteByte('\n')
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return run()
}

// sharedMSSQL is one container every test reads from. A start failure on
// an arm64 host skips: the SQL Server image is amd64-only and runs there
// only under emulation.
func sharedMSSQL(t *testing.T) Config {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { sharedCfg, sharedErr = runMSSQL() })
	if sharedErr != nil && runtime.GOARCH == "arm64" {
		t.Skipf("SQL Server image is amd64-only and failed under emulation: %v", sharedErr)
	}
	require.NoError(t, sharedErr)
	return sharedCfg
}

func execSQL(t *testing.T, cfg Config, query string) {
	t.Helper()
	ctx := context.Background()
	d, err := connect(ctx, cfg)
	require.NoError(t, err)
	defer d.close()
	_, err = d.ExecContext(ctx, query)
	require.NoError(t, err)
}

func queryString(t *testing.T, cfg Config, query string) string {
	t.Helper()
	d, err := connect(t.Context(), cfg)
	require.NoError(t, err)
	defer d.close()
	var s string
	require.NoError(t, d.QueryRowContext(t.Context(), query).Scan(&s))
	return s
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

// sshServer is an in-process SSH server with an Ed25519 and an ECDSA host
// key that serves direct-tcpip channels,
// dialing the address routes maps a requested address to.
type sshServer struct {
	addr     string
	hostKey  ssh.PublicKey
	ecdsaKey ssh.PublicKey
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
	ecdsaPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ecdsaHostKey, err := ssh.NewSignerFromKey(ecdsaPriv)
	require.NoError(t, err)
	s := &sshServer{hostKey: hostKey.PublicKey(), ecdsaKey: ecdsaHostKey.PublicKey(), password: password, key: key, routes: routes}
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
	cfg.AddHostKey(ecdsaHostKey)
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
