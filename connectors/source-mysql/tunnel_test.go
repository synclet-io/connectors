package main

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

func TestSSL(t *testing.T) {
	cfg := sharedMySQL(t)
	cipher := func(c Config) string {
		d, err := connect(t.Context(), c)
		require.NoError(t, err)
		defer func() { require.NoError(t, d.close()) }()
		var name, value string
		require.NoError(t, d.QueryRowContext(t.Context(), "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &value))
		return value
	}
	for _, mode := range []string{sslPreferred, sslRequired} {
		c := cfg
		c.SSLMode = SSLMode{Mode: mode}
		require.NotEmpty(t, cipher(c), "ssl_mode %s encrypts with a server that has TLS", mode)
	}

	other := newTestCA(t)
	for _, mode := range []string{sslVerifyCA, sslVerifyIdentity} {
		c := cfg
		c.SSLMode = SSLMode{Mode: mode, CACertificate: other.certPEM}
		require.ErrorContains(t, Source{}.Check(t.Context(), c), "is not trusted (ssl_mode "+mode+")")
	}
}

func TestSSHTunnel(t *testing.T) {
	cfg := sharedMySQL(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sshPub, err := ssh.NewPublicKey(pub)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(priv, "")
	require.NoError(t, err)
	keyPEM := string(pem.EncodeToMemory(block))

	// db.internal resolves nowhere but on the SSH server, so a read that
	// works went through the tunnel.
	dbAddr := net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port))
	if cfg.Host == "localhost" {
		dbAddr = net.JoinHostPort("127.0.0.1", fmt.Sprint(cfg.Port))
	}
	srv := startSSHServer(t, "tunnel-pass", sshPub, map[string]string{"db.internal:3306": dbAddr})
	host, port := srv.hostPort(t)
	tunneled := cfg
	tunneled.Host, tunneled.Port = "db.internal", 3306

	for _, tm := range []TunnelMethod{
		{TunnelMethod: tunnelPassword, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", TunnelUserPassword: "tunnel-pass"},
		{TunnelMethod: tunnelKey, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", SSHKey: keyPEM},
	} {
		t.Run(tm.TunnelMethod, func(t *testing.T) {
			c := tunneled
			c.TunnelMethod = tm
			require.NoError(t, Source{}.Check(t.Context(), c))
			users := discoverStream(t, c, "users")
			require.Len(t, runRead(t, c, fullRefresh(users), nil).Records(users.Descriptor()), 2)
		})
	}

	t.Run("wrong tunnel password", func(t *testing.T) {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", TunnelUserPassword: "nope"}
		require.ErrorContains(t, Source{}.Check(t.Context(), c), `cannot open SSH tunnel as "bastion"`)
	})
	pinned := func(hostKey string) Config {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelKey, TunnelHost: host, TunnelPort: port, TunnelUser: "bastion", SSHKey: keyPEM, TunnelHostKey: hostKey}
		return c
	}
	t.Run("pinned host key", func(t *testing.T) {
		require.NoError(t, Source{}.Check(t.Context(), pinned(strings.TrimSpace(string(ssh.MarshalAuthorizedKey(srv.hostKey))))))
		require.NoError(t, Source{}.Check(t.Context(), pinned(strings.TrimSpace(string(ssh.MarshalAuthorizedKey(srv.ecdsaKey))))))
	})
	t.Run("mismatched host key", func(t *testing.T) {
		otherPub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		other, err := ssh.NewPublicKey(otherPub)
		require.NoError(t, err)
		require.ErrorContains(t, Source{}.Check(t.Context(), pinned(string(ssh.MarshalAuthorizedKey(other)))), "host key mismatch")
	})
	t.Run("unreachable tunnel host", func(t *testing.T) {
		c := tunneled
		c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: "127.0.0.1", TunnelPort: closedPort(t), TunnelUser: "bastion", TunnelUserPassword: "x"}
		require.ErrorContains(t, Source{}.Check(t.Context(), c), "cannot reach SSH tunnel host")
	})
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
