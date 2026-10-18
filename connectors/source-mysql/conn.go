package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/crypto/ssh"
)

const connectTimeout = 30 * time.Second

const (
	errAccessDenied         = 1045
	errDBAccessDenied       = 1044
	errBadDB                = 1049
	errTableAccessDenied    = 1142
	errColumnAccessDenied   = 1143
	errSpecificAccessDenied = 1227
)

// db is an open connection pool, with the SSH client it runs over when the
// configuration asks for a tunnel.
type db struct {
	*sql.DB
	ssh *ssh.Client
}

func (d *db) close() error {
	err := d.Close()
	if d.ssh != nil {
		err = errors.Join(err, d.ssh.Close())
	}
	return err
}

// connect validates cfg and opens a connection with the session settings the
// record rendering relies on: the text protocol for every query, utf8mb4, a
// UTC time zone and a write timeout long enough for a slow reader.
func connect(ctx context.Context, cfg Config) (*db, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	mc, err := mysqlConfig(cfg)
	if err != nil {
		return nil, err
	}
	d := &db{}
	if cfg.TunnelMethod.TunnelMethod != tunnelNone {
		client, err := dialSSH(ctx, cfg.TunnelMethod)
		if err != nil {
			return nil, err
		}
		d.ssh = client
		mc.DialFunc = func(ctx context.Context, _, addr string) (net.Conn, error) {
			return client.DialContext(ctx, "tcp", addr)
		}
	}
	connector, err := mysql.NewConnector(mc)
	if err != nil {
		if d.ssh != nil {
			_ = d.ssh.Close()
		}
		return nil, synclet.ConfigError(fmt.Errorf("build connection config: %w", err))
	}
	d.DB = sql.OpenDB(connector)
	pctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := d.PingContext(pctx); err != nil {
		_ = d.close()
		return nil, connectError(cfg, err)
	}
	return d, nil
}

// mysqlConfig builds the driver configuration. Every query, with or without
// arguments, runs on the text protocol, so each value arrives as MySQL
// renders it; time values stay text.
func mysqlConfig(cfg Config) (*mysql.Config, error) {
	mc := mysql.NewConfig()
	mc.User = cfg.Username
	mc.Passwd = cfg.Password
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	mc.DBName = cfg.Database
	mc.Timeout = connectTimeout
	mc.InterpolateParams = true
	mc.ParseTime = false
	mc.Params = map[string]string{
		"time_zone":         "'+00:00'",
		"net_write_timeout": "3600",
	}
	if err := mc.Apply(mysql.Charset("utf8mb4", "")); err != nil {
		return nil, fmt.Errorf("build connection config: %w", err)
	}
	if err := setTLS(mc, cfg); err != nil {
		return nil, synclet.ConfigError(err)
	}
	return mc, nil
}

// setTLS sets the connection's encryption for ssl_mode: preferred encrypts
// when the server supports it, required always encrypts, verify_ca also
// checks that the server certificate is signed by the CA, verify_identity
// also checks that it names the host. Certificates stay in memory.
func setTLS(mc *mysql.Config, cfg Config) error {
	s := cfg.SSLMode
	tc := &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	if s.ClientCertificate != "" {
		cert, err := tls.X509KeyPair([]byte(s.ClientCertificate), []byte(s.ClientKey))
		if err != nil {
			return fmt.Errorf("client_certificate and client_key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	var roots *x509.CertPool
	if s.CACertificate != "" {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(s.CACertificate)) {
			return errors.New("ca_certificate holds no PEM certificate")
		}
	}
	switch {
	case s.Mode == sslVerifyIdentity:
		tc.RootCAs = roots
	case roots != nil:
		tc.InsecureSkipVerify = true //nolint:gosec // the chain is verified by VerifyPeerCertificate, the hostname is not
		tc.VerifyPeerCertificate = verifyChain(roots)
	default:
		tc.InsecureSkipVerify = true //nolint:gosec // ssl_mode preferred and required do not verify the server certificate
	}
	mc.TLS = tc
	mc.AllowFallbackToPlaintext = s.Mode == sslPreferred
	return nil
}

func verifyChain(roots *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return errors.New("server sent no certificate")
		}
		certs := make([]*x509.Certificate, len(raw))
		for i, b := range raw {
			c, err := x509.ParseCertificate(b)
			if err != nil {
				return fmt.Errorf("parse server certificate: %w", err)
			}
			certs[i] = c
		}
		inter := x509.NewCertPool()
		for _, c := range certs[1:] {
			inter.AddCert(c)
		}
		_, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter})
		return err
	}
}

func dialSSH(ctx context.Context, t TunnelMethod) (*ssh.Client, error) {
	var auth ssh.AuthMethod
	if t.TunnelMethod == tunnelKey {
		signer, err := ssh.ParsePrivateKey([]byte(t.SSHKey))
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("ssh_key: %w", err))
		}
		auth = ssh.PublicKeys(signer)
	} else {
		auth = ssh.Password(t.TunnelUserPassword)
	}
	hostKey := ssh.InsecureIgnoreHostKey() //nolint:gosec // no tunnel_host_key configured: the host key is not verified
	var hostKeyAlgos []string
	if t.TunnelHostKey != "" {
		keys, err := parseHostKeys(t.TunnelHostKey)
		if err != nil {
			return nil, synclet.ConfigError(err)
		}
		hostKey = pinnedHostKeys(keys)
		hostKeyAlgos = hostKeyAlgorithms(keys)
	}
	addr := net.JoinHostPort(t.TunnelHost, strconv.Itoa(t.TunnelPort))
	sc := &ssh.ClientConfig{
		User:              t.TunnelUser,
		Auth:              []ssh.AuthMethod{auth},
		HostKeyCallback:   hostKey,
		HostKeyAlgorithms: hostKeyAlgos,
		Timeout:           connectTimeout,
	}
	var d net.Dialer
	dctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	nc, err := d.DialContext(dctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cannot reach SSH tunnel host %s: %w", addr, err)
	}
	conn, chans, reqs, err := ssh.NewClientConn(nc, addr, sc)
	if err != nil {
		_ = nc.Close()
		return nil, synclet.ConfigError(fmt.Errorf("cannot open SSH tunnel as %q at %s: %w", t.TunnelUser, addr, err))
	}
	return ssh.NewClient(conn, chans, reqs), nil
}

// parseHostKeys reads the keys of tunnel_host_key, one per line, each in
// authorized_keys format ("ssh-ed25519 AAAA... comment") or as a known_hosts
// line ("host ssh-ed25519 AAAA...") as ssh-keyscan prints it. Blank lines and
// # comments are skipped; marked known_hosts lines (@revoked,
// @cert-authority) are refused.
func parseHostKeys(s string) ([]ssh.PublicKey, error) {
	var keys []ssh.PublicKey
	for i, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
			continue
		case strings.HasPrefix(line, "@"):
			marker, _, _ := strings.Cut(line, " ")
			return nil, fmt.Errorf("tunnel_host_key line %d is marked %s; only plain host keys can be pinned", i+1, marker)
		}
		if key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line)); err == nil {
			keys = append(keys, key)
			continue
		}
		_, _, key, _, _, err := ssh.ParseKnownHosts([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("tunnel_host_key line %d is not a public key in authorized_keys or known_hosts format", i+1)
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, errors.New("tunnel_host_key holds no public key")
	}
	return keys, nil
}

// pinnedHostKeys accepts a server that presents one of keys.
func pinnedHostKeys(keys []ssh.PublicKey) ssh.HostKeyCallback {
	return func(_ string, _ net.Addr, presented ssh.PublicKey) error {
		for _, k := range keys {
			if bytes.Equal(k.Marshal(), presented.Marshal()) {
				return nil
			}
		}
		return errors.New("ssh: host key mismatch")
	}
}

// hostKeyAlgorithms are the host key algorithms the client offers, so the
// server presents a pinned key rather than another of its keys.
func hostKeyAlgorithms(keys []ssh.PublicKey) []string {
	var algos []string
	for _, k := range keys {
		names := []string{k.Type()}
		if k.Type() == ssh.KeyAlgoRSA {
			names = []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
		}
		for _, n := range names {
			if !slices.Contains(algos, n) {
				algos = append(algos, n)
			}
		}
	}
	return algos
}

// connectError names the cause of a failed connection.
func connectError(cfg Config, err error) error {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case errAccessDenied:
			return synclet.ConfigError(fmt.Errorf("cannot log in as %q: wrong user name or password", cfg.Username))
		case errBadDB:
			return synclet.ConfigError(fmt.Errorf("database %q does not exist", cfg.Database))
		case errDBAccessDenied:
			return synclet.ConfigError(fmt.Errorf("user %q has no privilege on database %q", cfg.Username, cfg.Database))
		}
		return fmt.Errorf("connect to %s: %s (error %d)", addr, myErr.Message, myErr.Number)
	}
	if errors.Is(err, mysql.ErrNoTLS) {
		return synclet.ConfigError(fmt.Errorf("server at %s does not support SSL, which ssl_mode %s needs", addr, cfg.SSLMode.Mode))
	}
	var certErr *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownCA) || errors.As(err, &hostErr) {
		return synclet.ConfigError(fmt.Errorf("server certificate of %s is not trusted (ssl_mode %s): %w", addr, cfg.SSLMode.Mode, err))
	}
	var netErr net.Error
	var opErr *net.OpError
	var dnsErr *net.DNSError
	if errors.As(err, &opErr) || errors.As(err, &dnsErr) || errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("cannot reach %s: %w", addr, err)
	}
	return fmt.Errorf("connect to %s: %w", addr, err)
}

// ident quotes a name, or a dotted path of names, with backticks.
func ident(parts ...string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = "`" + strings.ReplaceAll(p, "`", "``") + "`"
	}
	return strings.Join(quoted, ".")
}
