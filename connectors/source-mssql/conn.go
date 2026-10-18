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

	mssql "github.com/microsoft/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb/integratedauth/ntlm" // registers the "ntlm" authenticator
	"github.com/microsoft/go-mssqldb/msdsn"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/crypto/ssh"
)

const connectTimeout = 30 * time.Second

// db is an open connection pool, with the SSH client it runs over when the
// configuration asks for a tunnel.
type db struct {
	*sql.DB
	ssh *ssh.Client
}

func (d *db) close() {
	if d.DB != nil {
		_ = d.Close()
	}
	if d.ssh != nil {
		_ = d.ssh.Close()
	}
}

// tunnelDialer dials through the SSH client and lets the jump server
// resolve the database host.
type tunnelDialer struct {
	client *ssh.Client
	host   string
}

func (t tunnelDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return t.client.DialContext(ctx, network, addr)
}

func (t tunnelDialer) HostName() string { return t.host }

// connect validates cfg and opens a connection to cfg.Database.
func connect(ctx context.Context, cfg Config) (*db, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	mc, err := msConfig(cfg, cfg.Database)
	if err != nil {
		return nil, synclet.ConfigError(err)
	}
	d := &db{}
	if cfg.TunnelMethod.TunnelMethod != tunnelNone {
		client, err := dialSSH(ctx, cfg.TunnelMethod)
		if err != nil {
			return nil, err
		}
		d.ssh = client
	}
	if err := d.open(ctx, cfg, mc); err != nil {
		err = connectError(ctx, d, cfg, err)
		d.close()
		return nil, err
	}
	return d, nil
}

// open connects d as mc says and checks the connection with a ping.
func (d *db) open(ctx context.Context, cfg Config, mc msdsn.Config) error {
	conn := mssql.NewConnectorConfig(mc)
	if d.ssh != nil {
		conn.Dialer = tunnelDialer{client: d.ssh, host: cfg.Host}
	}
	pool := sql.OpenDB(conn)
	pctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := pool.PingContext(pctx); err != nil {
		_ = pool.Close()
		return err
	}
	d.DB = pool
	return nil
}

// msConfig builds the driver configuration from cfg alone.
func msConfig(cfg Config, database string) (msdsn.Config, error) {
	mc, err := msdsn.Parse("sqlserver://localhost")
	if err != nil {
		return msdsn.Config{}, fmt.Errorf("build connection config: %w", err)
	}
	mc.Host = cfg.Host
	mc.Port = uint64(cfg.Port)
	mc.Instance = ""
	mc.Database = database
	mc.User = cfg.Username
	mc.Password = cfg.Password
	mc.AppName = "synclet-source-mssql"
	mc.DialTimeout = connectTimeout
	mc.DisableRetry = true
	mc.Protocols = []string{"tcp"}
	mc.Parameters = map[string]string{}
	if cfg.Authentication == authNTLM {
		mc.Parameters["authenticator"] = "ntlm"
	}
	tc, enc, err := tlsConfig(cfg)
	if err != nil {
		return msdsn.Config{}, err
	}
	mc.Encryption = enc
	mc.TLSConfig = tc
	mc.HostInCertificateProvided = cfg.SSLMethod.HostNameInCertificate != ""
	return mc, nil
}

// tlsConfig returns the TLS settings of cfg.SSLMethod. Unencrypted still
// encrypts the login when the server supports it, as SQL Server clients do,
// without checking the certificate. Certificates stay in memory.
func tlsConfig(cfg Config) (*tls.Config, msdsn.Encryption, error) {
	tc := &tls.Config{
		ServerName:                  cfg.Host,
		MinVersion:                  tls.VersionTLS12,
		DynamicRecordSizingDisabled: true,
	}
	switch cfg.SSLMethod.SSLMethod {
	case sslUnencrypted:
		tc.InsecureSkipVerify = true //nolint:gosec // only the login is encrypted; the certificate is not checked
		return tc, msdsn.EncryptionOff, nil
	case sslTrust:
		tc.InsecureSkipVerify = true //nolint:gosec // encrypted_trust_server_certificate: the certificate is not checked
		return tc, msdsn.EncryptionRequired, nil
	}
	if h := cfg.SSLMethod.HostNameInCertificate; h != "" {
		tc.ServerName = h
	}
	if pemCA := cfg.SSLMethod.Certificate; pemCA != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(pemCA)) {
			return nil, 0, errors.New("ssl_method.certificate holds no PEM certificate")
		}
		tc.RootCAs = roots
	}
	return tc, msdsn.EncryptionRequired, nil
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

// SQL Server error numbers connectError tells apart.
const (
	errLoginFailed    = 18456
	errCannotOpenDB   = 4060
	errDefaultDB      = 4063
	errLoginNoDefault = 4064
)

// connectError names the cause of a failed connection. A database the
// login cannot open is looked up through master to tell a missing database
// from a missing grant.
func connectError(ctx context.Context, d *db, cfg Config, err error) error {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	var me mssql.Error
	if errors.As(err, &me) {
		numbers := []int32{me.Number}
		for _, e := range me.All {
			numbers = append(numbers, e.Number)
		}
		switch {
		case slices.Contains(numbers, errCannotOpenDB) || slices.Contains(numbers, errDefaultDB) || slices.Contains(numbers, errLoginNoDefault):
			return synclet.ConfigError(databaseError(ctx, d, cfg))
		case slices.Contains(numbers, errLoginFailed):
			return synclet.ConfigError(fmt.Errorf("cannot log in as %q: wrong user name or password", cfg.Username))
		}
		return fmt.Errorf("connect to %s: %s (error %d)", addr, me.Message, me.Number)
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "server does not support encryption"):
		return synclet.ConfigError(fmt.Errorf("server at %s does not support encryption, which ssl_method %s needs", addr, cfg.SSLMethod.SSLMethod))
	case isCertError(err):
		return synclet.ConfigError(fmt.Errorf("server certificate of %s is not trusted (ssl_method %s): %w", addr, cfg.SSLMethod.SSLMethod, err))
	}
	var netErr net.Error
	var opErr *net.OpError
	var dnsErr *net.DNSError
	if errors.As(err, &opErr) || errors.As(err, &dnsErr) || errors.As(err, &netErr) ||
		errors.Is(err, context.DeadlineExceeded) || strings.Contains(msg, "unable to open tcp connection") {
		return fmt.Errorf("cannot reach %s: %w", addr, err)
	}
	return fmt.Errorf("connect to %s: %w", addr, err)
}

// isCertError reports whether err is a failed check of the server
// certificate. The driver wraps the TLS handshake error as text.
func isCertError(err error) bool {
	var certErr *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownCA) || errors.As(err, &hostErr) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "TLS Handshake failed") && strings.Contains(msg, "x509:")
}

// databaseError tells why the login cannot open cfg.Database: it does not
// exist, or the login has no user in it.
func databaseError(ctx context.Context, d *db, cfg Config) error {
	cannot := fmt.Errorf("user %q cannot open database %q: the database does not exist or the login has no user in it", cfg.Username, cfg.Database)
	mc, err := msConfig(cfg, "master")
	if err != nil {
		return cannot
	}
	master := &db{ssh: d.ssh}
	if err := master.open(ctx, cfg, mc); err != nil {
		return cannot
	}
	defer func() { _ = master.Close() }()
	var id sql.NullInt64
	if err := master.QueryRowContext(ctx, "SELECT DB_ID(@p1)", cfg.Database).Scan(&id); err != nil {
		return cannot
	}
	if !id.Valid {
		return fmt.Errorf("database %q does not exist", cfg.Database)
	}
	return fmt.Errorf("user %q cannot open database %q: the login has no user in it", cfg.Username, cfg.Database)
}
