package main

import (
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
	"github.com/microsoft/go-mssqldb/msdsn"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const connectTimeout = 30 * time.Second

// sessionSettings make an error abort the whole transaction and a value
// that does not fit its column an error rather than a truncation.
const sessionSettings = "SET XACT_ABORT ON; SET ANSI_WARNINGS ON; SET ARITHABORT ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; SET NOCOUNT ON"

// SQL Server error numbers connectError tells apart.
const (
	errLoginFailed    = 18456
	errCannotOpenDB   = 4060
	errDefaultDB      = 4063
	errLoginNoDefault = 4064
)

// db is one connection of its own pool; temporary tables live as long as
// it does.
type db struct {
	pool *sql.DB
	*sql.Conn
}

func (d *db) close() {
	_ = d.Close()
	_ = d.pool.Close()
}

// connect validates cfg and opens a connection to cfg.Database with
// sessionSettings applied.
func connect(ctx context.Context, cfg Config) (*db, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	mc, err := msConfig(cfg, cfg.Database)
	if err != nil {
		return nil, synclet.ConfigError(err)
	}
	d, err := open(ctx, mc)
	if err != nil {
		return nil, connectError(ctx, cfg, err)
	}
	if _, err := d.ExecContext(ctx, sessionSettings); err != nil {
		d.close()
		return nil, fmt.Errorf("set session options: %w", err)
	}
	return d, nil
}

func open(ctx context.Context, mc msdsn.Config) (*db, error) {
	pool := sql.OpenDB(mssql.NewConnectorConfig(mc))
	cctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	conn, err := pool.Conn(cctx)
	if err == nil {
		err = conn.PingContext(cctx)
		if err != nil {
			_ = conn.Close()
		}
	}
	if err != nil {
		_ = pool.Close()
		return nil, err
	}
	return &db{pool: pool, Conn: conn}, nil
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
	mc.AppName = "synclet-destination-mssql"
	mc.DialTimeout = connectTimeout
	mc.DisableRetry = true
	mc.Protocols = []string{"tcp"}
	mc.Parameters = map[string]string{}
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

// errorNumbers returns the number of a SQL Server error and of every error
// sent with it, or nil when err is not one.
func errorNumbers(err error) []int32 {
	var me mssql.Error
	if !errors.As(err, &me) {
		return nil
	}
	numbers := []int32{me.Number}
	for _, e := range me.All {
		numbers = append(numbers, e.Number)
	}
	return numbers
}

// connectError names the cause of a failed connection. A database the
// login cannot open is looked up through master to tell a missing database
// from a missing grant.
func connectError(ctx context.Context, cfg Config, err error) error {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	if numbers := errorNumbers(err); numbers != nil {
		switch {
		case slices.Contains(numbers, errCannotOpenDB) || slices.Contains(numbers, errDefaultDB) || slices.Contains(numbers, errLoginNoDefault):
			return synclet.ConfigError(databaseError(ctx, cfg))
		case slices.Contains(numbers, errLoginFailed):
			return synclet.ConfigError(fmt.Errorf("cannot log in as %q: wrong user name or password", cfg.Username))
		}
		return fmt.Errorf("connect to %s: %w", addr, err)
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
func databaseError(ctx context.Context, cfg Config) error {
	cannot := fmt.Errorf("user %q cannot open database %q: the database does not exist or the login has no user in it", cfg.Username, cfg.Database)
	mc, err := msConfig(cfg, "master")
	if err != nil {
		return cannot
	}
	master, err := open(ctx, mc)
	if err != nil {
		return cannot
	}
	defer master.close()
	var id sql.NullInt64
	if err := master.QueryRowContext(ctx, "SELECT DB_ID(@p1)", cfg.Database).Scan(&id); err != nil {
		return cannot
	}
	if !id.Valid {
		return fmt.Errorf("database %q does not exist", cfg.Database)
	}
	return fmt.Errorf("user %q cannot open database %q: the login has no user in it", cfg.Username, cfg.Database)
}
