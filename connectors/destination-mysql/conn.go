package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const connectTimeout = 30 * time.Second

// sqlMode makes MySQL reject a value that does not fit its column instead of
// storing a truncated, rounded or zero value with a warning.
const sqlMode = "STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"

// MySQL error numbers the connector maps to a cause.
const (
	errDBAccessDenied    = 1044
	errAccessDenied      = 1045
	errBadDB             = 1049
	errTableAccessDenied = 1142
)

// db is one connection of its own pool.
type db struct {
	pool *sql.DB
	*sql.Conn
}

func (d *db) close() {
	_ = d.Close()
	_ = d.pool.Close()
}

type quietLogger struct{}

func (quietLogger) Print(...any) {}

// connect validates cfg and opens a connection whose session runs in UTC,
// with utf8mb4 and a strict sql_mode.
func connect(ctx context.Context, cfg Config) (*db, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	mc, err := driverConfig(cfg)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(mc)
	if err != nil {
		return nil, synclet.ConfigError(fmt.Errorf("build connection config: %w", err))
	}
	pool := sql.OpenDB(connector)
	conn, err := pool.Conn(ctx)
	if err != nil {
		_ = pool.Close()
		return nil, connectError(cfg, err)
	}
	return &db{pool: pool, Conn: conn}, nil
}

// driverConfig builds the driver configuration from cfg alone. Values are
// interpolated into the statement text on the client.
func driverConfig(cfg Config) (*mysql.Config, error) {
	mc := mysql.NewConfig()
	mc.User = cfg.Username
	mc.Passwd = cfg.Password
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	mc.DBName = cfg.Database
	mc.Timeout = connectTimeout
	mc.InterpolateParams = true
	mc.Logger = quietLogger{}
	mc.Params = map[string]string{
		"time_zone": "'+00:00'",
		"sql_mode":  "'" + sqlMode + "'",
	}
	if err := mc.Apply(mysql.Charset("utf8mb4", "utf8mb4_bin")); err != nil {
		return nil, fmt.Errorf("build connection config: %w", err)
	}
	if err := setTLS(mc, cfg); err != nil {
		return nil, synclet.ConfigError(err)
	}
	return mc, nil
}

// setTLS sets mc.TLS for the ssl_mode. Certificates stay in memory.
func setTLS(mc *mysql.Config, cfg Config) error {
	mode := cfg.SSLMode.Mode
	if mode == sslDisabled {
		return nil
	}
	tc := &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	if cfg.SSLMode.ClientCertificate != "" {
		cert, err := tls.X509KeyPair([]byte(cfg.SSLMode.ClientCertificate), []byte(cfg.SSLMode.ClientKey))
		if err != nil {
			return fmt.Errorf("client_certificate and client_key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	var roots *x509.CertPool
	if cfg.SSLMode.CACertificate != "" {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(cfg.SSLMode.CACertificate)) {
			return errors.New("ca_certificate holds no PEM certificate")
		}
	}
	switch {
	case mode == sslVerifyIdentity:
		tc.RootCAs = roots
	case roots != nil:
		// verify_ca, and required with a CA: the chain is checked, the host
		// name is not.
		tc.InsecureSkipVerify = true
		tc.VerifyPeerCertificate = verifyChain(roots)
	default:
		tc.InsecureSkipVerify = true
	}
	mc.TLS = tc
	mc.AllowFallbackToPlaintext = mode == sslPreferred
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
			return synclet.ConfigError(fmt.Errorf("user %q has no access to database %q", cfg.Username, cfg.Database))
		}
		return fmt.Errorf("connect to %s: %w", addr, myErr)
	}
	var certErr *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownCA) || errors.As(err, &hostErr) {
		return synclet.ConfigError(fmt.Errorf("server certificate of %s is not trusted (ssl_mode %s): %w", addr, cfg.SSLMode.Mode, err))
	}
	if errors.Is(err, mysql.ErrNoTLS) {
		return synclet.ConfigError(fmt.Errorf("server at %s does not support SSL, which ssl_mode %s needs", addr, cfg.SSLMode.Mode))
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("cannot reach %s: %w", addr, err)
	}
	return fmt.Errorf("connect to %s: %w", addr, err)
}
