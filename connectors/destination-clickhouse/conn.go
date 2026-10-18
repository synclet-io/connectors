package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const connectTimeout = 30 * time.Second

// ClickHouse error codes the connector names.
const (
	codeUnknownDatabase      = 81
	codeAccessDenied         = 497
	codeAuthenticationFailed = 516
)

// db is an open connection pool.
type db struct {
	driver.Conn
}

func (d *db) close() { _ = d.Close() }

// connect validates cfg, opens a connection with synchronous inserts and
// pings the server. A failed ping names its cause.
func connect(ctx context.Context, cfg Config) (*db, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	opts, err := chOptions(cfg)
	if err != nil {
		return nil, synclet.ConfigError(err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, connectError(cfg, err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, connectError(cfg, err)
	}
	return &db{Conn: conn}, nil
}

// chOptions builds the driver options from cfg. Inserts run with
// async_insert=0, so an INSERT returns once its rows are written to a part.
func chOptions(cfg Config) (*clickhouse.Options, error) {
	opts := &clickhouse.Options{
		Addr:     []string{net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))},
		Protocol: clickhouse.Native,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		ClientInfo: clickhouse.ClientInfo{Products: []struct {
			Name    string
			Version string
		}{{Name: "synclet-destination-clickhouse", Version: "0.1.0"}}},
		Settings:     clickhouse.Settings{"async_insert": 0},
		DialTimeout:  connectTimeout,
		MaxOpenConns: 2,
		MaxIdleConns: 2,
	}
	if cfg.Protocol == protocolHTTP {
		opts.Protocol = clickhouse.HTTP
	}
	if !cfg.TLS.Enabled {
		return opts, nil
	}
	tc := &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	if cfg.TLS.CACertificate != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(cfg.TLS.CACertificate)) {
			return nil, errors.New("ca_certificate holds no PEM certificate")
		}
		tc.RootCAs = roots
	}
	if cfg.TLS.ClientCertificate != "" {
		cert, err := tls.X509KeyPair([]byte(cfg.TLS.ClientCertificate), []byte(cfg.TLS.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("client_certificate and client_key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	opts.TLS = tc
	return opts, nil
}

var codeText = regexp.MustCompile(`(?i)\bcode:? (\d+)`)

// errorCode returns the ClickHouse error code of err: the code of a native
// exception, or the "Code: N" an HTTP error carries in its text.
func errorCode(err error) (int, bool) {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		return int(ex.Code), true
	}
	if m := codeText.FindStringSubmatch(err.Error()); m != nil {
		n, convErr := strconv.Atoi(m[1])
		return n, convErr == nil
	}
	return 0, false
}

// connectError names the cause of a failed connection.
func connectError(cfg Config, err error) error {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	if code, ok := errorCode(err); ok {
		switch code {
		case codeAuthenticationFailed:
			return synclet.ConfigError(fmt.Errorf("cannot log in as %q: wrong user name or password", cfg.Username))
		case codeUnknownDatabase:
			return synclet.ConfigError(fmt.Errorf("database %q does not exist", cfg.Database))
		case codeAccessDenied:
			return synclet.ConfigError(fmt.Errorf("user %q has no access to database %q: %w", cfg.Username, cfg.Database, err))
		}
	}
	var certErr *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownCA) || errors.As(err, &hostErr) {
		return synclet.ConfigError(fmt.Errorf("server certificate of %s is not trusted: %w", addr, err))
	}
	var opErr *net.OpError
	var dnsErr *net.DNSError
	var netErr net.Error
	if errors.As(err, &opErr) || errors.As(err, &dnsErr) || errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("cannot reach %s: %w", addr, err)
	}
	return fmt.Errorf("connect to %s: %w", addr, err)
}
