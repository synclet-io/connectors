package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const connectTimeout = 30 * time.Second

// db is an open connection.
type db struct {
	*pgx.Conn
}

func (d *db) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.Close(ctx)
}

// connect validates cfg and opens a connection with the session settings the
// value rendering relies on: UTC time zone and ISO dates.
func connect(ctx context.Context, cfg Config) (*db, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	pc, err := pgConfig(cfg)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(ctx, pc)
	if err != nil {
		return nil, connectError(cfg, err)
	}
	return &db{Conn: conn}, nil
}

// neutralSettings sets every connection parameter libpq reads from a PG*
// environment variable or the home directory, so none of them reaches pgx.
// The placeholder host, port, database, user and password are replaced by
// the configured ones.
const neutralSettings = "host=localhost port=5432 dbname=postgres user=postgres password=placeholder passfile='' " +
	"connect_timeout=0 sslmode=disable sslrootcert='' sslcert='' sslkey='' sslpassword='' sslsni=1 sslnegotiation=postgres " +
	"target_session_attrs=any min_protocol_version=3.0 max_protocol_version=3.0 channel_binding=prefer require_auth=''"

// pgConfig builds the pgx configuration from cfg alone; PG* environment
// variables and files in the home directory are ignored.
func pgConfig(cfg Config) (*pgx.ConnConfig, error) {
	pc, err := pgx.ParseConfig(neutralSettings)
	if err != nil {
		return nil, fmt.Errorf("build connection config: %w", err)
	}
	pc.Host = cfg.Host
	pc.Port = uint16(cfg.Port)
	pc.Database = cfg.Database
	pc.User = cfg.Username
	pc.Password = cfg.Password
	pc.ConnectTimeout = connectTimeout
	pc.KerberosSrvName = ""
	pc.KerberosSpn = ""
	pc.RuntimeParams = map[string]string{
		"application_name": "synclet-destination-postgres",
		"TimeZone":         "UTC",
		"DateStyle":        "ISO, MDY",
	}
	if err := setTLS(&pc.Config, cfg); err != nil {
		return nil, synclet.ConfigError(err)
	}
	return pc, nil
}

// setTLS gives the connection libpq's sslmode behaviour. Certificates stay
// in memory.
func setTLS(pc *pgconn.Config, cfg Config) error {
	pc.Fallbacks = nil
	pc.TLSConfig = nil
	mode := cfg.SSLMode.Mode
	if mode == sslDisable {
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
	case mode == sslVerifyFull:
		tc.RootCAs = roots
	case roots != nil:
		// verify-ca, and require with a CA as libpq does: the chain is
		// checked, the hostname is not.
		tc.InsecureSkipVerify = true
		tc.VerifyPeerCertificate = verifyChain(roots)
	default:
		tc.InsecureSkipVerify = true
	}

	switch mode {
	case sslAllow:
		pc.Fallbacks = []*pgconn.FallbackConfig{{Host: pc.Host, Port: pc.Port, TLSConfig: tc}}
	case sslPrefer:
		pc.TLSConfig = tc
		pc.Fallbacks = []*pgconn.FallbackConfig{{Host: pc.Host, Port: pc.Port}}
	default:
		pc.TLSConfig = tc
	}
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
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "28P01":
			return synclet.ConfigError(fmt.Errorf("cannot log in as %q: wrong user name or password", cfg.Username))
		case "28000":
			return synclet.ConfigError(fmt.Errorf("cannot log in as %q: %s", cfg.Username, pgErr.Message))
		case "42501":
			return synclet.ConfigError(fmt.Errorf("permission denied for database %q: user %q has no CONNECT privilege", cfg.Database, cfg.Username))
		case "3D000":
			return synclet.ConfigError(fmt.Errorf("database %q does not exist", cfg.Database))
		}
		return fmt.Errorf("connect to %s: %s (SQLSTATE %s)", addr, pgErr.Message, pgErr.Code)
	}
	var certErr *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownCA) || errors.As(err, &hostErr) {
		return synclet.ConfigError(fmt.Errorf("server certificate of %s is not trusted (ssl_mode %s): %w", addr, cfg.SSLMode.Mode, unwrapConnect(err)))
	}
	if msg := err.Error(); strings.Contains(msg, "server refused TLS connection") {
		return synclet.ConfigError(fmt.Errorf("server at %s does not support SSL, which ssl_mode %s needs", addr, cfg.SSLMode.Mode))
	}
	var netErr net.Error
	var opErr *net.OpError
	var dnsErr *net.DNSError
	if errors.As(err, &opErr) || errors.As(err, &dnsErr) || errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("cannot reach %s: %w", addr, unwrapConnect(err))
	}
	return fmt.Errorf("connect to %s: %w", addr, err)
}

func unwrapConnect(err error) error {
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		if inner := errors.Unwrap(ce); inner != nil {
			return inner
		}
	}
	return err
}
