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

// warehouse is the SQL side of Redshift the connector uses. Statements run
// one at a time on one session; transactions are BEGIN, COMMIT and ROLLBACK
// statements.
type warehouse interface {
	exec(ctx context.Context, sql string) error
	// query returns every row of a result as the values pgx decodes.
	query(ctx context.Context, sql string) ([][]any, error)
	close()
}

// pgWarehouse is a warehouse on one pgx connection in simple-protocol
// mode: Redshift answers the extended protocol's describe steps for only
// some statements, and every statement here is sent as text.
type pgWarehouse struct {
	conn *pgx.Conn
}

func (w *pgWarehouse) exec(ctx context.Context, sql string) error {
	_, err := w.conn.Exec(ctx, sql)
	return err
}

func (w *pgWarehouse) query(ctx context.Context, sql string) ([][]any, error) {
	rows, err := w.conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

func (w *pgWarehouse) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = w.conn.Close(ctx)
}

// connect validates cfg and opens a session on the cluster.
func connect(ctx context.Context, cfg Config) (warehouse, error) {
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
	return &pgWarehouse{conn: conn}, nil
}

// neutralSettings sets every connection parameter libpq reads from a PG*
// environment variable or the home directory, so none of them reaches pgx.
const neutralSettings = "host=localhost port=5439 dbname=dev user=placeholder password=placeholder passfile='' " +
	"connect_timeout=0 sslmode=disable sslrootcert='' sslcert='' sslkey='' sslpassword='' sslsni=1 sslnegotiation=postgres " +
	"target_session_attrs=any min_protocol_version=3.0 max_protocol_version=3.0 channel_binding=prefer require_auth=''"

// pgConfig builds the pgx configuration from cfg alone.
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
	pc.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pc.RuntimeParams = map[string]string{"application_name": "synclet-destination-redshift"}
	pc.Fallbacks = nil
	pc.TLSConfig = nil
	switch cfg.SSLMode.Mode {
	case sslRequire:
		pc.TLSConfig = &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}
	case sslVerifyFull:
		pc.TLSConfig = &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	}
	return pc, nil
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
