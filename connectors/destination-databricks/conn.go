package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"go.uber.org/multierr"
)

// sqlRunner runs statements on the SQL warehouse.
type sqlRunner interface {
	exec(ctx context.Context, query string) error
	// query returns the rows of a statement, every value as text and NULL as
	// "", keyed by column name.
	query(ctx context.Context, query string) ([]map[string]string, error)
	close() error
}

type dbRunner struct{ db *sql.DB }

// openWarehouse connects to the SQL warehouse through databricks-sql-go and
// opens a session, so a bad token, path or host fails here.
func openWarehouse(ctx context.Context, cfg Config, client *http.Client, b *bearer, retries int) (sqlRunner, error) {
	opts := []dbsql.ConnOption{
		dbsql.WithServerHostname(cfg.Hostname),
		dbsql.WithPort(cfg.Port),
		dbsql.WithHTTPPath(cfg.HTTPPath),
		dbsql.WithAuthenticator(b),
		dbsql.WithUserAgentEntry("synclet-destination-databricks"),
		dbsql.WithRetries(retries, time.Second, 30*time.Second),
	}
	if client != nil && client.Transport != nil {
		opts = append(opts, dbsql.WithTransport(client.Transport))
	}
	connector, err := dbsql.NewConnector(opts...)
	if err != nil {
		return nil, fmt.Errorf("configure SQL driver: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, describeConnectError(cfg, err)
	}
	return &dbRunner{db: db}, nil
}

func (r *dbRunner) exec(ctx context.Context, query string) error {
	_, err := r.db.ExecContext(ctx, query)
	return err
}

func (r *dbRunner) query(ctx context.Context, query string) (out []map[string]string, rerr error) {
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]string, len(cols))
		for i, c := range cols {
			row[c] = vals[i].String
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *dbRunner) close() error { return r.db.Close() }

var httpStatus = regexp.MustCompile(`(?:HTTP Response code:|unexpected HTTP status) (\d{3})`)

// describeConnectError names why a session could not be opened: a rejected
// token, a warehouse the token may not use, no warehouse at http_path, or a
// host that cannot be reached.
func describeConnectError(cfg Config, err error) error {
	where := fmt.Sprintf("%s:%d%s", cfg.Hostname, cfg.Port, cfg.HTTPPath)
	if m := httpStatus.FindStringSubmatch(err.Error()); m != nil {
		switch code, _ := strconv.Atoi(m[1]); code {
		case http.StatusUnauthorized:
			return fmt.Errorf("invalid token: the workspace rejected the credentials (HTTP 401 from %s)", where)
		case http.StatusForbidden:
			return fmt.Errorf("access denied to the SQL warehouse at %s (HTTP 403): the token is invalid or lacks CAN USE on the warehouse", where)
		case http.StatusNotFound:
			return fmt.Errorf("SQL warehouse not found at http_path %q on %s (HTTP 404)", cfg.HTTPPath, cfg.Hostname)
		}
	}
	var tokErr *tokenError
	if errors.As(err, &tokErr) {
		return tokErr.err
	}
	var dnsErr *net.DNSError
	var opErr *net.OpError
	if errors.As(err, &dnsErr) || errors.As(err, &opErr) || unreachable(err) {
		return fmt.Errorf("SQL warehouse unreachable at %s: %w", where, err)
	}
	return fmt.Errorf("connect to SQL warehouse at %s: %w", where, err)
}

// unreachable reports a dial or DNS failure that the driver passed on as text.
func unreachable(err error) bool {
	msg := err.Error()
	for _, s := range []string{"connection refused", "no such host", "i/o timeout", "dial tcp", "network is unreachable", "connection reset"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// tokenError carries a failure of the token source through the SQL driver.
type tokenError struct{ err error }

func (e *tokenError) Error() string { return e.err.Error() }
func (e *tokenError) Unwrap() error { return e.err }

// permissionDenied reports a statement Databricks refused for lack of a
// privilege.
func permissionDenied(err error) bool {
	var ee dbsqlerr.DBExecutionError
	if errors.As(err, &ee) && ee.SqlState() == "42501" {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLSTATE: 42501") || strings.Contains(msg, "INSUFFICIENT_PERMISSIONS") || strings.Contains(msg, "PERMISSION_DENIED")
}
