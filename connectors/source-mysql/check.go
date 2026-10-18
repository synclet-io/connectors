package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// Check connects, runs a query and probes SELECT on every table discovery
// would list; with CDC it also checks the binlog settings and the privilege
// to read the binlog position.
func (Source) Check(ctx context.Context, cfg Config) (rerr error) {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(d.close))
	var one int
	if err := d.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("run a test query: %w", err)
	}
	tables, err := loadTables(ctx, d.DB, cfg, "")
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := probeSelect(ctx, d.DB, cfg, t); err != nil {
			return err
		}
	}
	if cfg.cdc() {
		return checkCDC(ctx, d.DB, cfg)
	}
	return nil
}

func probeSelect(ctx context.Context, conn *sql.DB, cfg Config, t *table) (rerr error) {
	rows, err := conn.QueryContext(ctx, "SELECT * FROM "+ident(t.schema, t.name)+" LIMIT 0")
	var myErr *mysql.MySQLError
	switch {
	case errors.As(err, &myErr) && (myErr.Number == errTableAccessDenied || myErr.Number == errColumnAccessDenied):
		return synclet.ConfigError(fmt.Errorf("user %q may not SELECT from table %s.%s", cfg.Username, t.schema, t.name))
	case err != nil:
		return fmt.Errorf("read table %s.%s: %w", t.schema, t.name, err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))
	return rows.Err()
}

// checkCDC verifies that the server writes a full row-based binlog and that
// the user may read its position.
func checkCDC(ctx context.Context, conn *sql.DB, cfg Config) (rerr error) {
	for _, v := range []struct{ name, want string }{
		{"log_bin", "ON"},
		{"binlog_format", "ROW"},
		{"binlog_row_image", "FULL"},
	} {
		got, err := showVariable(ctx, conn, v.name)
		if err != nil {
			return fmt.Errorf("read %s: %w", v.name, err)
		}
		if got != v.want {
			return synclet.ConfigError(fmt.Errorf("CDC needs %s=%s, the server has %s=%s", v.name, v.want, v.name, got))
		}
	}
	rows, err := conn.QueryContext(ctx, "SHOW MASTER STATUS")
	var myErr *mysql.MySQLError
	switch {
	case errors.As(err, &myErr) && myErr.Number == errSpecificAccessDenied:
		return synclet.ConfigError(fmt.Errorf("user %q lacks the REPLICATION CLIENT privilege CDC needs", cfg.Username))
	case err != nil:
		return fmt.Errorf("read the binlog position: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read the binlog position: %w", err)
		}
		return synclet.ConfigError(errors.New("SHOW MASTER STATUS returned no rows: binary logging is not active"))
	}
	return nil
}

// showVariable reads a system variable; name is a constant, since SHOW takes
// no placeholders.
func showVariable(ctx context.Context, conn *sql.DB, name string) (string, error) {
	var varName, value string
	if err := conn.QueryRowContext(ctx, "SHOW VARIABLES LIKE '"+name+"'").Scan(&varName, &value); err != nil {
		return "", err
	}
	return value, nil
}
