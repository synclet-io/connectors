package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const checkTable = "_synclet_check"

// Check connects, verifies that the user can create tables in the
// configured schema (or create the schema) and temporary tables, then
// uploads a probe file to the staging bucket, loads it with COPY into a
// temporary table and deletes it.
func (d Destination) Check(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	w, err := d.open(ctx, cfg)
	if err != nil {
		return err
	}
	defer w.close()
	if err := checkPrivileges(ctx, w, cfg); err != nil {
		return err
	}

	st, err := newStaging(ctx, cfg, d.s3Endpoint, d.stsEndpoint)
	if err != nil {
		return err
	}
	if _, err := st.retrieve(ctx); err != nil {
		return synclet.ConfigError(err)
	}
	key := st.key(checkTable + "_" + uuid.NewString() + ".csv")
	if err := st.put(ctx, key, []byte("\"synclet\"\n")); err != nil {
		return synclet.ConfigError(err)
	}
	copyErr := checkCopy(ctx, w, st, key)
	if err := st.delete(context.WithoutCancel(ctx), []string{key}); err != nil {
		return synclet.ConfigError(errors.Join(copyErr, err))
	}
	return copyErr
}

func checkPrivileges(ctx context.Context, w warehouse, cfg Config) error {
	rows, err := w.query(ctx, `SELECT has_schema_privilege(n.nspname, 'CREATE') FROM pg_catalog.pg_namespace n WHERE n.nspname = `+literal(cfg.Schema))
	if err != nil {
		return fmt.Errorf("check schema %q: %w", cfg.Schema, err)
	}
	if len(rows) == 0 {
		ok, err := queryBool(ctx, w, `SELECT has_database_privilege(current_database(), 'CREATE')`)
		if err != nil {
			return fmt.Errorf("check database privileges: %w", err)
		}
		if !ok {
			return synclet.ConfigError(fmt.Errorf("schema %q does not exist and user %q cannot create it", cfg.Schema, cfg.Username))
		}
	} else if create, _ := rows[0][0].(bool); !create {
		return synclet.ConfigError(fmt.Errorf("user %q has no CREATE privilege on schema %q", cfg.Username, cfg.Schema))
	}
	temp, err := queryBool(ctx, w, `SELECT has_database_privilege(current_database(), 'TEMP')`)
	if err != nil {
		return fmt.Errorf("check database privileges: %w", err)
	}
	if !temp {
		return synclet.ConfigError(fmt.Errorf("user %q cannot create temporary tables in database %q, which loading batches needs", cfg.Username, cfg.Database))
	}
	return nil
}

func queryBool(ctx context.Context, w warehouse, sql string) (bool, error) {
	rows, err := w.query(ctx, sql)
	if err != nil {
		return false, err
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return false, fmt.Errorf("%s: expected one value", sql)
	}
	b, ok := rows[0][0].(bool)
	if !ok {
		return false, fmt.Errorf("%s: got %T, expected a boolean", sql, rows[0][0])
	}
	return b, nil
}

// checkCopy loads the probe into a temporary table, which proves that the
// cluster can read the staging bucket with the configured authorization.
func checkCopy(ctx context.Context, w warehouse, st *staging, key string) error {
	auth, err := st.copyAuth(ctx)
	if err != nil {
		return synclet.ConfigError(err)
	}
	if err := w.exec(ctx, "DROP TABLE IF EXISTS "+quote(checkTable)); err != nil {
		return fmt.Errorf("drop table %s: %w", checkTable, err)
	}
	if err := w.exec(ctx, "CREATE TEMP TABLE "+quote(checkTable)+" ("+quote("v")+" VARCHAR(16))"); err != nil {
		return fmt.Errorf("create temporary table: %w", err)
	}
	defer func() { _ = w.exec(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+quote(checkTable)) }()
	sql := "COPY " + quote(checkTable) + " (" + quote("v") + ") FROM " + literal(st.url(key)) + " " + auth +
		" REGION " + literal(st.region) + " FORMAT AS CSV"
	if err := w.exec(ctx, sql); err != nil {
		return copyAuthError(st, key, err)
	}
	return nil
}

// copyAuthError names why COPY could not read a staged file.
func copyAuthError(st *staging, key string, err error) error {
	msg := err.Error()
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		msg = pgErr.Message
	}
	lower := strings.ToLower(msg)
	switch {
	case st.iamRole != "" && strings.Contains(lower, "not authorized to assume iam role"):
		return synclet.ConfigError(fmt.Errorf("the Redshift cluster cannot assume IAM role %s: %s", st.iamRole, msg))
	case strings.Contains(lower, "access denied") || strings.Contains(lower, "accessdenied") ||
		strings.Contains(lower, "invalidaccesskeyid") || strings.Contains(lower, "signaturedoesnotmatch") ||
		strings.Contains(lower, "token") && strings.Contains(lower, "invalid"):
		return synclet.ConfigError(fmt.Errorf("COPY cannot read %s with the configured authorization: %s", st.url(key), msg))
	}
	return fmt.Errorf("COPY from %s: %s", st.url(key), msg)
}
