package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check connects and verifies that the user can create tables in the
// configured schema, or create the schema when it is missing, and can create
// the temporary tables batches are loaded through.
func (Destination) Check(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer d.close()

	var create bool
	err = d.QueryRow(ctx, `SELECT has_schema_privilege(n.oid, 'CREATE') FROM pg_catalog.pg_namespace n WHERE n.nspname = $1::text`,
		cfg.Schema).Scan(&create)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		var createSchema bool
		if err := d.QueryRow(ctx, `SELECT has_database_privilege(current_database(), 'CREATE')`).Scan(&createSchema); err != nil {
			return fmt.Errorf("check database privileges: %w", err)
		}
		if !createSchema {
			return synclet.ConfigError(fmt.Errorf("schema %q does not exist and user %q cannot create it", cfg.Schema, cfg.Username))
		}
	case err != nil:
		return fmt.Errorf("check schema %q: %w", cfg.Schema, err)
	case !create:
		return synclet.ConfigError(fmt.Errorf("user %q has no CREATE privilege on schema %q", cfg.Username, cfg.Schema))
	}

	var temp bool
	if err := d.QueryRow(ctx, `SELECT has_database_privilege(current_database(), 'TEMP')`).Scan(&temp); err != nil {
		return fmt.Errorf("check database privileges: %w", err)
	}
	if !temp {
		return synclet.ConfigError(fmt.Errorf("user %q cannot create temporary tables in database %q, which loading batches needs", cfg.Username, cfg.Database))
	}
	return nil
}
