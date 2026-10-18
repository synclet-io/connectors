package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Check connects, runs a query and verifies that every configured schema
// exists and grants the user USAGE.
func (Source) Check(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer d.close()
	var one int
	if err := d.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("run a test query: %w", err)
	}
	for _, s := range cfg.Schemas {
		var usage bool
		err := d.QueryRow(ctx, `
SELECT has_schema_privilege(n.oid, 'USAGE')
FROM pg_catalog.pg_namespace n WHERE n.nspname = $1::text`, s).Scan(&usage)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("schema %q does not exist", s)
		case err != nil:
			return fmt.Errorf("check schema %q: %w", s, err)
		case !usage:
			return fmt.Errorf("user %q has no USAGE privilege on schema %q", cfg.Username, s)
		}
	}
	return nil
}
