package main

import (
	"context"
	"fmt"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// writePrivileges are what a sync runs on the database's tables.
var writePrivileges = []string{"CREATE TABLE", "INSERT", "SELECT", "DROP TABLE", "ALTER ADD COLUMN"}

// exchangeEngines are the database engines EXCHANGE TABLES works in.
var exchangeEngines = []string{"Atomic", "Replicated", "Shared"}

// Check connects, verifies that the database exists and that its engine
// supports EXCHANGE TABLES, and that the user holds every privilege a sync
// needs on its tables. It creates nothing. CHECK GRANT needs ClickHouse 24.5
// or later.
func (Destination) Check(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer d.close()

	var engine string
	rows, err := d.Query(ctx, `SELECT engine FROM system.databases WHERE name = ?`, cfg.Database)
	if err != nil {
		return fmt.Errorf("look up database %q: %w", cfg.Database, err)
	}
	found := false
	for rows.Next() {
		if err := rows.Scan(&engine); err != nil {
			_ = rows.Close()
			return fmt.Errorf("look up database %q: %w", cfg.Database, err)
		}
		found = true
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("look up database %q: %w", cfg.Database, err)
	}
	if !found {
		return synclet.ConfigError(fmt.Errorf("database %q does not exist", cfg.Database))
	}
	if !slices.Contains(exchangeEngines, engine) {
		return synclet.ConfigError(fmt.Errorf("database %q has engine %s; overwrite needs EXCHANGE TABLES, which only %v databases support", cfg.Database, engine, exchangeEngines))
	}

	for _, priv := range writePrivileges {
		var granted uint8
		if err := d.QueryRow(ctx, "CHECK GRANT "+priv+" ON "+quote(cfg.Database)+".*").Scan(&granted); err != nil {
			return fmt.Errorf("check %s privilege: %w", priv, err)
		}
		if granted == 0 {
			return synclet.ConfigError(fmt.Errorf("user %q has no %s privilege on database %q", cfg.Username, priv, cfg.Database))
		}
	}
	return nil
}
