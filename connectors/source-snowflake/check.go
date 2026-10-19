package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check logs in, resolves the warehouse and database, and for each
// configured schema (each schema with a table when none is configured)
// reads one row of its first table or view. A failure names the cause: bad
// credentials, unknown account, missing role grant, a warehouse, database or
// schema that is missing or not usable by the role, or a missing SELECT.
func (s Source) Check(ctx context.Context, cfg Config) error {
	c, err := s.connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.close()
	schemaRows, err := c.rows(ctx, `SELECT SCHEMA_NAME FROM `+ident(c.database)+`.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA' ORDER BY SCHEMA_NAME`)
	if err != nil {
		return fmt.Errorf("list schemas of database %q: %w", c.database, err)
	}
	var schemas []string
	for _, r := range schemaRows {
		schemas = append(schemas, r[0])
	}
	for _, name := range cfg.Schemas {
		if slices.Contains(schemas, name) {
			continue
		}
		msg := fmt.Sprintf("schema %q of database %q does not exist, or role %q has no USAGE privilege on it", name, c.database, c.role)
		if up := strings.ToUpper(name); up != name && slices.Contains(schemas, up) {
			msg += fmt.Sprintf(" (unquoted names are stored in upper case: %q)", up)
		}
		return synclet.ConfigError(fmt.Errorf("%s", msg))
	}
	tables, err := c.tableNames(ctx)
	if err != nil {
		return err
	}
	targets := cfg.Schemas
	if len(targets) == 0 {
		for _, s := range schemas {
			if len(tables[s]) > 0 {
				targets = append(targets, s)
			}
		}
		if len(targets) == 0 {
			return synclet.ConfigError(fmt.Errorf("database %q has no table or view role %q can read: grant it USAGE on the schemas and SELECT on the tables to sync", c.database, c.role))
		}
	}
	for _, schema := range targets {
		names := tables[schema]
		if len(names) == 0 {
			return synclet.ConfigError(fmt.Errorf("schema %q has no table or view role %q can read: grant it SELECT on the tables to sync", schema, c.role))
		}
		ref := c.tableRef(schema, names[0])
		if _, err := c.rows(ctx, `SELECT 1 FROM `+ref+` LIMIT 1`); err != nil {
			if notAuthorized(err) {
				return synclet.ConfigError(fmt.Errorf("role %q has no SELECT privilege on %s: %w", c.role, ref, err))
			}
			return fmt.Errorf("read %s: %w", ref, err)
		}
	}
	return nil
}

// tableNames returns the tables, views and materialized views the role can
// see, by schema, in name order.
func (c *conn) tableNames(ctx context.Context) (map[string][]string, error) {
	rows, err := c.rows(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME FROM `+ident(c.database)+`.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA' AND TABLE_TYPE IN `+tableTypesSQL+` ORDER BY TABLE_SCHEMA, TABLE_NAME`)
	if err != nil {
		return nil, fmt.Errorf("list tables of database %q: %w", c.database, err)
	}
	out := map[string][]string{}
	for _, r := range rows {
		out[r[0]] = append(out[r[0]], r[1])
	}
	return out, nil
}

// tableTypesSQL are the INFORMATION_SCHEMA table types read as streams.
const tableTypesSQL = `('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')`
