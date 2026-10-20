package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	mssql "github.com/microsoft/go-mssqldb"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const checkPrefix = "_synclet_check_"

// SQL Server error numbers of a statement the user lacks a permission for.
var permissionErrors = []int32{
	229,   // The %ls permission was denied on the object
	230,   // The %ls permission was denied on the column
	262,   // %ls permission denied in database
	300,   // %ls permission was denied on object
	2760,  // The specified schema name either does not exist or you do not have permission to use it
	3701,  // Cannot drop the table, because it does not exist or you do not have permission
	15151, // Cannot find the object, because it does not exist or you do not have permission
	15247, // User does not have permission to perform this action
	15248, // Either the parameter @objname is ambiguous or the claimed @objtype is wrong
}

// Check connects and runs, in a transaction it rolls back, every kind of
// statement a sync runs: CREATE SCHEMA when the schema is missing, then
// CREATE TABLE, INSERT, MERGE, sp_rename and DROP TABLE on a probe table.
// Nothing is left behind.
func (Destination) Check(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer d.close()

	var b [4]byte
	_, _ = rand.Read(b[:])
	probe := checkPrefix + hex.EncodeToString(b[:])
	renamed := probe + "_r"
	table, renamedTable := qualified(cfg.Schema, probe), qualified(cfg.Schema, renamed)

	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin probe transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	in := fmt.Sprintf(" in schema %q of database %q", cfg.Schema, cfg.Database)
	steps := []struct {
		what  string
		query string
		args  []any
	}{
		{fmt.Sprintf("create schema %q in database %q", cfg.Schema, cfg.Database), createSchemaSQL, []any{cfg.Schema, quote(cfg.Schema)}},
		{"create a table" + in, "CREATE TABLE " + table + " (id BIGINT NOT NULL PRIMARY KEY, v BIGINT)", nil},
		{"insert into a table" + in, "INSERT INTO " + table + " (id, v) VALUES (1, 1)", nil},
		{"merge into a table" + in, "MERGE INTO " + table + " AS t USING (SELECT 1 AS id, 2 AS v) AS s ON t.id = s.id " +
			"WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);", nil},
		{"rename a table" + in, renameSQL, []any{table, renamed}},
		{"drop a table" + in, "DROP TABLE " + renamedTable, nil},
	}
	for _, s := range steps {
		if _, err := tx.ExecContext(ctx, s.query, s.args...); err != nil {
			return checkError(cfg, s.what, err)
		}
	}
	return nil
}

// checkError names the permission a failed probe statement lacks.
func checkError(cfg Config, what string, err error) error {
	if slices.ContainsFunc(errorNumbers(err), func(n int32) bool { return slices.Contains(permissionErrors, n) }) {
		var me mssql.Error
		errors.As(err, &me)
		return synclet.ConfigError(fmt.Errorf("user %q may not %s: %s", cfg.Username, what, me.Message))
	}
	return fmt.Errorf("probe table: cannot %s: %w", what, err)
}
