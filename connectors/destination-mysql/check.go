package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const checkPrefix = "_synclet_check_"

var deniedCommand = regexp.MustCompile(`^(\w+) command denied`)

// Check connects and runs on a probe table every kind of statement a sync
// runs: CREATE, INSERT, UPDATE, RENAME and DROP. The probe table is dropped
// again.
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
	steps := []struct{ what, sql string }{
		{"create a table", "CREATE TABLE " + quote(probe) + " (id BIGINT PRIMARY KEY, v BIGINT)" + tableOptions},
		{"insert into a table", "INSERT INTO " + quote(probe) + " (id, v) VALUES (1, 1)"},
		{"update a table", "INSERT INTO " + quote(probe) + " (id, v) VALUES (1, 2) AS n ON DUPLICATE KEY UPDATE v = n.v"},
		{"rename a table", "RENAME TABLE " + quote(probe) + " TO " + quote(renamed)},
		{"drop a table", "DROP TABLE " + quote(renamed)},
	}
	for i, s := range steps {
		if _, err := d.ExecContext(ctx, s.sql); err != nil {
			if i > 0 {
				cleanup := context.WithoutCancel(ctx)
				_, _ = d.ExecContext(cleanup, "DROP TABLE IF EXISTS "+quote(probe))
				_, _ = d.ExecContext(cleanup, "DROP TABLE IF EXISTS "+quote(renamed))
			}
			return checkError(cfg, s.what, err)
		}
	}
	return nil
}

// checkError names the privilege a failed probe statement lacks.
func checkError(cfg Config, what string, err error) error {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && (myErr.Number == errTableAccessDenied || myErr.Number == errDBAccessDenied) {
		priv := "required"
		if m := deniedCommand.FindStringSubmatch(myErr.Message); m != nil {
			priv = strings.ToUpper(m[1])
		}
		return synclet.ConfigError(fmt.Errorf("user %q has no %s privilege on database %q, which it needs to %s: %s",
			cfg.Username, priv, cfg.Database, what, myErr.Message))
	}
	return fmt.Errorf("probe table: cannot %s: %w", what, err)
}
