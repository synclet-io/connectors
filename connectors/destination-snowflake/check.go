package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check logs in, resolves the warehouse and database, creates the schema
// when it is missing, and creates, writes and drops a table in it. A failure
// names the cause: a key pair that does not match the user, an unknown
// account, a role not granted, a warehouse or database that is missing or
// not usable, or a missing CREATE privilege.
func (d Destination) Check(ctx context.Context, cfg Config) (rerr error) {
	c, err := d.connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := c.close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	if err := c.ensureSchema(ctx); err != nil {
		return err
	}
	table := "_SYNCLET_CHECK_" + strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", ""))
	ref := c.tableRef(table)
	if err := c.exec(ctx, `CREATE TABLE `+ref+` ("ID" NUMBER(38,0))`); err != nil {
		return c.createError(table, err)
	}
	defer func() {
		if err := c.exec(context.WithoutCancel(ctx), `DROP TABLE IF EXISTS `+ref); err != nil {
			synclet.Logger(ctx).Warn(fmt.Sprintf("drop check table %s: %v", ref, err))
		}
	}()
	if err := c.exec(ctx, `INSERT INTO `+ref+` ("ID") VALUES (1)`); err != nil {
		return fmt.Errorf("write to %s: %w", ref, err)
	}
	return nil
}
