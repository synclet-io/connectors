package main

import (
	"context"
	"database/sql"
	"fmt"
)

// Check connects, runs a query and verifies that every configured schema
// exists and that the user may SELECT from it: through a schema or
// database grant, or a grant on one of its tables or views.
func (Source) Check(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer d.close()
	var one int
	if err := d.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("run a test query: %w", err)
	}
	for _, s := range cfg.Schemas {
		var id sql.NullInt64
		var readable bool
		err := d.QueryRowContext(ctx, `
SELECT SCHEMA_ID(@p1),
       CASE WHEN HAS_PERMS_BY_NAME(QUOTENAME(@p1), N'SCHEMA', N'SELECT') = 1 OR EXISTS (
         SELECT 1 FROM sys.objects o JOIN sys.columns c ON c.object_id = o.object_id
         WHERE o.schema_id = SCHEMA_ID(@p1) AND o.type IN ('U', 'V') AND o.is_ms_shipped = 0
           AND HAS_PERMS_BY_NAME(QUOTENAME(@p1) + N'.' + QUOTENAME(o.name), N'OBJECT', N'SELECT', QUOTENAME(c.name), N'COLUMN') = 1
       ) THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END`, s).Scan(&id, &readable)
		switch {
		case err != nil:
			return fmt.Errorf("check schema %q: %w", s, err)
		case !id.Valid:
			return fmt.Errorf("schema %q does not exist", s)
		case !readable:
			return fmt.Errorf("user %q may not SELECT from any table or view in schema %q", cfg.Username, s)
		}
	}
	return nil
}
