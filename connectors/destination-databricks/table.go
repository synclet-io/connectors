package main

import (
	"context"
	"fmt"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// prepareTable makes the table a stream is written to ready: a fresh shadow
// table for overwrite; for append and append_dedup the table is created when
// missing, and a new schema property adds a nullable column. A column whose
// type differs from the catalog's fails the sync.
func prepareTable(ctx context.Context, run sqlRunner, loc location, p *streamPlan) error {
	if p.mode == synclet.DestinationSyncModeOverwrite {
		if err := run.exec(ctx, createShadowSQL(loc, p)); err != nil {
			return fmt.Errorf("create table %s: %w", loc.table(p.shadow), err)
		}
		return nil
	}
	table := loc.table(p.table)
	existing, err := tableColumns(ctx, run, loc, p.table)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		if err := run.exec(ctx, createTableSQL(loc, p)); err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}
		return nil
	}
	var missing []column
	for _, c := range p.columns {
		typ, ok := existing[strings.ToLower(c.name)]
		switch {
		case !ok:
			missing = append(missing, c)
		case !sameType(typ, c.typ):
			return fmt.Errorf("column %q of table %s is %s, the catalog needs %s; changing a column type is not supported", c.name, table, strings.ToUpper(typ), c.typ)
		}
	}
	if len(missing) > 0 {
		if err := run.exec(ctx, addColumnsSQL(loc, p, missing)); err != nil {
			return fmt.Errorf("add columns to table %s: %w", table, err)
		}
	}
	return nil
}

// tableColumns returns the columns of a table, by lower-case name, and
// their types; none when the table does not exist.
func tableColumns(ctx context.Context, run sqlRunner, loc location, name string) (map[string]string, error) {
	rows, err := run.query(ctx, "SELECT column_name, full_data_type FROM "+ident(strings.ToLower(loc.catalog))+
		".information_schema.columns WHERE table_schema = "+sqlString(strings.ToLower(loc.schema))+
		" AND table_name = "+sqlString(name)+" ORDER BY ordinal_position")
	if err != nil {
		return nil, describeSQLError(err, "read columns of table "+loc.table(name))
	}
	cols := make(map[string]string, len(rows))
	for _, r := range rows {
		cols[strings.ToLower(r["column_name"])] = r["full_data_type"]
	}
	return cols, nil
}

func sameType(have string, want colType) bool {
	return strings.EqualFold(strings.ReplaceAll(have, " ", ""), string(want))
}
