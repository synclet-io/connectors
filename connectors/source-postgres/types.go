package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// kind is how a column's value is rendered and declared.
type kind int

const (
	kindOther kind = iota
	kindBool
	kindInt
	kindFloat
	kindString
	kindBytea
	kindJSON
	kindDate
	kindTime
	kindTimeTZ
	kindTimestamp
	kindTimestampTZ
	kindInterval
)

// colType is a column's kind after domains are resolved to their base type;
// for an array it is the kind of the elements.
type colType struct {
	kind  kind
	array bool
}

var builtinKinds = map[string]kind{
	"bool":        kindBool,
	"int2":        kindInt,
	"int4":        kindInt,
	"int8":        kindInt,
	"float4":      kindFloat,
	"float8":      kindFloat,
	"numeric":     kindFloat,
	"text":        kindString,
	"varchar":     kindString,
	"bpchar":      kindString,
	"char":        kindString,
	"name":        kindString,
	"uuid":        kindString,
	"xml":         kindString,
	"bytea":       kindBytea,
	"json":        kindJSON,
	"jsonb":       kindJSON,
	"date":        kindDate,
	"time":        kindTime,
	"timetz":      kindTimeTZ,
	"timestamp":   kindTimestamp,
	"timestamptz": kindTimestampTZ,
	"interval":    kindInterval,
}

type pgType struct {
	name     string
	typtype  string
	category string
	base     uint32
	elem     uint32
	builtin  bool
}

// typeResolver loads pg_type rows on demand and caches them.
type typeResolver struct {
	conn  *pgx.Conn
	types map[uint32]pgType
}

func newTypeResolver(conn *pgx.Conn) *typeResolver {
	return &typeResolver{conn: conn, types: map[uint32]pgType{}}
}

func (r *typeResolver) load(ctx context.Context, oids []uint32) error {
	for len(oids) > 0 {
		var missing []uint32
		for _, o := range oids {
			if _, ok := r.types[o]; !ok && o != 0 {
				missing = append(missing, o)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		rows, err := r.conn.Query(ctx, `
SELECT t.oid, t.typname, t.typtype::text, t.typcategory::text, t.typbasetype, t.typelem,
       t.typnamespace = 'pg_catalog'::regnamespace
FROM pg_catalog.pg_type t WHERE t.oid = ANY($1::oid[])`, missing)
		if err != nil {
			return fmt.Errorf("load column types: %w", err)
		}
		oids = nil
		for rows.Next() {
			var oid uint32
			var t pgType
			if err := rows.Scan(&oid, &t.name, &t.typtype, &t.category, &t.base, &t.elem, &t.builtin); err != nil {
				rows.Close()
				return fmt.Errorf("load column types: %w", err)
			}
			r.types[oid] = t
			oids = append(oids, t.base, t.elem)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("load column types: %w", err)
		}
	}
	return nil
}

// resolve returns the colType of a loaded type oid.
func (r *typeResolver) resolve(oid uint32) colType {
	t := r.base(oid)
	if t.category == "A" && t.elem != 0 {
		return colType{kind: kindOf(r.base(t.elem)), array: true}
	}
	return colType{kind: kindOf(t)}
}

func (r *typeResolver) base(oid uint32) pgType {
	t := r.types[oid]
	for i := 0; t.typtype == "d" && i < 32; i++ {
		t = r.types[t.base]
	}
	return t
}

func kindOf(t pgType) kind {
	if t.typtype == "e" {
		return kindString
	}
	if !t.builtin {
		return kindOther
	}
	return builtinKinds[t.name]
}

func nullable(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}
}

// scalarSchema is the JSON schema of one value of kind k.
func scalarSchema(k kind) synclet.PropertySpec {
	switch k {
	case kindBool:
		return synclet.PropertySpec{PropertyType: nullable("boolean")}
	case kindInt:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Integer)}
	case kindFloat:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Number)}
	case kindJSON:
		return synclet.PropertySpec{}
	case kindDate:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String), Format: synclet.Date}
	case kindTime:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: "time_without_timezone"}}
	case kindTimeTZ:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: "time_with_timezone"}, Format: synclet.Time}
	case kindTimestamp:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWOTZ}}
	case kindTimestampTZ:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWithTZ}, Format: synclet.DateTime}
	default:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String)}
	}
}

// schema is the JSON schema of a column of type c. Values without a time
// zone carry no format: JSON schema's time and date-time need an offset.
func (c colType) schema() synclet.PropertySpec {
	if !c.array {
		return scalarSchema(c.kind)
	}
	items := map[string]any{}
	el := scalarSchema(c.elementKind())
	if len(el.Type) > 0 {
		types := make([]string, len(el.Type))
		for i, t := range el.Type {
			types[i] = string(t)
		}
		items["type"] = types
	}
	if el.Format != "" && c.kind != kindTimeTZ {
		items["format"] = string(el.Format)
	}
	if el.AirbyteType != "" {
		items["airbyte_type"] = string(el.AirbyteType)
	}
	return synclet.PropertySpec{PropertyType: nullable(synclet.Array), Items: items}
}

// elementKind is the kind array elements are emitted as: bytea and
// unmapped elements are cast to text.
func (c colType) elementKind() kind {
	if c.kind == kindBytea {
		return kindOther
	}
	return c.kind
}

// expr is the SQL expression that renders column ref so that to_json emits
// the value its schema declares.
func (c colType) expr(ref string) string {
	const nonFinite = `::text IN ('NaN', 'Infinity', '-Infinity')`
	switch {
	case c.array && c.kind == kindFloat:
		return fmt.Sprintf(`CASE WHEN %[1]s IS NULL THEN NULL ELSE ARRAY(SELECT CASE WHEN e%[2]s THEN NULL ELSE e END FROM unnest(%[1]s) WITH ORDINALITY AS u(e, i) ORDER BY i) END`, ref, nonFinite)
	case c.array && c.elementKind() == kindOther:
		return ref + "::text[]"
	case c.array:
		return ref
	case c.kind == kindFloat:
		return fmt.Sprintf(`CASE WHEN %[1]s%[2]s THEN NULL ELSE %[1]s END`, ref, nonFinite)
	case c.kind == kindTimeTZ:
		return fmt.Sprintf(`regexp_replace(%s::text, '([+-][0-9]{2})$', '\1:00')`, ref)
	case c.kind == kindBytea:
		return fmt.Sprintf(`translate(encode(%s, 'base64'), E'\n', '')`, ref)
	case c.kind == kindOther:
		return ref + "::text"
	default:
		return ref
	}
}
