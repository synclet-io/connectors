package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectQuery(t *testing.T) {
	tbl := &table{schema: "my schema", name: `we]ird`, columns: []column{
		{name: "id", typ: colType{name: "int"}},
		{name: "amount", typ: colType{name: "decimal", precision: 18, scale: 2}},
		{name: "at", typ: colType{name: "datetime2", scale: 7}},
	}}

	full, args := selectQuery{table: tbl, columns: tbl.columns}.sql()
	require.Empty(t, args)
	require.Equal(t, `SELECT _synclet_t.[id], CONVERT(varchar(48), _synclet_t.[amount]), CONVERT(varchar(27), _synclet_t.[at], 126) `+
		`FROM [my schema].[we]]ird] AS _synclet_t`, full)

	cursor := tbl.columns[2]
	first, args := selectQuery{table: tbl, columns: tbl.columns[:1], cursor: &cursor}.sql()
	require.Empty(t, args)
	require.Equal(t, `SELECT _synclet_t.[id], CONVERT(varchar(27), _synclet_t.[at], 126) FROM [my schema].[we]]ird] AS _synclet_t `+
		`ORDER BY _synclet_t.[at]`, first)

	from := "2024-01-02T00:00:00.1234567"
	resumed, args := selectQuery{table: tbl, columns: tbl.columns[:1], cursor: &cursor, from: &from}.sql()
	require.Equal(t, []any{from}, args)
	require.Equal(t, `SELECT _synclet_t.[id], CONVERT(varchar(27), _synclet_t.[at], 126) FROM [my schema].[we]]ird] AS _synclet_t `+
		`WHERE _synclet_t.[at] >= CONVERT(datetime2(7), @p1, 126) ORDER BY _synclet_t.[at]`, resumed)
}

func TestColumnExpr(t *testing.T) {
	cases := []struct {
		typ                 colType
		expr, cursor, param string
	}{
		{colType{name: "bit"}, `c`, `CONVERT(nvarchar(max), c)`, `CONVERT(bit, @p1)`},
		{colType{name: "bigint"}, `c`, `CONVERT(nvarchar(max), c)`, `CONVERT(bigint, @p1)`},
		{colType{name: "decimal", precision: 38, scale: 10}, `CONVERT(varchar(48), c)`, `CONVERT(varchar(48), c)`, `CONVERT(decimal(38,10), @p1)`},
		{colType{name: "money"}, `CONVERT(varchar(32), c, 2)`, `CONVERT(varchar(32), c, 2)`, `CONVERT(money, @p1)`},
		{colType{name: "float"}, `c`, `CONVERT(varchar(30), c, 3)`, `CONVERT(float, @p1)`},
		{colType{name: "real"}, `c`, `CONVERT(varchar(30), c, 3)`, `CONVERT(real, @p1)`},
		{colType{name: "nvarchar", maxLength: 40}, `c`, `CONVERT(nvarchar(max), c)`, `CONVERT(nvarchar(20), @p1)`},
		{colType{name: "varchar", maxLength: -1}, `c`, `CONVERT(nvarchar(max), c)`, `CONVERT(varchar(max), @p1)`},
		{colType{name: "xml"}, `CONVERT(nvarchar(max), c)`, `CONVERT(nvarchar(max), c)`, `CONVERT(xml, @p1)`},
		{colType{name: "uniqueidentifier"}, `LOWER(CONVERT(char(36), c))`, `LOWER(CONVERT(char(36), c))`, `CONVERT(uniqueidentifier, @p1)`},
		{colType{name: "varbinary", maxLength: 16}, `c`, `CONVERT(varchar(max), CONVERT(varbinary(max), c), 1)`, `CONVERT(varbinary(16), @p1, 1)`},
		{colType{name: "timestamp", maxLength: 8}, `c`, `CONVERT(varchar(max), CONVERT(varbinary(max), c), 1)`, `CONVERT(binary(8), @p1, 1)`},
		{colType{name: "date"}, `CONVERT(char(10), c, 23)`, `CONVERT(char(10), c, 23)`, `CONVERT(date, @p1, 23)`},
		{colType{name: "time", scale: 7}, `CONVERT(varchar(16), c)`, `CONVERT(varchar(16), c)`, `CONVERT(time(7), @p1)`},
		{colType{name: "datetime"}, `CONVERT(varchar(27), c, 126)`, `CONVERT(varchar(27), c, 126)`, `CONVERT(datetime, @p1, 126)`},
		{colType{name: "datetimeoffset", scale: 3}, `CONVERT(varchar(34), c, 126)`, `CONVERT(varchar(34), c, 126)`, `CONVERT(datetimeoffset(3), @p1, 126)`},
		{colType{name: "hierarchyid"}, `c.ToString()`, `c.ToString()`, `hierarchyid::Parse(@p1)`},
		{colType{name: "geography"}, `c.STAsText()`, `c.STAsText()`, `CONVERT(geography, @p1)`},
		{colType{name: "my_clr_type"}, `CONVERT(nvarchar(max), c)`, `CONVERT(nvarchar(max), c)`, `CONVERT(my_clr_type, @p1)`},
	}
	for _, tc := range cases {
		require.Equal(t, tc.expr, tc.typ.expr("c"), tc.typ.name)
		require.Equal(t, tc.cursor, tc.typ.cursorExpr("c"), tc.typ.name)
		require.Equal(t, tc.param, tc.typ.cursorParam("@p1"), tc.typ.name)
	}
}

func TestOrderable(t *testing.T) {
	for _, name := range []string{"int", "datetime2", "timestamp", "uniqueidentifier", "nvarchar", "decimal", "hierarchyid", "date"} {
		require.True(t, colType{name: name}.orderable(), name)
	}
	for _, name := range []string{"xml", "text", "ntext", "image", "geography", "geometry", "sql_variant", "my_clr_type"} {
		require.False(t, colType{name: name}.orderable(), name)
	}
}

// TestColumnSchema maps every SQL Server type to its JSON schema.
func TestColumnSchema(t *testing.T) {
	const (
		str = `{"type":["string","null"]}`
		num = `{"type":["number","null"]}`
		i   = `{"type":["integer","null"]}`
	)
	cases := map[string]string{
		"bit":              `{"type":["boolean","null"]}`,
		"tinyint":          i,
		"smallint":         i,
		"int":              i,
		"bigint":           i,
		"decimal":          num,
		"numeric":          num,
		"money":            num,
		"smallmoney":       num,
		"real":             num,
		"float":            num,
		"char":             str,
		"varchar":          str,
		"nchar":            str,
		"nvarchar":         str,
		"sysname":          str,
		"text":             str,
		"ntext":            str,
		"xml":              str,
		"sql_variant":      str,
		"uniqueidentifier": str,
		"binary":           str,
		"varbinary":        str,
		"image":            str,
		"timestamp":        str,
		"date":             `{"type":["string","null"],"format":"date"}`,
		"time":             `{"type":["string","null"],"airbyte_type":"time_without_timezone"}`,
		"datetime":         `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`,
		"datetime2":        `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`,
		"smalldatetime":    `{"type":["string","null"],"airbyte_type":"timestamp_without_timezone"}`,
		"datetimeoffset":   `{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"}`,
		"hierarchyid":      str,
		"geography":        str,
		"geometry":         str,
		"my_clr_type":      str,
	}
	for name := range builtinKinds {
		require.Contains(t, cases, name, "every mapped type has a case")
	}
	for name, want := range cases {
		got, err := json.Marshal(colType{name: name}.schema())
		require.NoError(t, err)
		require.JSONEq(t, want, string(got), name)
	}
}

func TestAppendValue(t *testing.T) {
	cases := []struct {
		kind kind
		v    any
		want string
	}{
		{kindInt, nil, `null`},
		{kindBool, true, `true`},
		{kindInt, int64(9007199254740993), `9007199254740993`},
		{kindFloat, float64(1.5), `1.5`},
		{kindFloat, float32(0.1), `0.1`},
		{kindReal, float64(float32(0.1)), `0.1`},
		{kindDecimal, "1234567890123456789012345678.0123456789", `1234567890123456789012345678.0123456789`},
		{kindDecimal, "-.50", `-0.50`},
		{kindMoney, ".5000", `0.5000`},
		{kindBinary, []byte{0x00, 0xff, 0x10}, `"AP8Q"`},
		{kindString, "héllo \"x\"", `"héllo \"x\""`},
		{kindString, []byte("raw"), `"raw"`},
	}
	for _, tc := range cases {
		got, err := appendValue(nil, tc.kind, tc.v)
		require.NoError(t, err)
		require.Equal(t, tc.want, string(got))
	}
	_, err := appendValue(nil, kindDecimal, "abc")
	require.ErrorContains(t, err, "not a number")
}

func TestRecordWriter(t *testing.T) {
	w, err := newRecordWriter([]column{{name: "id", typ: colType{name: "int"}}, {name: `n"ame`, typ: colType{name: "nvarchar"}}})
	require.NoError(t, err)
	got, err := w.write([]any{int64(1), "a"})
	require.NoError(t, err)
	require.Equal(t, `{"id":1,"n\"ame":"a"}`, string(got))
	got, err = w.write([]any{int64(2), nil})
	require.NoError(t, err)
	require.Equal(t, `{"id":2,"n\"ame":null}`, string(got))
}
