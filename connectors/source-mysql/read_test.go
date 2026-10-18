package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdent(t *testing.T) {
	require.Equal(t, "`plain`", ident("plain"))
	require.Equal(t, "`my.db`.`we``ird`", ident("my.db", "we`ird"))
}

func TestSelectQuery(t *testing.T) {
	tbl := &table{schema: "my.db", name: "we`ird", columns: []column{
		{name: "id", kind: kindInt},
		{name: "a.b", kind: kindString},
		{name: "shape", kind: kindGeometry},
	}}

	full, args := selectQuery{table: tbl, columns: tbl.columns}.sql()
	require.Empty(t, args)
	require.Equal(t, "SELECT `id` AS `id`, `a.b` AS `a.b`, ST_AsText(`shape`) AS `shape` FROM `my.db`.`we``ird`", full)

	cursor := column{name: "up`d", kind: kindDatetime}
	first, args := selectQuery{table: tbl, columns: tbl.columns[:1], cursor: &cursor}.sql()
	require.Empty(t, args)
	require.Equal(t, "SELECT `id` AS `id`, `up``d` FROM `my.db`.`we``ird` ORDER BY `up``d`", first)

	from := "2024-01-02 00:00:00"
	resumed, args := selectQuery{table: tbl, columns: tbl.columns[:1], cursor: &cursor, from: &from}.sql()
	require.Equal(t, []any{from}, args)
	require.Equal(t, "SELECT `id` AS `id`, `up``d` FROM `my.db`.`we``ird` WHERE `up``d` >= ? ORDER BY `up``d`", resumed)
}

func TestSnapshotQuery(t *testing.T) {
	tbl := &table{schema: "db", name: "t`x", columns: []column{{name: "a"}, {name: "b.c"}}, primaryKey: []string{"a", "b.c"}}
	q, args := snapshotQuery(tbl, nil)
	require.Empty(t, args)
	require.Equal(t, "SELECT `a` AS `a`, `b.c` AS `b.c` FROM `db`.`t``x` ORDER BY `a`, `b.c` LIMIT 10000", q)

	q, args = snapshotQuery(tbl, map[string]any{"a": "1", "b.c": json.Number("2")})
	require.Equal(t, []any{"1", json.Number("2")}, args)
	require.Equal(t, "SELECT `a` AS `a`, `b.c` AS `b.c` FROM `db`.`t``x` WHERE (`a`, `b.c`) > (?, ?) ORDER BY `a`, `b.c` LIMIT 10000", q)

	tbl.primaryKey = nil
	q, args = snapshotQuery(tbl, nil)
	require.Empty(t, args)
	require.Equal(t, "SELECT `a` AS `a`, `b.c` AS `b.c` FROM `db`.`t``x`", q)
}

func TestNewColumn(t *testing.T) {
	cases := []struct {
		dataType, columnType string
		want                 column
	}{
		{"tinyint", "tinyint(1)", column{name: "c", kind: kindInt}},
		{"bigint", "bigint unsigned", column{name: "c", kind: kindInt}},
		{"bit", "bit(1)", column{name: "c", kind: kindBool}},
		{"bit", "bit(12)", column{name: "c", kind: kindBit}},
		{"decimal", "decimal(65,30)", column{name: "c", kind: kindDecimal, scale: 30}},
		{"decimal", "decimal(10,0)", column{name: "c", kind: kindDecimal}},
		{"enum", "enum('a','it''s')", column{name: "c", kind: kindEnum, values: []string{"a", "it's"}}},
		{"set", "set('x','y,z')", column{name: "c", kind: kindSet, values: []string{"x", "y,z"}}},
		{"geomcollection", "geomcollection", column{name: "c", kind: kindGeometry}},
		{"vector", "vector(3)", column{name: "c", kind: kindOther}},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, newColumn("c", tc.dataType, tc.columnType), tc.columnType)
	}
}

func TestEncode(t *testing.T) {
	cases := []struct {
		kind kind
		raw  string
		want string
	}{
		{kindInt, "18446744073709551615", `18446744073709551615`},
		{kindDecimal, "-0.10", `-0.10`},
		{kindFloat, "1e-7", `1e-7`},
		{kindBool, "\x01", `true`},
		{kindBool, "\x00", `false`},
		{kindBit, "\x0a\xaa", `2730`},
		{kindBit, "\xff\xff\xff\xff\xff\xff\xff\xff", `18446744073709551615`},
		{kindString, "a\"b 🎉", `"a\"b 🎉"`},
		{kindJSON, `{"a": [1, 2.50]}`, `{"a": [1, 2.50]}`},
		{kindBinary, "\x00\xff\x10", `"AP8Q"`},
		{kindDate, "2024-02-29", `"2024-02-29"`},
		{kindDatetime, "2024-02-29 13:14:15.123456", `"2024-02-29T13:14:15.123456"`},
		{kindTimestamp, "2024-02-29 13:14:15", `"2024-02-29T13:14:15Z"`},
		{kindTime, "-838:59:59.000000", `"-838:59:59.000000"`},
		{kindGeometry, "POINT(1 2)", `"POINT(1 2)"`},
		{kindDate, "0000-00-00", `null`},
		{kindDate, "2024-00-15", `null`},
		{kindDatetime, "2024-03-00 10:00:00", `null`},
		{kindTimestamp, "0000-00-00 00:00:00", `null`},
		{kindDate, "0000-01-01", `"0000-01-01"`},
		{kindInt, "00042", `42`},
		{kindInt, "00000", `0`},
		{kindDecimal, "0001.50", `1.50`},
		{kindDecimal, "0000.50", `0.50`},
		{kindFloat, "0000001.5", `1.5`},
		{kindFloat, "-0.5", `-0.5`},
	}
	for _, tc := range cases {
		got, err := column{kind: tc.kind}.encode([]byte(tc.raw), false)
		require.NoError(t, err, tc.raw)
		require.Equal(t, tc.want, string(got), tc.raw)
	}
	got, err := column{kind: kindString}.encode(nil, false)
	require.NoError(t, err)
	require.Equal(t, `null`, string(got))

	_, err = column{kind: kindDatetime}.encode([]byte("0000-00-00 00:00:00"), true)
	require.ErrorIs(t, err, errZeroDate)
	_, err = column{kind: kindDate}.encode([]byte("2024-01-02"), true)
	require.NoError(t, err)
}

type stringer string

func (s stringer) String() string { return string(s) }

// fixedDecimal renders like shopspring's decimal.Decimal: String drops
// trailing zeros, StringFixed keeps the column's scale.
type fixedDecimal struct{}

func (fixedDecimal) String() string             { return "0.1" }
func (fixedDecimal) StringFixed(n int32) string { return "0.1" + strings.Repeat("0", int(n)-1) }

func TestEncodeCDC(t *testing.T) {
	cases := []struct {
		col  column
		v    any
		want string
	}{
		{column{kind: kindInt}, int8(-128), `-128`},
		{column{kind: kindInt}, uint64(18446744073709551615), `18446744073709551615`},
		{column{kind: kindBool}, int64(1), `true`},
		{column{kind: kindBool}, int64(0), `false`},
		{column{kind: kindBit}, int64(-1), `18446744073709551615`},
		{column{kind: kindEnum, values: []string{"small", "it's big"}}, int64(2), `"it's big"`},
		{column{kind: kindSet, values: []string{"a", "b", "c"}}, int64(5), `"a,c"`},
		{column{kind: kindDecimal}, stringer("0.10"), `0.10`},
		{column{kind: kindDecimal, scale: 3}, fixedDecimal{}, `0.100`},
		{column{kind: kindFloat}, float32(1.1), `1.1`},
		{column{kind: kindFloat}, float64(3.141592653589793), `3.141592653589793`},
		{column{kind: kindBinary}, "\x00\xff\x10", `"AP8Q"`},
		{column{kind: kindBinary}, []byte("blob"), `"YmxvYg=="`},
		{column{kind: kindJSON}, `{"a":1}`, `{"a":1}`},
		{column{kind: kindDatetime}, "2024-02-29 13:14:15.123456", `"2024-02-29T13:14:15.123456"`},
		{column{kind: kindTimestamp}, "2024-02-29 11:14:15.120", `"2024-02-29T11:14:15.120Z"`},
		{column{kind: kindDate}, "0000-00-00", `null`},
		{column{kind: kindGeometry}, []byte{0, 0, 0, 0, 1}, `"AAAAAAE="`},
		{column{kind: kindString}, nil, `null`},
	}
	for _, tc := range cases {
		got, err := tc.col.encodeCDC(tc.v, false)
		require.NoError(t, err, "%v", tc.v)
		require.Equal(t, tc.want, string(got), "%v", tc.v)
	}
	_, err := column{kind: kindDate}.encodeCDC("0000-00-00", true)
	require.ErrorIs(t, err, errZeroDate)
}

func TestLegacyCursor(t *testing.T) {
	require.Nil(t, legacyCursor(column{kind: kindInt}, nil))
	require.Equal(t, "12345678901234567.89", *legacyCursor(column{kind: kindDecimal}, json.Number("12345678901234567.89")))
	require.Equal(t, "2024-01-02 03:04:05", *legacyCursor(column{kind: kindDatetime}, "2024-01-02T03:04:05Z"))
	require.Equal(t, "2024-01-02 01:04:05.5", *legacyCursor(column{kind: kindTimestamp}, "2024-01-02T03:04:05.5+02:00"))
	require.Equal(t, "abc", *legacyCursor(column{kind: kindString}, "abc"))
	require.Equal(t, "1", *legacyCursor(column{kind: kindBool}, true))
	require.Equal(t, "YWJjZA==", *legacyCursor(column{kind: kindBinary}, "abcd"))
	require.Equal(t, "3840", *legacyCursor(column{kind: kindBit}, "\x0f\x00"))
	require.Equal(t, "3840", *legacyCursor(column{kind: kindBit}, json.Number("3840")))
}
