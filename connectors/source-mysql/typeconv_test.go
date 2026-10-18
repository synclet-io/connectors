package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestMapMySQLTypeToAirbyte(t *testing.T) {
	tests := []struct {
		name     string
		col      ColumnInfo
		wantType synclet.PropTypes
		wantAT   synclet.AirbytePropType
	}{
		{
			name:     "tinyint(1) is integer",
			col:      ColumnInfo{DataType: "tinyint", ColumnType: "tinyint(1)"},
			wantType: synclet.PropTypes{synclet.Integer},
		},
		{
			name:     "tinyint(4) is integer",
			col:      ColumnInfo{DataType: "tinyint", ColumnType: "tinyint(4)"},
			wantType: synclet.PropTypes{synclet.Integer},
		},
		{
			name:     "int is integer",
			col:      ColumnInfo{DataType: "int", ColumnType: "int"},
			wantType: synclet.PropTypes{synclet.Integer},
		},
		{
			name:     "bigint is integer",
			col:      ColumnInfo{DataType: "bigint", ColumnType: "bigint"},
			wantType: synclet.PropTypes{synclet.Integer},
		},
		{
			name:     "bigint unsigned is big_integer",
			col:      ColumnInfo{DataType: "bigint", ColumnType: "bigint unsigned"},
			wantType: synclet.PropTypes{synclet.Integer},
			wantAT:   synclet.BigInteger,
		},
		{
			name:     "float is number",
			col:      ColumnInfo{DataType: "float", ColumnType: "float"},
			wantType: synclet.PropTypes{synclet.Number},
		},
		{
			name:     "double is number",
			col:      ColumnInfo{DataType: "double", ColumnType: "double"},
			wantType: synclet.PropTypes{synclet.Number},
		},
		{
			name:     "decimal with scale 0 is big_integer",
			col:      ColumnInfo{DataType: "decimal", ColumnType: "decimal(10,0)", NumericScale: intPtr(0)},
			wantType: synclet.PropTypes{synclet.Integer},
			wantAT:   synclet.BigInteger,
		},
		{
			name:     "decimal with scale > 0 is big_number",
			col:      ColumnInfo{DataType: "decimal", ColumnType: "decimal(10,2)", NumericScale: intPtr(2)},
			wantType: synclet.PropTypes{synclet.Number},
			wantAT:   synclet.BigNumber,
		},
		{
			name:     "varchar is string",
			col:      ColumnInfo{DataType: "varchar", ColumnType: "varchar(255)"},
			wantType: synclet.PropTypes{synclet.String},
		},
		{
			name:     "text is string",
			col:      ColumnInfo{DataType: "text", ColumnType: "text"},
			wantType: synclet.PropTypes{synclet.String},
		},
		{
			name:     "datetime is timestamp_without_timezone",
			col:      ColumnInfo{DataType: "datetime", ColumnType: "datetime"},
			wantType: synclet.PropTypes{synclet.String},
			wantAT:   synclet.TimestampWOTZ,
		},
		{
			name:     "timestamp is timestamp_with_timezone",
			col:      ColumnInfo{DataType: "timestamp", ColumnType: "timestamp"},
			wantType: synclet.PropTypes{synclet.String},
			wantAT:   synclet.TimestampWithTZ,
		},
		{
			name:     "date is string",
			col:      ColumnInfo{DataType: "date", ColumnType: "date"},
			wantType: synclet.PropTypes{synclet.String},
		},
		{
			name:     "json is string",
			col:      ColumnInfo{DataType: "json", ColumnType: "json"},
			wantType: synclet.PropTypes{synclet.String},
		},
		{
			name:     "blob is string",
			col:      ColumnInfo{DataType: "blob", ColumnType: "blob"},
			wantType: synclet.PropTypes{synclet.String},
		},
		{
			name:     "enum is string",
			col:      ColumnInfo{DataType: "enum", ColumnType: "enum('a','b')"},
			wantType: synclet.PropTypes{synclet.String},
		},
		{
			name:     "year is integer",
			col:      ColumnInfo{DataType: "year", ColumnType: "year"},
			wantType: synclet.PropTypes{synclet.Integer},
		},
		{
			name:     "geometry is string",
			col:      ColumnInfo{DataType: "geometry", ColumnType: "geometry"},
			wantType: synclet.PropTypes{synclet.String},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapMySQLTypeToAirbyte(tt.col)
			assert.Equal(t, tt.wantType, got.Type)
			assert.Equal(t, tt.wantAT, got.AirbyteType)
		})
	}
}

func TestIsBinaryType(t *testing.T) {
	assert.True(t, isBinaryType("blob"))
	assert.True(t, isBinaryType("BINARY"))
	assert.True(t, isBinaryType("varbinary"))
	assert.False(t, isBinaryType("varchar"))
	assert.False(t, isBinaryType("text"))
}

func intPtr(i int) *int {
	return &i
}
