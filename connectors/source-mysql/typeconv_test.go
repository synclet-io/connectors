package main

import (
	"testing"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
	"github.com/stretchr/testify/assert"
)

func TestMapMySQLTypeToAirbyte(t *testing.T) {
	tests := []struct {
		name     string
		col      ColumnInfo
		wantType []airbyte.PropType
		wantAT   airbyte.AirbytePropType
	}{
		{
			name:     "tinyint(1) is integer",
			col:      ColumnInfo{DataType: "tinyint", ColumnType: "tinyint(1)"},
			wantType: []airbyte.PropType{airbyte.Integer},
		},
		{
			name:     "tinyint(4) is integer",
			col:      ColumnInfo{DataType: "tinyint", ColumnType: "tinyint(4)"},
			wantType: []airbyte.PropType{airbyte.Integer},
		},
		{
			name:     "int is integer",
			col:      ColumnInfo{DataType: "int", ColumnType: "int"},
			wantType: []airbyte.PropType{airbyte.Integer},
		},
		{
			name:     "bigint is integer",
			col:      ColumnInfo{DataType: "bigint", ColumnType: "bigint"},
			wantType: []airbyte.PropType{airbyte.Integer},
		},
		{
			name:     "bigint unsigned is big_integer",
			col:      ColumnInfo{DataType: "bigint", ColumnType: "bigint unsigned"},
			wantType: []airbyte.PropType{airbyte.Integer},
			wantAT:   airbyte.BigInteger,
		},
		{
			name:     "float is number",
			col:      ColumnInfo{DataType: "float", ColumnType: "float"},
			wantType: []airbyte.PropType{airbyte.Number},
		},
		{
			name:     "double is number",
			col:      ColumnInfo{DataType: "double", ColumnType: "double"},
			wantType: []airbyte.PropType{airbyte.Number},
		},
		{
			name:     "decimal with scale 0 is big_integer",
			col:      ColumnInfo{DataType: "decimal", ColumnType: "decimal(10,0)", NumericScale: intPtr(0)},
			wantType: []airbyte.PropType{airbyte.Integer},
			wantAT:   airbyte.BigInteger,
		},
		{
			name:     "decimal with scale > 0 is big_number",
			col:      ColumnInfo{DataType: "decimal", ColumnType: "decimal(10,2)", NumericScale: intPtr(2)},
			wantType: []airbyte.PropType{airbyte.Number},
			wantAT:   airbyte.BigNumber,
		},
		{
			name:     "varchar is string",
			col:      ColumnInfo{DataType: "varchar", ColumnType: "varchar(255)"},
			wantType: []airbyte.PropType{airbyte.String},
		},
		{
			name:     "text is string",
			col:      ColumnInfo{DataType: "text", ColumnType: "text"},
			wantType: []airbyte.PropType{airbyte.String},
		},
		{
			name:     "datetime is timestamp_without_timezone",
			col:      ColumnInfo{DataType: "datetime", ColumnType: "datetime"},
			wantType: []airbyte.PropType{airbyte.String},
			wantAT:   airbyte.TimestampWOTZ,
		},
		{
			name:     "timestamp is timestamp_with_timezone",
			col:      ColumnInfo{DataType: "timestamp", ColumnType: "timestamp"},
			wantType: []airbyte.PropType{airbyte.String},
			wantAT:   airbyte.TimestampWithTZ,
		},
		{
			name:     "date is string",
			col:      ColumnInfo{DataType: "date", ColumnType: "date"},
			wantType: []airbyte.PropType{airbyte.String},
		},
		{
			name:     "json is string",
			col:      ColumnInfo{DataType: "json", ColumnType: "json"},
			wantType: []airbyte.PropType{airbyte.String},
		},
		{
			name:     "blob is string",
			col:      ColumnInfo{DataType: "blob", ColumnType: "blob"},
			wantType: []airbyte.PropType{airbyte.String},
		},
		{
			name:     "enum is string",
			col:      ColumnInfo{DataType: "enum", ColumnType: "enum('a','b')"},
			wantType: []airbyte.PropType{airbyte.String},
		},
		{
			name:     "year is integer",
			col:      ColumnInfo{DataType: "year", ColumnType: "year"},
			wantType: []airbyte.PropType{airbyte.Integer},
		},
		{
			name:     "geometry is string",
			col:      ColumnInfo{DataType: "geometry", ColumnType: "geometry"},
			wantType: []airbyte.PropType{airbyte.String},
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
