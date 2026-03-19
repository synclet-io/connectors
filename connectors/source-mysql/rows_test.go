package main

import (
	"encoding/base64"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConvertSQLValue(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  interface{}
	}{
		{"nil", nil, nil},
		{"string bytes", []byte("hello"), "hello"},
		{"int64", int64(42), int64(42)},
		{"float64", float64(3.14), float64(3.14)},
		{"float64 NaN", math.NaN(), nil},
		{"float64 Inf", math.Inf(1), nil},
		{"bool true", true, true},
		{"time", time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC), "2024-01-15T12:30:00Z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertSQLValue(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConvertCDCValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		dataType string
		want     interface{}
	}{
		{"nil", nil, "varchar", nil},
		{"string", "hello", "varchar", "hello"},
		{"int8", int8(42), "tinyint", int64(42)},
		{"int16", int16(1000), "smallint", int64(1000)},
		{"int32", int32(100000), "int", int64(100000)},
		{"int64", int64(1234567890), "bigint", int64(1234567890)},
		{"uint64", uint64(18446744073709551615), "bigint unsigned", uint64(18446744073709551615)},
		{"float32", float32(3.14), "float", float64(float32(3.14))},
		{"float64", float64(3.14159), "double", float64(3.14159)},
		{"bytes text", []byte("text data"), "varchar", "text data"},
		{"bytes binary", []byte{0x01, 0x02, 0x03}, "blob", base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03})},
		{"time", time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC), "datetime", "2024-06-15T14:30:00Z"},
		{"bool", true, "tinyint", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertCDCValue(tt.input, tt.dataType)
			assert.Equal(t, tt.want, got)
		})
	}
}
