package main

import (
	"math"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAirbyteTypeToBigQuery(t *testing.T) {
	tests := []struct {
		name       string
		airbyteType string
		format     string
		expected   bigquery.FieldType
	}{
		{"boolean", "boolean", "", bigquery.BooleanFieldType},
		{"integer", "integer", "", bigquery.IntegerFieldType},
		{"number", "number", "", bigquery.NumericFieldType},
		{"string default", "string", "", bigquery.StringFieldType},
		{"string date", "string", "date", bigquery.DateFieldType},
		{"string date-time", "string", "date-time", bigquery.TimestampFieldType},
		{"string time", "string", "time", bigquery.TimeFieldType},
		{"string time-with-timezone", "string", "time-with-timezone", bigquery.StringFieldType},
		{"array", "array", "", bigquery.JSONFieldType},
		{"object", "object", "", bigquery.JSONFieldType},
		{"unknown", "unknown", "", bigquery.JSONFieldType},
		{"empty type", "", "", bigquery.JSONFieldType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := airbyteTypeToBigQuery(tt.airbyteType, tt.format)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateAndCoerce(t *testing.T) {
	t.Run("integer valid max", func(t *testing.T) {
		val, err := validateAndCoerce("f", int64(math.MaxInt64), bigquery.IntegerFieldType)
		require.Nil(t, err)
		assert.Equal(t, int64(math.MaxInt64), val)
	})

	t.Run("integer valid from float64", func(t *testing.T) {
		val, err := validateAndCoerce("f", float64(42), bigquery.IntegerFieldType)
		require.Nil(t, err)
		assert.Equal(t, int64(42), val)
	})

	t.Run("integer overflow from float64", func(t *testing.T) {
		val, err := validateAndCoerce("f", float64(1e19), bigquery.IntegerFieldType)
		assert.Nil(t, val)
		require.NotNil(t, err)
		assert.Equal(t, reasonFieldSizeLimitation, err.reason)
	})

	t.Run("integer negative overflow from float64", func(t *testing.T) {
		val, err := validateAndCoerce("f", float64(-1e19), bigquery.IntegerFieldType)
		assert.Nil(t, val)
		require.NotNil(t, err)
		assert.Equal(t, reasonFieldSizeLimitation, err.reason)
	})

	t.Run("numeric valid", func(t *testing.T) {
		val, err := validateAndCoerce("f", float64(123.456), bigquery.NumericFieldType)
		require.Nil(t, err)
		assert.Equal(t, float64(123.456), val)
	})

	t.Run("numeric rounds to 9 decimal places", func(t *testing.T) {
		val, err := validateAndCoerce("f", float64(1.123456789012), bigquery.NumericFieldType)
		require.Nil(t, err)
		assert.InDelta(t, 1.123456789, val.(float64), 1e-12)
	})

	t.Run("numeric overflow", func(t *testing.T) {
		val, err := validateAndCoerce("f", float64(1e30), bigquery.NumericFieldType)
		assert.Nil(t, val)
		require.NotNil(t, err)
		assert.Equal(t, reasonFieldSizeLimitation, err.reason)
	})

	t.Run("date valid", func(t *testing.T) {
		val, err := validateAndCoerce("f", "2024-01-15", bigquery.DateFieldType)
		require.Nil(t, err)
		assert.Equal(t, "2024-01-15", val)
	})

	t.Run("date out of range year 0000", func(t *testing.T) {
		val, err := validateAndCoerce("f", "0000-01-01", bigquery.DateFieldType)
		assert.Nil(t, val)
		require.NotNil(t, err)
		assert.Equal(t, reasonSerializationError, err.reason)
	})

	t.Run("timestamp valid", func(t *testing.T) {
		val, err := validateAndCoerce("f", "2024-01-15T10:30:00Z", bigquery.TimestampFieldType)
		require.Nil(t, err)
		assert.Equal(t, "2024-01-15T10:30:00Z", val)
	})

	t.Run("timestamp out of range year 10000", func(t *testing.T) {
		val, err := validateAndCoerce("f", "10000-01-01T00:00:00Z", bigquery.TimestampFieldType)
		assert.Nil(t, val)
		require.NotNil(t, err)
		assert.Equal(t, reasonSerializationError, err.reason)
	})

	t.Run("timestamp RFC3339Nano", func(t *testing.T) {
		val, err := validateAndCoerce("f", "2024-01-15T10:30:00.123456Z", bigquery.TimestampFieldType)
		require.Nil(t, err)
		assert.Equal(t, "2024-01-15T10:30:00.123456Z", val)
	})

	t.Run("time valid", func(t *testing.T) {
		val, err := validateAndCoerce("f", "10:30:00", bigquery.TimeFieldType)
		require.Nil(t, err)
		assert.Equal(t, "10:30:00", val)
	})

	t.Run("time with microseconds", func(t *testing.T) {
		val, err := validateAndCoerce("f", "10:30:00.123456", bigquery.TimeFieldType)
		require.Nil(t, err)
		assert.Equal(t, "10:30:00.123456", val)
	})

	t.Run("string passthrough", func(t *testing.T) {
		val, err := validateAndCoerce("f", "hello", bigquery.StringFieldType)
		require.Nil(t, err)
		assert.Equal(t, "hello", val)
	})

	t.Run("json passthrough", func(t *testing.T) {
		val, err := validateAndCoerce("f", map[string]interface{}{"a": 1}, bigquery.JSONFieldType)
		require.Nil(t, err)
		assert.NotNil(t, val)
	})

	t.Run("nil passthrough", func(t *testing.T) {
		val, err := validateAndCoerce("f", nil, bigquery.IntegerFieldType)
		require.Nil(t, err)
		assert.Nil(t, val)
	})
}

func TestIsWideningConversion(t *testing.T) {
	tests := []struct {
		name     string
		from     bigquery.FieldType
		to       bigquery.FieldType
		expected bool
	}{
		{"INT64 to NUMERIC", bigquery.IntegerFieldType, bigquery.NumericFieldType, true},
		{"INT64 to FLOAT64", bigquery.IntegerFieldType, bigquery.FloatFieldType, true},
		{"INT64 to STRING", bigquery.IntegerFieldType, bigquery.StringFieldType, true},
		{"NUMERIC to STRING", bigquery.NumericFieldType, bigquery.StringFieldType, true},
		{"FLOAT64 to STRING", bigquery.FloatFieldType, bigquery.StringFieldType, true},
		{"DATE to STRING", bigquery.DateFieldType, bigquery.StringFieldType, true},
		{"TIMESTAMP to STRING", bigquery.TimestampFieldType, bigquery.StringFieldType, true},
		{"TIME to STRING", bigquery.TimeFieldType, bigquery.StringFieldType, true},
		{"STRING to INT64 not widening", bigquery.StringFieldType, bigquery.IntegerFieldType, false},
		{"same type", bigquery.StringFieldType, bigquery.StringFieldType, false},
		{"BOOLEAN to STRING not widening", bigquery.BooleanFieldType, bigquery.StringFieldType, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isWideningConversion(tt.from, tt.to))
		})
	}
}

func TestValidationError(t *testing.T) {
	err := &validationError{field: "age", reason: reasonFieldSizeLimitation}
	assert.Contains(t, err.Error(), "age")
	assert.Contains(t, err.Error(), reasonFieldSizeLimitation)
}
