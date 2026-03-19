package main

import (
	"fmt"
	"math"
	"time"

	"cloud.google.com/go/bigquery"
)

const (
	reasonFieldSizeLimitation  = "DESTINATION_FIELD_SIZE_LIMITATION"
	reasonSerializationError   = "DESTINATION_SERIALIZATION_ERROR"

	// numericMaxIntDigits is the max integer digits for NUMERIC(38,9): 10^29 - 1.
	numericMaxIntDigits = 29
	numericMaxScale     = 9
)

// validationError represents a field-level validation failure during type coercion.
type validationError struct {
	field  string
	reason string
}

func (e *validationError) Error() string {
	return fmt.Sprintf("validation error for field %s: %s", e.field, e.reason)
}

// airbyteTypeToBigQuery maps Airbyte JSON schema types to BigQuery field types.
func airbyteTypeToBigQuery(airbyteType string, format string) bigquery.FieldType {
	switch airbyteType {
	case "boolean":
		return bigquery.BooleanFieldType
	case "integer":
		return bigquery.IntegerFieldType
	case "number":
		return bigquery.NumericFieldType
	case "string":
		switch format {
		case "date":
			return bigquery.DateFieldType
		case "date-time":
			return bigquery.TimestampFieldType
		case "time":
			return bigquery.TimeFieldType
		case "time-with-timezone":
			return bigquery.StringFieldType
		default:
			return bigquery.StringFieldType
		}
	case "array", "object":
		return bigquery.JSONFieldType
	default:
		return bigquery.JSONFieldType
	}
}

// validateAndCoerce validates a value against BigQuery type constraints
// and coerces it if needed (e.g. rounding NUMERIC to 9 decimal places).
// Returns nil value + validationError if value is out of range.
func validateAndCoerce(fieldName string, value interface{}, bqType bigquery.FieldType) (interface{}, *validationError) {
	if value == nil {
		return nil, nil
	}

	switch bqType {
	case bigquery.IntegerFieldType:
		return coerceInteger(fieldName, value)
	case bigquery.NumericFieldType:
		return coerceNumeric(fieldName, value)
	case bigquery.DateFieldType:
		return coerceDate(fieldName, value)
	case bigquery.TimestampFieldType:
		return coerceTimestamp(fieldName, value)
	case bigquery.TimeFieldType:
		return coerceTime(fieldName, value)
	default:
		return value, nil
	}
}

func coerceInteger(fieldName string, value interface{}) (interface{}, *validationError) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case float64:
		if v > float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return nil, &validationError{
				field:  fieldName,
				reason: reasonFieldSizeLimitation,
			}
		}
		return int64(v), nil
	case int:
		return int64(v), nil
	default:
		return value, nil
	}
}

func coerceNumeric(fieldName string, value interface{}) (interface{}, *validationError) {
	v, ok := value.(float64)
	if !ok {
		return value, nil
	}

	// Check NUMERIC(38,9) range: absolute value must be < 10^29.
	if math.Abs(v) >= 1e29 {
		return nil, &validationError{
			field:  fieldName,
			reason: reasonFieldSizeLimitation,
		}
	}

	// Round to 9 decimal places if needed.
	rounded := math.Round(v*1e9) / 1e9
	return rounded, nil
}

func coerceDate(fieldName string, value interface{}) (interface{}, *validationError) {
	s, ok := value.(string)
	if !ok {
		return value, nil
	}

	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return nil, &validationError{
			field:  fieldName,
			reason: reasonSerializationError,
		}
	}

	if t.Year() < 1 || t.Year() > 9999 {
		return nil, &validationError{
			field:  fieldName,
			reason: reasonSerializationError,
		}
	}

	return s, nil
}

func coerceTimestamp(fieldName string, value interface{}) (interface{}, *validationError) {
	s, ok := value.(string)
	if !ok {
		return value, nil
	}

	// Try RFC3339 first, then RFC3339Nano.
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, &validationError{
				field:  fieldName,
				reason: reasonSerializationError,
			}
		}
	}

	if t.Year() < 1 || t.Year() > 9999 {
		return nil, &validationError{
			field:  fieldName,
			reason: reasonSerializationError,
		}
	}

	return s, nil
}

func coerceTime(fieldName string, value interface{}) (interface{}, *validationError) {
	s, ok := value.(string)
	if !ok {
		return value, nil
	}

	// Try HH:MM:SS first, then with microseconds.
	_, err := time.Parse("15:04:05", s)
	if err != nil {
		_, err = time.Parse("15:04:05.000000", s)
		if err != nil {
			// Try flexible microsecond parsing.
			_, err = time.Parse("15:04:05.999999", s)
			if err != nil {
				return nil, &validationError{
					field:  fieldName,
					reason: reasonSerializationError,
				}
			}
		}
	}

	return s, nil
}

// isWideningConversion returns true if converting from one BigQuery type to another
// is a safe widening conversion (no data loss).
func isWideningConversion(from, to bigquery.FieldType) bool {
	widenings := map[bigquery.FieldType][]bigquery.FieldType{
		bigquery.IntegerFieldType:   {bigquery.NumericFieldType, bigquery.FloatFieldType, bigquery.StringFieldType},
		bigquery.NumericFieldType:   {bigquery.StringFieldType},
		bigquery.FloatFieldType:     {bigquery.StringFieldType},
		bigquery.DateFieldType:      {bigquery.StringFieldType},
		bigquery.TimestampFieldType: {bigquery.StringFieldType},
		bigquery.TimeFieldType:      {bigquery.StringFieldType},
	}

	targets, ok := widenings[from]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == to {
			return true
		}
	}
	return false
}
