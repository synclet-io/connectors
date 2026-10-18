package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"math"
	"time"
)

// scanRow scans a single row from sql.Rows into a map keyed by column names.
func scanRow(rows *sql.Rows, columns []string) (map[string]interface{}, error) {
	values := make([]interface{}, len(columns))
	ptrs := make([]interface{}, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}

	if err := rows.Scan(ptrs...); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	record := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		record[col] = convertSQLValue(values[i])
	}
	return record, nil
}

// convertSQLValue converts a database/sql scanned value to a JSON-safe representation.
func convertSQLValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil
		}
		return val
	case float32:
		f := float64(val)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return f
	case bool:
		return val
	case int64:
		return val
	default:
		return val
	}
}

// convertCDCValue converts a go-mysql canal row event value to a JSON-safe representation.
// The dataType parameter is the MySQL column data type used to determine encoding.
func convertCDCValue(v interface{}, dataType string) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case []byte:
		if isBinaryType(dataType) {
			return base64.StdEncoding.EncodeToString(val)
		}
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	case float32:
		f := float64(val)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return f
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil
		}
		return val
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		// Keep as uint64 for values exceeding int64 range.
		return val
	case bool:
		return val
	case string:
		return val
	default:
		// decimal.Decimal and other types — use string representation.
		return fmt.Sprintf("%v", val)
	}
}
