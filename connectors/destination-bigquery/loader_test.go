package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeJSONL: both loaders send a null as null and an object or array as
// JSON, which a JSON column takes, where text formatting of the values would
// write <nil> and map[...].
func TestEncodeJSONL(t *testing.T) {
	buf, err := encodeJSONL([]map[string]interface{}{
		{"error_code": nil, "order_metadata": map[string]interface{}{"campaign": "test"}, "amount": json.Number("1000")},
		{"transactions": []interface{}{map[string]interface{}{"id": "t1"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, `{"amount":1000,"error_code":null,"order_metadata":{"campaign":"test"}}`+"\n"+
		`{"transactions":[{"id":"t1"}]}`+"\n", buf.String())
}
