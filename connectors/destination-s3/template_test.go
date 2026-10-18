package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathTemplate(t *testing.T) {
	start := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		template  string
		namespace string
		record    string
		want      string
		prefix    string
	}{
		{"default", defaultPathTemplate, "shop", `{}`, "shop/orders/2026_10_18/P", "shop/orders/"},
		{"default without namespace", defaultPathTemplate, "", `{}`, "orders/2026_10_18/P", "orders/"},
		{"partition field", "{stream}/country={field:country}/{part}", "", `{"country":"UA"}`, "orders/country=UA/P", "orders/"},
		{"null partition", "{stream}/country={field:country}/{part}", "", `{"country":null}`, "orders/country=__null__/P", "orders/"},
		{"missing partition", "{stream}/country={field:country}/{part}", "", `{}`, "orders/country=__null__/P", "orders/"},
		{"empty partition", "{stream}/country={field:country}/{part}", "", `{"country":""}`, "orders/country=__empty__/P", "orders/"},
		{"escaped partition", "{stream}/{field:city}/{part}", "", `{"city":"a/b c"}`, "orders/a%2Fb%20c/P", "orders/"},
		{"dot-dot partition", "{stream}/{field:city}/{part}", "", `{"city":".."}`, "orders/%2E%2E/P", "orders/"},
		{"number and bool partitions", "{stream}/{field:n}/{field:b}/{part}", "", `{"n":12.5,"b":true}`, "orders/12.5/true/P", "orders/"},
		{"hour and literal", "raw/{namespace}_{stream}/{yyyy}/{mm}/{dd}/{hh}/file-{part}", "shop", `{}`, "raw/shop_orders/2026/10/18/09/file-P", "raw/shop_orders/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, err := parsePathTemplate(tc.template)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tc.record), &fields))
			rc := renderContext{namespace: tc.namespace, stream: "orders", start: start}
			got, err := tmpl.render(rc, fields, "P")
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.prefix, tmpl.streamPrefix(rc))
		})
	}
}

func TestPathTemplateObjectPartitionFails(t *testing.T) {
	tmpl, err := parsePathTemplate("{stream}/{field:meta}/{part}")
	require.NoError(t, err)
	_, err = tmpl.render(renderContext{stream: "orders"}, map[string]json.RawMessage{"meta": json.RawMessage(`{"a":1}`)}, "P")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `partition field "meta"`)
}

func TestPathTemplateRejected(t *testing.T) {
	cases := map[string]string{
		"{yyyy}/{stream}/{part}":            "only {namespace} and {stream} may come before",
		"{stream}_{field:x}/{part}":         "only {namespace} and {stream} may come before",
		"{namespace}/{part}":                "{stream} is required",
		"{stream}/{yyyy}":                   "{part} is required",
		"{stream}/{part}/x":                 "{part} must be in the last segment",
		"{stream}/{part}_{part}":            "{part} may appear only once",
		"{stream}_{part}":                   "must be in a directory segment",
		"{stream}/{stream}/{part}":          "only once",
		"{stream}/{bogus}/{part}":           "unknown token {bogus}",
		"{stream}/{field:}/{part}":          "invalid field token",
		"{stream}/{part":                    "unclosed {",
		"{stream}/p}{part}":                 "unmatched }",
		"":                                  "template is empty",
		"{stream}/{yyyy}-{mm}-{dd}/{field:": "unclosed {",
	}
	for tmpl, want := range cases {
		t.Run(tmpl, func(t *testing.T) {
			_, err := parsePathTemplate(tmpl)
			require.Error(t, err)
			assert.Contains(t, err.Error(), want)
		})
	}
}
