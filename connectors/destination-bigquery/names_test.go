package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableOrColumnName(t *testing.T) {
	cases := map[string]string{
		"users":                                  "users",
		"Users":                                  "Users",
		"Sales Pipeline/Deals":                   "Sales_Pipeline_Deals",
		"sales_pipeline/deals/tblAbCdEfGhIjKlMn": "sales_pipeline_deals_tblAbCdEfGhIjKlMn",
		"price_(usd)":                            "price__usd_",
		"№_of_items":                             "No_of_items",
		"café":                                   "cafe",
		"ціна_товару":                            "___________",
		"30d,_total":                             "_30d__total",
		"a \t\n b":                               "a_b",
		"a__b":                                   "a__b",
		"emoji 🚀":                                "emoji__",
		"":                                       "_",
		"_airtable_id":                           "_airtable_id",
	}
	for in, want := range cases {
		assert.Equal(t, want, tableOrColumnName(in), "name of %q", in)
	}
}

func TestDatasetName(t *testing.T) {
	cases := map[string]string{
		"sales_data": "sales_data",
		"my-dataset": "my_dataset",
		"2026_data":  "2026_data",
		"_hidden":    "n_hidden",
		"":           "n",
	}
	for in, want := range cases {
		assert.Equal(t, want, datasetName(in), "dataset of %q", in)
	}
}

func TestColumnNames(t *testing.T) {
	t.Run("names that need no change keep them", func(t *testing.T) {
		got := columnNames([]string{"id", "name", "_ab_cdc_deleted_at"})
		assert.Equal(t, map[string]string{"id": "id", "name": "name", "_ab_cdc_deleted_at": "_ab_cdc_deleted_at"}, got)
	})
	t.Run("a collision takes the first free numbered name", func(t *testing.T) {
		got := columnNames([]string{"a_b", "a b", "a-b"})
		// Sorted: "a b", "a-b", "a_b".
		assert.Equal(t, map[string]string{"a b": "a_b", "a-b": "a_b_1", "a_b": "a_b_2"}, got)
	})
	t.Run("columns that differ only in case collide", func(t *testing.T) {
		got := columnNames([]string{"name", "Name"})
		assert.Equal(t, map[string]string{"Name": "Name", "name": "name_1"}, got)
	})
	t.Run("a numbered name skips one already taken", func(t *testing.T) {
		got := columnNames([]string{"a_b_1", "a b", "a_b"})
		// Sorted: "a b", "a_b", "a_b_1"; "a_b_1" finds its own name taken.
		assert.Equal(t, map[string]string{"a b": "a_b", "a_b": "a_b_1", "a_b_1": "a_b_1_1"}, got)
	})
}

func TestWriterRenamesToBigQueryNames(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	schema := map[string]interface{}{
		"properties": map[string]interface{}{
			"Record ID":     map[string]interface{}{"type": "string"},
			"Price (USD)":   map[string]interface{}{"type": "string"},
			"Last Modified": map[string]interface{}{"type": "string", "format": "date-time"},
			"plain":         map[string]interface{}{"type": "integer"},
		},
	}
	catalog := testCatalogWithMode("Sales Pipeline/Deals", "sales-data", "append_dedup",
		[][]string{{"Record ID"}}, []string{"Last Modified"}, schema)
	w := NewBigQueryWriter(ops, ldr, testConfig(), catalog)

	err := w.AddRecord(t.Context(), "sales-data.Sales Pipeline/Deals", map[string]interface{}{
		"Record ID":     "rec1",
		"Price (USD)":   "9.99",
		"Last Modified": "2026-11-05T10:00:00Z",
		"plain":         float64(7),
	})
	require.NoError(t, err)
	tracker, _ := newMockTracker()
	require.NoError(t, w.FlushAll(t.Context(), tracker))

	require.Len(t, ldr.loadCalls, 1)
	call := ldr.loadCalls[0]
	assert.Equal(t, "sales_data", call.datasetID)
	assert.Equal(t, "_airbyte_staging_Sales_Pipeline_Deals", call.tableName)
	var cols []string
	for _, f := range call.schema {
		cols = append(cols, f.Name)
	}
	assert.Subset(t, cols, []string{"Record_ID", "Price__USD_", "Last_Modified", "plain"})
	rec := call.records[0]
	assert.Equal(t, "rec1", rec["Record_ID"])
	assert.Equal(t, "9.99", rec["Price__USD_"])
	assert.Equal(t, "2026-11-05T10:00:00Z", rec["Last_Modified"])
	assert.Equal(t, int64(7), rec["plain"])
	assert.NotContains(t, rec, "Record ID")
	assert.NotContains(t, rec, "Price (USD)")

	var create, merge string
	for _, sql := range ops.executeQueryCalls {
		switch {
		case strings.HasPrefix(sql, "CREATE TABLE"):
			create = sql
		case strings.HasPrefix(sql, "MERGE"):
			merge = sql
		}
	}
	assert.Contains(t, create, "`test-project`.`sales_data`.`Sales_Pipeline_Deals`")
	assert.Contains(t, create, "CLUSTER BY `Record_ID`, `_airbyte_extracted_at`")
	assert.Contains(t, merge, "target.`Record_ID` = source.`Record_ID`")
	assert.Contains(t, merge, "`Last_Modified`")
}
