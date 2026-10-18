package main

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataSchema(t *testing.T) {
	schema := metadataSchema()
	require.Len(t, schema, 4)

	assert.Equal(t, "_airbyte_raw_id", schema[0].Name)
	assert.Equal(t, bigquery.StringFieldType, schema[0].Type)
	assert.True(t, schema[0].Required)

	assert.Equal(t, "_airbyte_extracted_at", schema[1].Name)
	assert.Equal(t, bigquery.TimestampFieldType, schema[1].Type)
	assert.True(t, schema[1].Required)

	assert.Equal(t, "_airbyte_meta", schema[2].Name)
	assert.Equal(t, bigquery.JSONFieldType, schema[2].Type)
	assert.True(t, schema[2].Required)

	assert.Equal(t, "_airbyte_generation_id", schema[3].Name)
	assert.Equal(t, bigquery.IntegerFieldType, schema[3].Type)
	assert.False(t, schema[3].Required)
}

func TestRawTableSchema(t *testing.T) {
	schema := rawTableSchema()
	require.Len(t, schema, 6)

	names := make([]string, len(schema))
	for i, f := range schema {
		names[i] = f.Name
	}
	assert.Contains(t, names, "_airbyte_raw_id")
	assert.Contains(t, names, "_airbyte_extracted_at")
	assert.Contains(t, names, "_airbyte_loaded_at")
	assert.Contains(t, names, "_airbyte_data")
	assert.Contains(t, names, "_airbyte_meta")
	assert.Contains(t, names, "_airbyte_generation_id")
}

func TestBuildSchema(t *testing.T) {
	jsonSchema := map[string]interface{}{
		"properties": map[string]interface{}{
			"name": map[string]interface{}{"type": "string"},
			"age":  map[string]interface{}{"type": "integer"},
		},
	}

	schema, cols := buildSchema(jsonSchema)

	// 4 metadata + 2 user columns = 6
	require.Len(t, schema, 6)

	// Metadata columns first
	assert.Equal(t, "_airbyte_raw_id", schema[0].Name)
	assert.Equal(t, "_airbyte_extracted_at", schema[1].Name)
	assert.Equal(t, "_airbyte_meta", schema[2].Name)
	assert.Equal(t, "_airbyte_generation_id", schema[3].Name)

	// User columns sorted alphabetically
	assert.Equal(t, "age", schema[4].Name)
	assert.Equal(t, bigquery.IntegerFieldType, schema[4].Type)
	assert.Equal(t, "name", schema[5].Name)
	assert.Equal(t, bigquery.StringFieldType, schema[5].Type)

	// columnDef slice should match user columns
	require.Len(t, cols, 2)
	assert.Equal(t, "age", cols[0].name)
	assert.Equal(t, bigquery.IntegerFieldType, cols[0].bqType)
	assert.Equal(t, "name", cols[1].name)
	assert.Equal(t, bigquery.StringFieldType, cols[1].bqType)
}

func TestBuildSchemaWithArrayType(t *testing.T) {
	// Airbyte sends type as array: ["string", "null"] meaning nullable string
	jsonSchema := map[string]interface{}{
		"properties": map[string]interface{}{
			"email": map[string]interface{}{
				"type": []interface{}{"string", "null"},
			},
		},
	}

	schema, _ := buildSchema(jsonSchema)
	// 4 metadata + 1 user = 5
	require.Len(t, schema, 5)
	assert.Equal(t, "email", schema[4].Name)
	assert.Equal(t, bigquery.StringFieldType, schema[4].Type)
}

func TestBuildSchemaWithFormat(t *testing.T) {
	jsonSchema := map[string]interface{}{
		"properties": map[string]interface{}{
			"created_at": map[string]interface{}{
				"type":   "string",
				"format": "date-time",
			},
		},
	}

	schema, cols := buildSchema(jsonSchema)
	require.Len(t, schema, 5)
	assert.Equal(t, "created_at", schema[4].Name)
	assert.Equal(t, bigquery.TimestampFieldType, schema[4].Type)
	require.Len(t, cols, 1)
	assert.Equal(t, bigquery.TimestampFieldType, cols[0].bqType)
}

func TestDiffSchema(t *testing.T) {
	existing := bigquery.Schema{
		{Name: "_airbyte_raw_id", Type: bigquery.StringFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "old_col", Type: bigquery.IntegerFieldType},
	}

	desired := bigquery.Schema{
		{Name: "_airbyte_raw_id", Type: bigquery.StringFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "email", Type: bigquery.StringFieldType},
	}

	diff := diffSchema(existing, desired)

	// email added
	require.Len(t, diff.added, 1)
	assert.Equal(t, "email", diff.added[0].name)

	// old_col removed
	require.Len(t, diff.removed, 1)
	assert.Equal(t, "old_col", diff.removed[0])
}

func TestDiffSchemaTypeChange(t *testing.T) {
	existing := bigquery.Schema{
		{Name: "count", Type: bigquery.IntegerFieldType},
	}

	desired := bigquery.Schema{
		{Name: "count", Type: bigquery.NumericFieldType},
	}

	diff := diffSchema(existing, desired)

	// INT64 -> NUMERIC is a widening conversion
	require.Len(t, diff.typeChanged, 1)
	assert.Equal(t, "count", diff.typeChanged[0].name)
	assert.Equal(t, "INT64", diff.typeChanged[0].oldType)
	assert.Equal(t, "NUMERIC", diff.typeChanged[0].newType)
}

func TestDiffSchemaNoWideningIgnored(t *testing.T) {
	existing := bigquery.Schema{
		{Name: "data", Type: bigquery.StringFieldType},
	}

	desired := bigquery.Schema{
		{Name: "data", Type: bigquery.IntegerFieldType},
	}

	diff := diffSchema(existing, desired)

	// STRING -> INT64 is NOT widening, should be ignored
	assert.Empty(t, diff.typeChanged)
}

func TestClusteringColumns(t *testing.T) {
	t.Run("pk columns plus extracted_at", func(t *testing.T) {
		cols := clusteringColumns([]string{"id"}, map[string]bigquery.FieldType{
			"id": bigquery.IntegerFieldType,
		})
		assert.Equal(t, []string{"id", "_airbyte_extracted_at"}, cols)
	})

	t.Run("max 4 total", func(t *testing.T) {
		cols := clusteringColumns([]string{"a", "b", "c", "d"}, map[string]bigquery.FieldType{
			"a": bigquery.StringFieldType,
			"b": bigquery.StringFieldType,
			"c": bigquery.StringFieldType,
			"d": bigquery.StringFieldType,
		})
		// 3 PK + 1 extracted_at = 4
		assert.Len(t, cols, 4)
		assert.Equal(t, "_airbyte_extracted_at", cols[3])
	})

	t.Run("skip JSON typed PK", func(t *testing.T) {
		cols := clusteringColumns([]string{"id", "data"}, map[string]bigquery.FieldType{
			"id":   bigquery.IntegerFieldType,
			"data": bigquery.JSONFieldType,
		})
		assert.Equal(t, []string{"id", "_airbyte_extracted_at"}, cols)
	})

	t.Run("no PK columns", func(t *testing.T) {
		cols := clusteringColumns(nil, nil)
		assert.Equal(t, []string{"_airbyte_extracted_at"}, cols)
	})
}
