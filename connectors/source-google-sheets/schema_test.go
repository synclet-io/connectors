package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseHeaders(t *testing.T) {
	t.Run("stops at empty header", func(t *testing.T) {
		row := []interface{}{"Name", "Email", "", "Phone"}
		headers, err := parseHeaders(row)
		require.NoError(t, err)
		assert.Equal(t, []string{"Name", "Email"}, headers)
	})

	t.Run("deduplicates with column letter", func(t *testing.T) {
		row := []interface{}{"Name", "Name", "Age"}
		headers, err := parseHeaders(row)
		require.NoError(t, err)
		assert.Equal(t, []string{"Name_A", "Name_B", "Age"}, headers)
	})

	t.Run("error on zero headers", func(t *testing.T) {
		row := []interface{}{"", "something"}
		_, err := parseHeaders(row)
		assert.Error(t, err)
	})

	t.Run("empty row returns error", func(t *testing.T) {
		row := []interface{}{}
		_, err := parseHeaders(row)
		assert.Error(t, err)
	})

	t.Run("single header", func(t *testing.T) {
		row := []interface{}{"ID"}
		headers, err := parseHeaders(row)
		require.NoError(t, err)
		assert.Equal(t, []string{"ID"}, headers)
	})
}

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"First Name", "first_name"},
		{"firstName", "first_name"},
		{"Already_Snake", "already_snake"},
		{"camelCase", "camel_case"},
		{"PascalCase", "pascal_case"},
		{"some-kebab-case", "some_kebab_case"},
		{"ALLCAPS", "allcaps"},
		{"XMLParser", "xml_parser"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := toSnakeCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestColumnLetter(t *testing.T) {
	tests := []struct {
		index    int
		expected string
	}{
		{0, "A"},
		{1, "B"},
		{25, "Z"},
		{26, "AA"},
		{27, "AB"},
		{51, "AZ"},
		{52, "BA"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := columnLetter(tt.index)
			assert.Equal(t, tt.expected, result)
		})
	}
}
