package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestSpec(t *testing.T) {
	source := NewGoogleSheetsSource()

	spec, err := source.Spec(context.Background())
	require.NoError(t, err)
	require.NotNil(t, spec)

	// Verify key properties
	assert.False(t, spec.SupportsIncremental, "Sheets is full_refresh only")
	assert.Equal(t, "object", spec.ConnectionSpecification.Type)
	assert.Contains(t, spec.ConnectionSpecification.Required, synclet.PropertyName("spreadsheet_id"))
	assert.Contains(t, spec.ConnectionSpecification.Required, synclet.PropertyName("credentials"))

	// Verify destination sync modes
	assert.Contains(t, spec.SupportedDestinationSyncModes, synclet.DestinationSyncModeOverwrite)
	assert.Contains(t, spec.SupportedDestinationSyncModes, synclet.DestinationSyncModeAppend)

	// Verify properties exist
	props := spec.ConnectionSpecification.Properties.Properties
	assert.Contains(t, props, synclet.PropertyName("spreadsheet_id"))
	assert.Contains(t, props, synclet.PropertyName("credentials"))
	assert.Contains(t, props, synclet.PropertyName("batch_size"))
	assert.Contains(t, props, synclet.PropertyName("names_conversion"))

	// Verify credentials uses oneOf with two auth variants
	credSpec := props["credentials"]
	require.Len(t, credSpec.OneOf, 3, "credentials should have 3 oneOf variants (OAuth, Service, ADC)")

	// OAuth variant
	oauth := credSpec.OneOf[0]
	assert.Equal(t, "Authenticate via Google (OAuth)", oauth.Title)
	assert.Contains(t, oauth.Properties, synclet.PropertyName("auth_type"))
	assert.Contains(t, oauth.Properties, synclet.PropertyName("client_id"))
	assert.Contains(t, oauth.Properties, synclet.PropertyName("client_secret"))
	assert.Contains(t, oauth.Properties, synclet.PropertyName("refresh_token"))
	assert.Equal(t, "Client", oauth.Properties["auth_type"].Const)

	// Service Account variant
	sa := credSpec.OneOf[1]
	assert.Equal(t, "Service Account Key Authentication", sa.Title)
	assert.Contains(t, sa.Properties, synclet.PropertyName("auth_type"))
	assert.Contains(t, sa.Properties, synclet.PropertyName("service_account_info"))
	assert.Equal(t, "Service", sa.Properties["auth_type"].Const)

	// ADC variant
	adc := credSpec.OneOf[2]
	assert.Equal(t, "Application Default Credentials", adc.Title)
	assert.Contains(t, adc.Properties, synclet.PropertyName("auth_type"))
	assert.Equal(t, "ApplicationDefault", adc.Properties["auth_type"].Const)
}

func TestBuildStreamSchema(t *testing.T) {
	headers := []string{"name", "email", "age"}
	schema := buildStreamSchema(headers)

	assert.Len(t, schema, 3)

	for _, h := range headers {
		prop, exists := schema[synclet.PropertyName(h)]
		require.True(t, exists, "property %q should exist", h)
		assert.Equal(t, synclet.PropTypes{synclet.Null, synclet.String}, prop.Type)
	}
}

func TestMapRowToRecord(t *testing.T) {
	headers := []string{"name", "email", "age"}

	t.Run("normal row", func(t *testing.T) {
		row := []interface{}{"Alice", "alice@example.com", "30"}
		record := mapRowToRecord(headers, row)
		require.NotNil(t, record)
		assert.Equal(t, "Alice", record["name"])
		assert.Equal(t, "alice@example.com", record["email"])
		assert.Equal(t, "30", record["age"])
	})

	t.Run("short row pads with nil", func(t *testing.T) {
		row := []interface{}{"Bob"}
		record := mapRowToRecord(headers, row)
		require.NotNil(t, record)
		assert.Equal(t, "Bob", record["name"])
		assert.Nil(t, record["email"])
		assert.Nil(t, record["age"])
	})

	t.Run("empty row returns nil", func(t *testing.T) {
		row := []interface{}{"", "", ""}
		record := mapRowToRecord(headers, row)
		assert.Nil(t, record)
	})

	t.Run("all nil row returns nil", func(t *testing.T) {
		row := []interface{}{nil, nil, nil}
		record := mapRowToRecord(headers, row)
		assert.Nil(t, record)
	})
}

func TestCheckFailureMessage(t *testing.T) {
	var out bytes.Buffer
	err := synclet.ServeSource(context.Background(), NewGoogleSheetsSource(), synclet.Request{
		Command: synclet.CommandCheck,
		Config:  json.RawMessage(`{"spreadsheet_id":"abc","credentials":{"auth_type":"Unknown"}}`),
	}, &out)
	require.NoError(t, err)

	var msg synclet.Message
	require.NoError(t, json.Unmarshal(out.Bytes(), &msg))
	require.Equal(t, synclet.MessageTypeConnectionStatus, msg.Type)
	assert.Equal(t, synclet.ConnectionStatusFailed, msg.ConnectionStatus.Status)
	assert.Contains(t, msg.ConnectionStatus.Message, "unsupported auth_type: Unknown")
}
