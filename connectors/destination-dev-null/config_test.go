package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseMode(t *testing.T, raw string) (mode, error) {
	t.Helper()
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(raw), &cfg))
	return cfg.mode()
}

func TestConfigModes(t *testing.T) {
	seed := uint64(1900)
	for _, c := range []struct {
		name, raw string
		want      mode
	}{
		{"empty is silent", `{}`, mode{kind: typeSilent}},
		{"silent", `{"test_destination":{"test_destination_type":"SILENT"}}`, mode{kind: typeSilent}},
		{"first n", `{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"FirstN","max_entry_count":5}}}`,
			mode{kind: typeLogging, maxEntries: 5, every: 1, ratio: 1}},
		{"logging without logging_config is first n", `{"test_destination":{"test_destination_type":"LOGGING"}}`,
			mode{kind: typeLogging, maxEntries: 100, every: 1, ratio: 1}},
		{"first n default", `{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"FirstN"}}}`,
			mode{kind: typeLogging, maxEntries: 100, every: 1, ratio: 1}},
		{"every nth", `{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"EveryNth","nth_entry_to_log":3,"max_entry_count":10.0}}}`,
			mode{kind: typeLogging, maxEntries: 10, every: 3, ratio: 1}},
		{"random sampling", `{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"RandomSampling","sampling_ratio":0.5,"seed":1900,"max_entry_count":7}}}`,
			mode{kind: typeLogging, maxEntries: 7, every: 1, ratio: 0.5, seed: &seed}},
		{"random sampling defaults", `{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"RandomSampling"}}}`,
			mode{kind: typeLogging, maxEntries: 100, every: 1, ratio: 0.001}},
		{"throttled", `{"test_destination":{"test_destination_type":"THROTTLED","millis_per_record":25}}`,
			mode{kind: typeThrottled, delay: 25 * time.Millisecond}},
		{"failing", `{"test_destination":{"test_destination_type":"FAILING","num_messages":3}}`,
			mode{kind: typeFailing, failAfter: 3}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseMode(t, c.raw)
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestConfigInvalid(t *testing.T) {
	for _, c := range []struct{ raw, want string }{
		{`{"test_destination":{"test_destination_type":"BLACKHOLE"}}`, `test_destination_type "BLACKHOLE" is not one of`},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"LastN"}}}`, `logging_type "LastN" is not one of`},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"FirstN","max_entry_count":0}}}`, "max_entry_count 0 is not"},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"FirstN","max_entry_count":1001}}}`, "max_entry_count 1001 is not"},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"FirstN","max_entry_count":2.5}}}`, "max_entry_count 2.5 is not"},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"EveryNth"}}}`, "EveryNth needs nth_entry_to_log"},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"EveryNth","nth_entry_to_log":0}}}`, "nth_entry_to_log 0 is not"},
		{`{"test_destination":{"test_destination_type":"LOGGING","logging_config":{"logging_type":"RandomSampling","sampling_ratio":1.5}}}`, "sampling_ratio 1.5 is not"},
		{`{"test_destination":{"test_destination_type":"THROTTLED"}}`, "THROTTLED needs millis_per_record"},
		{`{"test_destination":{"test_destination_type":"THROTTLED","millis_per_record":-1}}`, "millis_per_record -1 is negative"},
		{`{"test_destination":{"test_destination_type":"FAILING"}}`, "FAILING needs num_messages"},
		{`{"test_destination":{"test_destination_type":"FAILING","num_messages":-2}}`, "num_messages -2 is negative"},
	} {
		_, err := parseMode(t, c.raw)
		require.Error(t, err, c.raw)
		assert.Contains(t, err.Error(), c.want)
	}
}
