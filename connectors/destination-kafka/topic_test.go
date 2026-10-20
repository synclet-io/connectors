package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicName(t *testing.T) {
	for _, c := range []struct{ pattern, namespace, stream, want string }{
		{"{namespace}.{stream}", "public", "users", "public_users"},
		{"{stream}", "", "users", "users"},
		{"{namespace}-{stream}", "", "users", "_users"},
		{"sample.topic", "public", "users", "sample_topic"},
		{"cdc_{stream}_{stream}", "", "orders", "cdc_orders_orders"},
		{"{stream}", "", "Ünïcödé  name", "Unicode_name"},
		{"{stream}", "", "заказы", "______"},
		{"{stream}", "", "a\t\nb", "a_b"},
		{"{Stream}", "", "users", "_Stream_"},
	} {
		got, err := topicName(c.pattern, c.namespace, c.stream)
		require.NoError(t, err, c)
		assert.Equal(t, c.want, got, c)
	}
}

func TestTopicNameErrors(t *testing.T) {
	_, err := topicName("{namespace}", "", "users")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "topic name is empty")

	_, err = topicName("{stream}", "", strings.Repeat("s", 250))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than Kafka's 249")
}

func TestValidTopic(t *testing.T) {
	for _, ok := range []string{"a", "a.b-c_D9", strings.Repeat("x", 249)} {
		assert.NoError(t, validTopic(ok), ok)
	}
	for _, bad := range []string{"", ".", "..", "a/b", "a b", "é", strings.Repeat("x", 250)} {
		assert.Error(t, validTopic(bad), bad)
	}
}
