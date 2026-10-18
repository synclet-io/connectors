package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGAQLParse(t *testing.T) {
	q, err := parseQuery("select campaign.id, metrics.clicks ,segments.date from campaign where campaign.status = 'ENABLED' order by campaign.id limit 10")
	require.NoError(t, err)
	assert.Equal(t, []string{"campaign.id", "metrics.clicks", "segments.date"}, q.fields)
	assert.Equal(t, "campaign", q.resource)
	assert.True(t, q.selects("segments.date"))
	assert.False(t, q.selects("campaign.name"))

	for _, bad := range []string{
		"",
		"campaign.id FROM campaign",
		"SELECT FROM campaign",
		"SELECT campaign.id",
		"SELECT campaign.id, FROM campaign",
		"SELECT campaign.id FROM",
		"SELECT Campaign-Id FROM campaign",
	} {
		_, err := parseQuery(bad)
		assert.Error(t, err, bad)
	}
}

func TestGAQLDateRange(t *testing.T) {
	tests := []struct {
		name, query, want string
	}{
		{
			name:  "no WHERE",
			query: "SELECT campaign.id, segments.date FROM campaign",
			want:  "SELECT campaign.id, segments.date FROM campaign WHERE segments.date BETWEEN '2026-10-01' AND '2026-10-18'",
		},
		{
			name:  "WHERE present",
			query: "SELECT campaign.id, segments.date FROM campaign WHERE campaign.status = 'ENABLED'",
			want:  "SELECT campaign.id, segments.date FROM campaign WHERE campaign.status = 'ENABLED' AND segments.date BETWEEN '2026-10-01' AND '2026-10-18'",
		},
		{
			name:  "ORDER BY, LIMIT and PARAMETERS kept after the clause",
			query: "SELECT campaign.id, segments.date FROM campaign WHERE campaign.status = 'ENABLED' ORDER BY campaign.id LIMIT 5 PARAMETERS include_drafts=true",
			want:  "SELECT campaign.id, segments.date FROM campaign WHERE campaign.status = 'ENABLED' AND segments.date BETWEEN '2026-10-01' AND '2026-10-18' ORDER BY campaign.id LIMIT 5 PARAMETERS include_drafts=true",
		},
		{
			name:  "no WHERE before ORDER BY",
			query: "SELECT campaign.id, segments.date FROM campaign ORDER BY campaign.id",
			want:  "SELECT campaign.id, segments.date FROM campaign WHERE segments.date BETWEEN '2026-10-01' AND '2026-10-18' ORDER BY campaign.id",
		},
		{
			name:  "lowercase keywords",
			query: "select campaign.id, segments.date from campaign where campaign.status = 'ENABLED' limit 5",
			want:  "select campaign.id, segments.date from campaign where campaign.status = 'ENABLED' AND segments.date BETWEEN '2026-10-01' AND '2026-10-18' limit 5",
		},
		{
			name:  "keywords inside a string literal",
			query: "SELECT campaign.id, segments.date FROM campaign WHERE campaign.name = 'Summer LIMIT ORDER BY sale'",
			want:  "SELECT campaign.id, segments.date FROM campaign WHERE campaign.name = 'Summer LIMIT ORDER BY sale' AND segments.date BETWEEN '2026-10-01' AND '2026-10-18'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parseQuery(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, q.withDateRange("2026-10-01", "2026-10-18"))
		})
	}
}

func TestGAQLCustomQueries(t *testing.T) {
	defs, err := customDefs([]CustomQuery{{Name: "my_keywords", Query: "SELECT ad_group_criterion.keyword.text, segments.date FROM keyword_view"}})
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "my_keywords", defs[0].name)
	assert.True(t, defs[0].incremental())

	for _, tt := range []struct {
		name string
		cq   CustomQuery
		msg  string
	}{
		{"collides with a built-in", CustomQuery{Name: streamCampaign, Query: "SELECT campaign.id FROM campaign"}, `custom query name "campaign" is the name of a built-in stream`},
		{"uppercase name", CustomQuery{Name: "MyKeywords", Query: "SELECT campaign.id FROM campaign"}, `custom query name "MyKeywords" must match ^[a-z][a-z0-9_]*$`},
		{"name with a dash", CustomQuery{Name: "my-keywords", Query: "SELECT campaign.id FROM campaign"}, `custom query name "my-keywords" must match ^[a-z][a-z0-9_]*$`},
		{"bad query", CustomQuery{Name: "broken", Query: "SELECT FROM campaign"}, `custom query broken:`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := customDefs([]CustomQuery{tt.cq})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.msg)
		})
	}

	_, err = customDefs([]CustomQuery{
		{Name: "twice", Query: "SELECT campaign.id FROM campaign"},
		{Name: "twice", Query: "SELECT ad_group.id FROM ad_group"},
	})
	assert.ErrorContains(t, err, `custom query name "twice" is used twice`)
}
