package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const usersSchema = `{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer"},"meta":{"type":"object"},"at":{"type":"string","format":"date-time"}}}`

func TestPlanStreams(t *testing.T) {
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{
		stream("users", usersSchema, synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}, {"n"}}, []string{"at"}),
		stream("rates", usersSchema, synclet.DestinationSyncModeOverwrite, nil, nil),
	}})
	require.NoError(t, err)
	require.Len(t, plans, 2)

	u := plans[0]
	assert.Equal(t, "users", u.target())
	assert.Equal(t, []string{"at", "id", "meta", "n"}, []string{u.columns[0].name, u.columns[1].name, u.columns[2].name, u.columns[3].name})
	assert.Equal(t, []int{1, 3}, u.pk)
	assert.Equal(t, 0, u.cursor)
	assert.Equal(t, keyTextType, u.columns[1].sqlType)
	assert.True(t, u.columns[1].key)
	assert.True(t, u.columns[3].key)
	assert.False(t, u.columns[0].key)
	assert.Equal(t, []string{
		"[_airbyte_raw_id] UNIQUEIDENTIFIER", "[_airbyte_extracted_at] DATETIMEOFFSET(7)", "[at] DATETIMEOFFSET(7)",
		"[id] NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL", "[meta] NVARCHAR(MAX) COLLATE Latin1_General_100_BIN2",
		"[n] BIGINT NOT NULL", "PRIMARY KEY NONCLUSTERED ([id], [n])",
	}, u.createDefs())

	r := plans[1]
	assert.Equal(t, "_synclet_tmp_rates", r.target())
	assert.Empty(t, r.pk)
	assert.Equal(t, -1, r.cursor)
	assert.Equal(t, "NVARCHAR(MAX)", r.columns[1].sqlType, "only dedup keys are bounded")
}

func TestPlanStreamsFails(t *testing.T) {
	cases := []struct {
		s    synclet.ConfiguredStream
		want string
	}{
		{stream("u", usersSchema, "upsert", nil, nil), `destination_sync_mode "upsert" is not one of`},
		{stream("u", usersSchema, synclet.DestinationSyncModeAppendDedup, nil, nil), "append_dedup needs a primary key"},
		{stream("u", usersSchema, synclet.DestinationSyncModeAppendDedup, [][]string{{"meta", "a"}}, nil), "is nested"},
		{stream("u", usersSchema, synclet.DestinationSyncModeAppendDedup, [][]string{{"nope"}}, nil), `"nope" is not a property`},
		{stream("u", usersSchema, synclet.DestinationSyncModeAppendDedup, [][]string{{"meta"}}, nil), `primary key field "meta" is an object`},
		{stream("u", usersSchema, synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"meta"}), `cursor field "meta" is an object`},
		{stream("u", `{"type":"object","properties":{"_airbyte_raw_id":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil), `both map to "_airbyte_raw_id"`},
		{stream("u", `{"properties":`, synclet.DestinationSyncModeAppend, nil, nil), "json_schema"},
	}
	for _, c := range cases {
		_, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{c.s}})
		require.Error(t, err, c.want)
		assert.Contains(t, err.Error(), c.want)
	}

	a := stream("events", usersSchema, synclet.DestinationSyncModeAppend, nil, nil)
	b := a
	b.Stream.Namespace = "other"
	_, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{a, b}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `table names "events" and "other.events" both map to "events"`)
}

func TestMoveSQL(t *testing.T) {
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{
		stream("users", usersSchema, synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"at"}),
		stream("plain", usersSchema, synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil),
		stream("events", usersSchema, synclet.DestinationSyncModeAppend, nil, nil),
	}})
	require.NoError(t, err)

	assert.Equal(t, "MERGE INTO [raw].[users] WITH (HOLDLOCK) AS t USING (SELECT c0, c1, c2, c3, c4, c5 FROM "+
		"(SELECT *, ROW_NUMBER() OVER (PARTITION BY c3 ORDER BY c2 DESC, [_synclet_seq] DESC) AS _synclet_rn FROM #synclet_stage) AS r "+
		"WHERE _synclet_rn = 1) AS s ON t.[id] = s.c3 "+
		"WHEN MATCHED AND (t.[at] IS NULL OR s.c2 >= t.[at]) THEN UPDATE SET [_airbyte_raw_id] = s.c0, [_airbyte_extracted_at] = s.c1, [at] = s.c2, [meta] = s.c4, [n] = s.c5 "+
		"WHEN NOT MATCHED THEN INSERT ([_airbyte_raw_id], [_airbyte_extracted_at], [at], [id], [meta], [n]) VALUES (s.c0, s.c1, s.c2, s.c3, s.c4, s.c5);",
		moveSQL("raw", plans[0]))
	assert.Contains(t, moveSQL("raw", plans[1]), "ORDER BY [_synclet_seq] DESC) AS _synclet_rn")
	assert.Contains(t, moveSQL("raw", plans[1]), "WHEN MATCHED THEN UPDATE SET")
	assert.Equal(t, "INSERT INTO [raw].[events] ([_airbyte_raw_id], [_airbyte_extracted_at], [at], [id], [meta], [n]) "+
		"SELECT c0, c1, c2, c3, c4, c5 FROM #synclet_stage ORDER BY [_synclet_seq]", moveSQL("raw", plans[2]))
}

func TestStateHolding(t *testing.T) {
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{
		stream("rates", usersSchema, synclet.DestinationSyncModeOverwrite, nil, nil),
		stream("events", usersSchema, synclet.DestinationSyncModeAppend, nil, nil),
	}})
	require.NoError(t, err)
	w := newWriter(nil, "dbo", nil, NewDestination(), plans)
	streamState := func(name, ns string) synclet.StateMessage {
		return synclet.StateMessage{Type: synclet.StateTypeStream, Stream: &synclet.StreamState{
			StreamDescriptor: synclet.StreamDescriptor{Name: name, Namespace: ns},
		}}
	}
	assert.True(t, w.isHeld(streamState("rates", "")))
	assert.True(t, w.isHeld(streamState("rates", "public")), "matched by name when no stream has the namespace")
	assert.False(t, w.isHeld(streamState("events", "")))
	assert.False(t, w.isHeld(streamState("events", "public")))
	assert.True(t, w.isHeld(streamState("unknown", "")), "a state of no catalog stream waits while an overwrite stream does")
	assert.True(t, w.isHeld(synclet.StateMessage{Type: synclet.StateTypeGlobal}))
	assert.True(t, w.isHeld(synclet.StateMessage{Type: synclet.StateTypeLegacy}))

	appendOnly := newWriter(nil, "dbo", nil, NewDestination(), plans[1:])
	assert.False(t, appendOnly.isHeld(synclet.StateMessage{Type: synclet.StateTypeGlobal}))
	assert.False(t, appendOnly.isHeld(streamState("unknown", "")))
}

func TestCheckKeyText(t *testing.T) {
	key := column{name: "k", sqlType: keyTextType, key: true}
	require.NoError(t, checkKeyText(key, "a b"))
	require.NoError(t, checkKeyText(column{sqlType: "BIGINT", key: true}, int64(1)))
	require.ErrorContains(t, checkKeyText(key, "a "), "ends with a space")
	long := ""
	for range 128 {
		long += "\U0001F600"
	}
	require.ErrorContains(t, checkKeyText(key, long), "longer than 255 characters")
}
