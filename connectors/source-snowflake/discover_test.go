package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestDiscover(t *testing.T) {
	src := newFake(t).on(sessionOK, discoverOK, pkOK).source()
	cat, err := src.Discover(t.Context(), passwordConfig())
	require.NoError(t, err)
	require.Len(t, cat.Streams, 2)

	events, types := cat.Streams[0], cat.Streams[1]
	require.Equal(t, eventsDesc, events.Descriptor())
	require.JSONEq(t, eventsSchema, string(events.JSONSchema))
	require.Equal(t, [][]string{{"ID"}}, events.SourceDefinedPrimaryKey)
	require.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, events.SupportedSyncModes)

	require.Equal(t, typesDesc, types.Descriptor())
	require.JSONEq(t, typesSchema, string(types.JSONSchema))
	require.Empty(t, types.SourceDefinedPrimaryKey)
}

func TestDiscoverEverySchema(t *testing.T) {
	other := [][]*string{infoColumn("OTHER", "A", "X", "TEXT", "", "")}
	all := fakeQuery{sql: discoverAllSQL, columns: discoverColumns, rows: append(other, eventsInfo...)}
	src := newFake(t).on(sessionOK, all, pkOK).source()
	cfg := passwordConfig()
	cfg.Schemas = nil
	cat, err := src.Discover(t.Context(), cfg)
	require.NoError(t, err)
	var names []string
	for _, s := range cat.Streams {
		names = append(names, s.Namespace+"."+s.Name)
	}
	require.Equal(t, []string{"OTHER.A", "PUBLIC.EVENTS"}, names)
}

func TestDiscoverUnsupportedType(t *testing.T) {
	q := discoverOK
	q.rows = append(append([][]*string{}, eventsInfo...), infoColumn("PUBLIC", "FILES", "F", "FILE", "", ""))
	src := newFake(t).on(sessionOK, q, pkOK).source()
	_, err := src.Discover(t.Context(), passwordConfig())
	require.EqualError(t, err, `table "SYNCLET_DB"."PUBLIC"."FILES": column "F" has type FILE, which the connector cannot read`)
}

func TestDiscoverPrimaryKeyOutsideColumns(t *testing.T) {
	pk := pkOK
	pk.rows = [][]*string{
		row(null, str("SYNCLET_DB"), str("PUBLIC"), str("EVENTS"), str("NAME"), str("2"), str("PK"), null, null),
		row(null, str("SYNCLET_DB"), str("PUBLIC"), str("EVENTS"), str("ID"), str("1"), str("PK"), null, null),
		row(null, str("SYNCLET_DB"), str("PUBLIC"), str("TYPES"), str("GONE"), str("1"), str("PK2"), null, null),
	}
	src := newFake(t).on(sessionOK, discoverOK, pk).source()
	cat, err := src.Discover(t.Context(), passwordConfig())
	require.NoError(t, err)
	require.Equal(t, [][]string{{"ID"}, {"NAME"}}, cat.Streams[0].SourceDefinedPrimaryKey)
	require.Empty(t, cat.Streams[1].SourceDefinedPrimaryKey)
}
