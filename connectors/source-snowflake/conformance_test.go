package main

import (
	"testing"

	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// conformanceFake answers every query of check, discover and two reads of
// the full refresh and incremental catalogs.
func conformanceFake(t *testing.T) Source {
	t3 := "2024-01-03T02:00:00.000000000+02:00"
	return newFake(t).on(
		sessionOK, schemataOK, tablesOK, probeOK, discoverOK, pkOK,
		typesColumnsOK, typesRead, eventsColumnsOK,
		fakeQuery{sql: readEventsFullSQL, columns: eventsReadColumns, rows: eventRows},
		fakeQuery{sql: readEventsSQL, columns: eventsReadColumns, rows: eventRows},
		fakeQuery{sql: readEventsFromSQL, bindings: []string{t3}, columns: eventsReadColumns, rows: eventRows[2:3]},
	).source()
}

func TestConformance(t *testing.T) {
	broken := passwordConfig()
	broken.Credentials.Password = "wrong"

	t.Run("full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, conformanceFake(t), testkit.SourceSuite{
			Config:       mustJSON(t, passwordConfig()),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      catalog(fullRefresh(eventsStream()), fullRefresh(typesStream())),
		})
	})
	t.Run("incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, conformanceFake(t), testkit.SourceSuite{
			Config:       mustJSON(t, keyPairConfig(t)),
			BrokenConfig: mustJSON(t, broken),
			Catalog:      catalog(incremental(eventsStream(), "UPDATED_AT")),
		})
	})
}
