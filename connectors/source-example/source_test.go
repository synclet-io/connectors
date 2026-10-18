package main

import (
	"encoding/json"
	"testing"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func configJSON(t *testing.T, cfg Config) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func numbersCatalog(t *testing.T) *synclet.ConfiguredCatalog {
	t.Helper()
	cat, err := Source{}.Discover(t.Context(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream:              cat.Streams[0],
		SyncMode:            synclet.SyncModeFullRefresh,
		DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
	}}}
}

func TestConformance(t *testing.T) {
	testkit.RunSourceSuite(t, Source{}, testkit.SourceSuite{
		Config:       configJSON(t, Config{Count: 5}),
		BrokenConfig: configJSON(t, Config{Count: -1}),
		Catalog:      numbersCatalog(t),
	})
}

func TestCheckRejectsOutOfRangeCount(t *testing.T) {
	for _, count := range []int{-1, maxCount + 1} {
		if err := (Source{}).Check(t.Context(), Config{Count: count}); err == nil {
			t.Errorf("count %d accepted", count)
		}
	}
	if err := (Source{}).Check(t.Context(), Config{}); err != nil {
		t.Errorf("default count rejected: %v", err)
	}
}

func TestReadEmitsConfiguredRows(t *testing.T) {
	catalog, err := json.Marshal(numbersCatalog(t))
	if err != nil {
		t.Fatal(err)
	}
	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{
		Command: synclet.CommandRead,
		Config:  configJSON(t, Config{Count: 3}),
		Catalog: catalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Err != nil {
		t.Fatal(out.Err)
	}
	recs := out.Records(synclet.StreamDescriptor{Name: streamName})
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	var last struct {
		ID    int    `json:"id"`
		Label string `json:"label"`
	}
	if err := json.Unmarshal(recs[2].Data, &last); err != nil {
		t.Fatal(err)
	}
	if last.ID != 3 || last.Label != "row 3" {
		t.Fatalf("last record = %+v", last)
	}
}
