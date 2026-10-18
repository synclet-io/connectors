package main

import (
	"encoding/json"
	"testing"
)

func TestEntryWritesBreakingChangesUnderReleases(t *testing.T) {
	c := Connector{
		Dir: "source-acme",
		Meta: Meta{
			Name:          "Acme",
			ConnectorType: "source",
			Version:       "2.0.0",
			ReleaseStage:  "alpha",
			SupportLevel:  "community",
			License:       "MIT",
			BreakingChanges: map[string]BreakingChange{
				"2.0.0": {Message: "renamed stream", MigrationDocumentationURL: "https://example.com/m", UpgradeDeadline: "2027-01-31"},
			},
		},
		Spec: json.RawMessage(`{"connectionSpecification":{}}`),
	}

	data, err := json.Marshal(toEntry(c, "ghcr.io/Synclet-IO/connectors/"))
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}

	if _, ok := got["breakingChanges"]; ok {
		t.Errorf("top-level breakingChanges present: %s", data)
	}

	if got["dockerRepository"] != "ghcr.io/synclet-io/connectors/source-acme" {
		t.Errorf("dockerRepository = %v", got["dockerRepository"])
	}

	bc, _ := got["releases"].(map[string]any)["breakingChanges"].(map[string]any)["2.0.0"].(map[string]any)
	want := map[string]any{"message": "renamed stream", "migrationDocumentationUrl": "https://example.com/m", "upgradeDeadline": "2027-01-31"}

	for k, v := range want {
		if bc[k] != v {
			t.Errorf("releases.breakingChanges[2.0.0].%s = %v, want %v (entry %s)", k, bc[k], v, data)
		}
	}
}
