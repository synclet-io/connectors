package sourceintercom

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"

	"source-declarative/declarativetest"
)

var (
	config = json.RawMessage(`{"access_token":"test-token"}`)
	auth   = declarativetest.Header("Authorization", "Bearer test-token")
	now    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
)

func TestManifest(t *testing.T) {
	declarativetest.Run(t, declarativetest.Suite{
		Config:        config,
		BrokenConfig:  json.RawMessage(`{"access_token":"wrong-token"}`),
		Auth:          auth,
		Now:           now,
		MinPages:      2,
		RateLimitOnce: true,
	})
}

func TestCheckForbidden(t *testing.T) {
	h := declarativetest.New(t, declarativetest.Suite{
		Config:    config,
		Auth:      auth,
		Now:       now,
		Overrides: map[string]declarativetest.Fixture{"admins": {Status: http.StatusForbidden, File: "errors/403.json"}},
	})
	out, err := testkit.RunSource(context.Background(), h.Source, synclet.Request{Command: synclet.CommandCheck, Config: config})
	if err != nil {
		t.Fatal(err)
	}
	st := out.ConnectionStatus()
	if st == nil {
		t.Fatal("check: no CONNECTION_STATUS")
	}
	if st.Status != synclet.ConnectionStatusFailed {
		t.Fatalf("check with a 403: %s", st.Status)
	}
	const want = "The access token does not have the read_admins permission"
	if !strings.Contains(st.Message, want) || strings.Contains(st.Message, "error.list") {
		t.Errorf("check with a 403: message %q, want the API's text %q without the raw body", st.Message, want)
	}
}
