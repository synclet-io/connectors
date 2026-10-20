package apiwatch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeGitHub keeps issues, labels, branches and pull requests of one
// repository in memory and serves the endpoints GitHub uses.
type fakeGitHub struct {
	mu       sync.Mutex
	issues   []*fakeIssue
	labels   map[string]bool
	branches map[string]bool
	pulls    map[string]string // head branch -> state
	writes   int
}

type fakeIssue struct {
	Number   int      `json:"number"`
	Title    string   `json:"title"`
	Body     string   `json:"body"`
	State    string   `json:"state"`
	Labels   []string `json:"-"`
	Comments []string `json:"-"`
	IsPull   bool     `json:"-"`
}

func newFakeGitHub(t *testing.T) (*fakeGitHub, *GitHub) {
	f := &fakeGitHub{labels: map[string]bool{}, branches: map[string]bool{}, pulls: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	gh := NewGitHub("synclet-io/connectors", "test-token")
	gh.BaseURL = srv.URL
	gh.Client = srv.Client()

	return f, gh
}

func (f *fakeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(w, "no token", http.StatusUnauthorized)

		return
	}

	var in map[string]any
	if r.Body != nil {
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &in)
	}

	if r.Method != http.MethodGet {
		f.writes++
	}

	path := strings.TrimPrefix(r.URL.EscapedPath(), "/repos/synclet-io/connectors/")
	parts := strings.Split(path, "/")

	switch {
	case r.Method == http.MethodGet && path == "issues":
		f.listIssues(w, r)
	case r.Method == http.MethodPost && path == "issues":
		is := &fakeIssue{Number: len(f.issues) + 1, Title: in["title"].(string), Body: in["body"].(string), State: "open", Labels: strs(in["labels"])}
		f.issues = append(f.issues, is)
		writeJSON(w, http.StatusCreated, is)
	case r.Method == http.MethodPatch && len(parts) == 2 && parts[0] == "issues":
		is := f.issue(parts[1])
		if v, ok := in["title"].(string); ok {
			is.Title = v
		}

		if v, ok := in["body"].(string); ok {
			is.Body = v
		}

		if v, ok := in["state"].(string); ok {
			is.State = v
		}

		if _, ok := in["labels"]; ok {
			is.Labels = strs(in["labels"])
		}

		writeJSON(w, http.StatusOK, is)
	case r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "comments":
		is := f.issue(parts[1])
		is.Comments = append(is.Comments, in["body"].(string))
		writeJSON(w, http.StatusCreated, map[string]int{"id": len(is.Comments)})
	case r.Method == http.MethodPost && path == "labels":
		name := in["name"].(string)
		if f.labels[name] {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"message": "Validation Failed"})

			return
		}

		f.labels[name] = true
		writeJSON(w, http.StatusCreated, in)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "git/ref/heads/"):
		if f.branches[strings.TrimPrefix(r.URL.Path, "/repos/synclet-io/connectors/git/ref/heads/")] {
			writeJSON(w, http.StatusOK, map[string]string{"ref": path})

			return
		}

		writeJSON(w, http.StatusNotFound, map[string]string{"message": "Not Found"})
	case r.Method == http.MethodGet && path == "pulls":
		head := strings.TrimPrefix(r.URL.Query().Get("head"), "synclet-io:")
		state, ok := f.pulls[head]
		want := r.URL.Query().Get("state")

		if ok && (want == "all" || want == state) {
			writeJSON(w, http.StatusOK, []map[string]string{{"state": state}})

			return
		}

		writeJSON(w, http.StatusOK, []any{})
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusTeapot)
	}
}

func (f *fakeGitHub) listIssues(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	perPage, _ := strconv.Atoi(q.Get("per_page"))

	type item struct {
		*fakeIssue
		Labels      []issueLabel `json:"labels"`
		PullRequest any          `json:"pull_request,omitempty"`
	}

	var match []item

	for _, is := range f.issues {
		if is.State != q.Get("state") || !slices.Contains(is.Labels, q.Get("labels")) {
			continue
		}

		it := item{fakeIssue: is}
		for _, l := range is.Labels {
			it.Labels = append(it.Labels, issueLabel{Name: l})
		}

		if is.IsPull {
			it.PullRequest = map[string]string{"url": "x"}
		}

		match = append(match, it)
	}

	start := min((page-1)*perPage, len(match))
	end := min(start+perPage, len(match))
	writeJSON(w, http.StatusOK, match[start:end])
}

func (f *fakeGitHub) issue(number string) *fakeIssue {
	n, _ := strconv.Atoi(number)

	return f.issues[n-1]
}

func (f *fakeGitHub) open() []*fakeIssue {
	var out []*fakeIssue

	for _, is := range f.issues {
		if is.State == "open" && !is.IsPull {
			out = append(out, is)
		}
	}

	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func strs(v any) []string {
	var out []string

	list, _ := v.([]any)
	for _, s := range list {
		out = append(out, s.(string))
	}

	return out
}

func days(n int) *int { return &n }

func entry(api string, status Status, latest, sunset string, d *int) Entry {
	return Entry{
		Connector: "source-" + strings.ToLower(api), API: api + " API", Pinned: "2025-01-01", Latest: latest,
		Sunset: sunset, Days: d, Status: status, Checker: strings.ToLower(api), Docs: "https://docs.example.com/" + strings.ToLower(api),
		Source: "https://changelog.example.com/" + strings.ToLower(api), File: "connectors/source-x/manifest.yaml", Line: 7,
	}
}

func TestIssuesPlanner(t *testing.T) {
	f, gh := newFakeGitHub(t)
	ctx := context.Background()

	// An unrelated open pull request with the label must never be touched.
	f.issues = append(f.issues, &fakeIssue{Number: 1, Title: "a PR", Body: "<!-- api-watch: source-stripe/stripe-api -->", State: "open", Labels: []string{LabelWatch}, IsPull: true})

	run1 := Report{Checked: "2026-10-19", Entries: []Entry{
		entry("Stripe", StatusNewVersion, "2026-09-30.endive", "", nil),
		entry("Klaviyo", StatusSunset90, "2026-10-15", "2026-12-31", days(71)),
		entry("Notion", StatusOK, "2025-01-01", "", nil),
		entry("Shopify", StatusCheckerError, "", "", nil),
		{Connector: "source-hubspot", API: "HubSpot CRM API", Pinned: "v3", Status: StatusUnchecked},
	}}
	run1.Entries[3].Error = "release schedule table not found"

	bumps, err := SyncIssues(ctx, gh, run1)
	if err != nil {
		t.Fatal(err)
	}

	open := f.open()
	if len(open) != 3 {
		t.Fatalf("%d open issues after the first run, want stripe, klaviyo and checkers: %+v", len(open), open)
	}

	stripe := findIssue(t, open, "source-stripe/stripe-api")
	if stripe.Title != "source-stripe: Stripe API 2025-01-01 → 2026-09-30.endive available" {
		t.Errorf("stripe title = %q", stripe.Title)
	}

	if !slices.Equal(stripe.Labels, []string{LabelWatch}) {
		t.Errorf("stripe labels = %v", stripe.Labels)
	}

	klaviyo := findIssue(t, open, "source-klaviyo/klaviyo-api")
	if klaviyo.Title != "source-klaviyo: Klaviyo API 2025-01-01 sunsets in 71 days (2026-12-31)" || !slices.Contains(klaviyo.Labels, LabelSunset90) {
		t.Errorf("klaviyo issue = %q %v", klaviyo.Title, klaviyo.Labels)
	}

	checkers := findIssue(t, open, checkersKey)
	if !strings.Contains(checkers.Body, "release schedule table not found") {
		t.Errorf("checkers body = %s", checkers.Body)
	}

	for _, l := range []string{LabelWatch, LabelSunset90, LabelSunset30, LabelBump, LabelNeedsReview} {
		if !f.labels[l] {
			t.Errorf("label %s not created", l)
		}
	}

	if len(bumps) != 2 || bumps[0].Connector != "source-stripe" || bumps[0].Issue != stripe.Number ||
		bumps[0].Branch != "api-bump/source-stripe/2026-09-30.endive" || bumps[0].Hosts != "docs.example.com,changelog.example.com" {
		t.Errorf("bumps = %+v", bumps)
	}

	// The same findings a week later: nothing is written but the labels.
	f.writes = 0
	rerun := run1
	rerun.Checked = "2026-10-26"

	if _, err := SyncIssues(ctx, gh, rerun); err != nil {
		t.Fatal(err)
	}

	if f.writes != 5 || len(f.open()) != 3 {
		t.Errorf("rerun made %d writes (want only the 5 label checks) and left %d open issues", f.writes, len(f.open()))
	}

	// Klaviyo nears its sunset, Stripe is bumped, Shopify's checker works again.
	f.pulls["api-bump/source-klaviyo/2026-10-15"] = "open"
	f.branches["api-bump/source-klaviyo/2026-10-15"] = true

	run3 := Report{Checked: "2026-12-07", Entries: []Entry{
		entry("Stripe", StatusOK, "2026-09-30.endive", "", nil),
		entry("Klaviyo", StatusSunset30, "2026-10-15", "2026-12-31", days(24)),
		entry("Shopify", StatusOK, "2025-01-01", "", nil),
	}}
	run3.Entries[0].Pinned = "2026-09-30.endive"

	bumps, err = SyncIssues(ctx, gh, run3)
	if err != nil {
		t.Fatalf("sunset_30 with an open bump pull request must not fail: %v", err)
	}

	if len(bumps) != 0 {
		t.Errorf("bumps = %+v, want none: klaviyo's branch exists", bumps)
	}

	if stripe.State != "closed" || len(stripe.Comments) != 1 || checkers.State != "closed" {
		t.Errorf("stripe %s %v, checkers %s: want both closed with a comment", stripe.State, stripe.Comments, checkers.State)
	}

	if !slices.Equal(klaviyo.Labels, []string{LabelWatch, LabelSunset30}) || !strings.Contains(klaviyo.Title, "24 days") {
		t.Errorf("klaviyo = %q %v, want the sunset-30 label swapped in", klaviyo.Title, klaviyo.Labels)
	}

	// Without the pull request the near sunset fails the run.
	delete(f.pulls, "api-bump/source-klaviyo/2026-10-15")

	if _, err := SyncIssues(ctx, gh, run3); err == nil || !strings.Contains(err.Error(), "without an open bump pull request") {
		t.Errorf("err = %v, want the sunset escalation", err)
	}
}

func TestIssuesKeepPeoplesLabels(t *testing.T) {
	f, gh := newFakeGitHub(t)
	e := entry("Stripe", StatusNewVersion, "2026-09-30.endive", "", nil)
	f.issues = append(f.issues, &fakeIssue{
		Number: 1, Title: "old title", Body: Body(e, "2026-10-01", gh.RepoURL()), State: "open", Labels: []string{LabelWatch, "help wanted"},
	})

	if _, err := SyncIssues(context.Background(), gh, Report{Checked: "2026-10-21", Entries: []Entry{e}}); err != nil {
		t.Fatal(err)
	}

	is := f.issues[0]
	if is.Title != Title(e) || !slices.Equal(is.Labels, []string{LabelWatch, "help wanted"}) || len(f.open()) != 1 {
		t.Errorf("issue = %q %v, open %d", is.Title, is.Labels, len(f.open()))
	}
}

func TestIssuesFailWhenEveryCheckFails(t *testing.T) {
	_, gh := newFakeGitHub(t)

	e := entry("Stripe", StatusCheckerError, "", "", nil)
	if _, err := SyncIssues(context.Background(), gh, Report{Checked: "2026-10-21", Entries: []Entry{e}}); err == nil {
		t.Error("no error when every check failed")
	}
}

func TestIssuesDryRunWritesNothing(t *testing.T) {
	f, gh := newFakeGitHub(t)
	gh.DryRun = true

	var log strings.Builder

	gh.Log = &log

	bumps, err := SyncIssues(context.Background(), gh, Report{Checked: "2026-10-21", Entries: []Entry{
		entry("Stripe", StatusNewVersion, "2026-09-30.endive", "", nil),
	}})
	if err != nil {
		t.Fatal(err)
	}

	if f.writes != 0 || len(bumps) != 1 || !strings.Contains(log.String(), "dry run: would create") {
		t.Errorf("writes %d, bumps %+v, log %q", f.writes, bumps, log.String())
	}
}

func TestIssuesPaginate(t *testing.T) {
	f, gh := newFakeGitHub(t)

	for i := range 150 {
		f.issues = append(f.issues, &fakeIssue{Number: i + 1, Title: "t", Body: "b", State: "open", Labels: []string{LabelWatch}})
	}

	open, err := gh.OpenIssues(context.Background(), LabelWatch)
	if err != nil || len(open) != 150 {
		t.Fatalf("OpenIssues = %d, %v; want 150", len(open), err)
	}
}

func findIssue(t *testing.T, issues []*fakeIssue, key string) *fakeIssue {
	t.Helper()

	for _, is := range issues {
		if strings.Contains(is.Body, "<!-- api-watch: "+key+" -->") {
			return is
		}
	}

	t.Fatalf("no open issue for %s", key)

	return nil
}

func TestUnsafeUpstreamVersionIsNotBumped(t *testing.T) {
	_, gh := newFakeGitHub(t)

	bumps, err := SyncIssues(context.Background(), gh, Report{Checked: "2026-10-21", Entries: []Entry{
		entry("Stripe", StatusNewVersion, "2026-09-30.endive$(curl evil)", "", nil),
		entry("Notion", StatusNewVersion, "../../main", "", nil),
	}})
	if err != nil {
		t.Fatal(err)
	}

	if len(bumps) != 0 {
		t.Errorf("bumps = %+v, want none", bumps)
	}
}
