package apiwatch

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const (
	LabelWatch       = "api-watch"
	LabelSunset90    = "sunset-90"
	LabelSunset30    = "sunset-30"
	LabelBump        = "api-bump"
	LabelNeedsReview = "needs-review"

	checkersKey = "checkers"
)

var labelColors = map[string]string{
	LabelWatch: "1d76db", LabelSunset90: "fbca04", LabelSunset30: "d93f0b", LabelBump: "0e8a16", LabelNeedsReview: "5319e7",
}

var (
	reMarker = regexp.MustCompile(`<!-- api-watch: (\S+) -->`)
	// reRefSafe bounds the upstream strings that end up in branch names and
	// the bump jobs' commands.
	reRefSafe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,63}$`)
)

// Key identifies the issue of one connector's API.
func Key(e Entry) string {
	return e.Connector + "/" + slug(e.API)
}

func slug(s string) string {
	var b strings.Builder

	dash := false

	for _, r := range strings.ToLower(s) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)

			dash = false
		} else if !dash && b.Len() > 0 {
			b.WriteByte('-')

			dash = true
		}
	}

	return strings.TrimSuffix(b.String(), "-")
}

// BranchName is the branch a bump of the connector to version is drafted on.
func BranchName(connector, version string) string {
	return "api-bump/" + connector + "/" + strings.ReplaceAll(version, "/", "-")
}

// Action is one change to the repository's issues.
type Action struct {
	Kind    string // create, update, close
	Key     string
	Number  int
	Title   string
	Body    string
	Labels  []string
	Comment string
}

// Plan compares the issues the report calls for with the open api-watch
// issues: a finding without an issue is created, a changed one updated, and
// the issue of an entry that is ok again closed. Entries a run could not
// check, or did not check, leave their issues alone. A run of every pin also
// closes the issues of pins no longer declared and keeps the checkers issue;
// a run of one connector touches neither.
func Plan(report Report, open []Issue, repoURL string) []Action {
	existing := map[string]Issue{}

	for _, is := range open {
		m := reMarker.FindStringSubmatch(is.Body)
		if m == nil {
			continue
		}

		if _, dup := existing[m[1]]; !dup {
			existing[m[1]] = is
		}
	}

	var (
		actions []Action
		failed  []Entry
		polled  int
	)

	want := func(key, title, body string, managed []string) {
		is, ok := existing[key]
		if !ok {
			actions = append(actions, Action{Kind: "create", Key: key, Title: title, Body: body, Labels: append([]string{LabelWatch}, managed...)})

			return
		}

		labels := mergeLabels(is.LabelNames(), managed)
		if is.Title == title && stableBody(is.Body) == stableBody(body) && slices.Equal(labels, sorted(is.LabelNames())) {
			return
		}

		actions = append(actions, Action{Kind: "update", Key: key, Number: is.Number, Title: title, Body: body, Labels: labels})
	}

	closeIssue := func(key, comment string) {
		if is, ok := existing[key]; ok {
			actions = append(actions, Action{Kind: "close", Key: key, Number: is.Number, Comment: comment})
		}
	}

	for _, e := range report.Entries {
		switch e.Status {
		case StatusUnchecked:
			continue
		case StatusCheckerError:
			polled++

			failed = append(failed, e)
		case StatusOK:
			polled++

			closeIssue(Key(e), fmt.Sprintf("Resolved on %s: %s pins %s `%s`; upstream latest is `%s`.",
				report.Checked, e.Connector, e.API, e.Pinned, e.Latest))
		default:
			polled++

			want(Key(e), Title(e), Body(e, report.Checked, repoURL), sunsetLabels(e.Status))
		}
	}

	if report.Connector != "" {
		return actions
	}

	declared := map[string]bool{checkersKey: true}
	for _, e := range report.Entries {
		declared[Key(e)] = true
	}

	for _, key := range slices.Sorted(maps.Keys(existing)) {
		if !declared[key] {
			closeIssue(key, "Resolved on "+report.Checked+": the connector no longer declares this pin in `api_versions`.")
		}
	}

	if len(failed) > 0 {
		want(checkersKey, fmt.Sprintf("api-watch: %d upstream version checks failed", len(failed)),
			checkersBody(failed, report.Checked), nil)
	} else if polled > 0 {
		closeIssue(checkersKey, "Every upstream version check passed on "+report.Checked+".")
	}

	return actions
}

func sunsetLabels(s Status) []string {
	switch s {
	case StatusSunset90:
		return []string{LabelSunset90}
	case StatusSunset30, StatusSunsetPassed:
		return []string{LabelSunset30}
	}

	return nil
}

// mergeLabels keeps the labels people added and replaces the sunset ones.
func mergeLabels(current, managed []string) []string {
	out := []string{LabelWatch}

	for _, l := range current {
		if l != LabelWatch && l != LabelSunset90 && l != LabelSunset30 {
			out = append(out, l)
		}
	}

	return sorted(append(out, managed...))
}

func sorted(s []string) []string {
	out := slices.Clone(s)
	sort.Strings(out)

	return slices.Compact(out)
}

// stableBody drops the line that changes on every run.
func stableBody(body string) string {
	var keep []string

	for _, l := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
		if !strings.HasPrefix(l, "Last checked:") {
			keep = append(keep, l)
		}
	}

	return strings.TrimSpace(strings.Join(keep, "\n"))
}

// Title is the issue title of a finding.
func Title(e Entry) string {
	head := fmt.Sprintf("%s: %s %s", e.Connector, e.API, e.Pinned)

	switch {
	case e.Status == StatusSunsetPassed && e.Sunset == "":
		return head + " is no longer served upstream"
	case e.Status == StatusSunsetPassed:
		return fmt.Sprintf("%s is past its sunset (%s)", head, e.Sunset)
	case e.Status == StatusSunset30 || e.Status == StatusSunset90:
		return fmt.Sprintf("%s sunsets in %d days (%s)", head, *e.Days, e.Sunset)
	}

	return fmt.Sprintf("%s → %s available", head, e.Latest)
}

// Body is the issue body of a finding; the marker line finds it again.
func Body(e Entry, checked, repoURL string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "<!-- api-watch: %s -->\n", Key(e))
	fmt.Fprintf(&b, "**%s** pins **%s** `%s`.\n\n", e.Connector, e.API, e.Pinned)
	b.WriteString("| | |\n|---|---|\n")
	fmt.Fprintf(&b, "| Pinned | `%s` in [%s:%d](%s/blob/main/%s#L%d) |\n", e.Pinned, e.File, e.Line, repoURL, e.File, e.Line)

	if e.Latest != "" {
		fmt.Fprintf(&b, "| Latest upstream | `%s` |\n", e.Latest)
	}

	if e.Sunset != "" {
		fmt.Fprintf(&b, "| Sunset of `%s` | %s |\n", e.Pinned, e.Sunset)
	}

	fmt.Fprintf(&b, "| Status | `%s` |\n", e.Status)

	if e.Note != "" {
		fmt.Fprintf(&b, "| Note | %s |\n", e.Note)
	}

	fmt.Fprintf(&b, "| Provider docs | %s |\n", e.Docs)

	if e.Source != "" {
		fmt.Fprintf(&b, "| Checked against | %s |\n", e.Source)
	}

	b.WriteString("\nBump the pin and `api_versions` in `meta.json` in one commit. ")

	if e.Newer() {
		fmt.Fprintf(&b, "With the `ANTHROPIC_API_KEY` secret set, the API watch workflow drafts the bump on branch `%s`; "+
			"delete that branch and run the workflow with `connector: %s` to draft it again.\n", BranchName(e.Connector, e.Latest), e.Connector)
	} else {
		b.WriteString("Upstream lists no newer version to move to.\n")
	}

	fmt.Fprintf(&b, "\nLast checked: %s\n", checked)

	return b.String()
}

func checkersBody(failed []Entry, checked string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "<!-- api-watch: %s -->\n", checkersKey)
	b.WriteString("These upstream version checks failed, so their pins went unchecked. A provider page probably changed " +
		"shape: update the checker in `scripts/apiwatch` and its fixture.\n\n")
	b.WriteString("| Connector | API | Checker | Error |\n|---|---|---|---|\n")

	for _, e := range failed {
		fmt.Fprintf(&b, "| %s | %s | `%s` | %s |\n", e.Connector, e.API, e.Checker, strings.ReplaceAll(e.Error, "|", `\|`))
	}

	fmt.Fprintf(&b, "\nLast checked: %s\n", checked)

	return b.String()
}

// Bump is a finding the LLM job may draft a pull request for.
type Bump struct {
	Connector string `json:"connector"`
	API       string `json:"api"`
	From      string `json:"from"`
	To        string `json:"to"`
	File      string `json:"file"`
	Docs      string `json:"docs"`
	Hosts     string `json:"hosts"`
	Branch    string `json:"branch"`
	Issue     int    `json:"issue"`
}

// SyncIssues applies Plan, then returns the findings that have no bump
// branch or pull request yet. It fails when a pin is within 30 days of its
// sunset (or past it) without an open bump pull request, and when every
// polled check failed.
func SyncIssues(ctx context.Context, gh *GitHub, report Report) ([]Bump, error) {
	open, err := gh.OpenIssues(ctx, LabelWatch)
	if err != nil {
		return nil, err
	}

	actions := Plan(report, open, gh.RepoURL())

	for _, l := range []string{LabelWatch, LabelSunset90, LabelSunset30, LabelBump, LabelNeedsReview} {
		if err := gh.EnsureLabel(ctx, l, labelColors[l]); err != nil {
			return nil, err
		}
	}

	numbers := map[string]int{}

	for _, is := range open {
		if m := reMarker.FindStringSubmatch(is.Body); m != nil {
			if _, ok := numbers[m[1]]; !ok {
				numbers[m[1]] = is.Number
			}
		}
	}

	for _, a := range actions {
		n, err := gh.Apply(ctx, a)
		if err != nil {
			return nil, err
		}

		numbers[a.Key] = n
	}

	if len(actions) == 0 {
		gh.logf("issues up to date")
	}

	var (
		bumps    []Bump
		errs     []error
		polled   int
		failures int
	)

	for _, e := range report.Entries {
		if e.Status == StatusUnchecked {
			continue
		}

		polled++

		if e.Status == StatusCheckerError {
			failures++

			continue
		}

		branch := BranchName(e.Connector, e.Latest)

		if e.Status == StatusSunset30 || e.Status == StatusSunsetPassed {
			openPR := false
			if e.Newer() {
				if openPR, err = gh.PullExists(ctx, branch, "open"); err != nil {
					return nil, err
				}
			}

			if !openPR {
				errs = append(errs, fmt.Errorf("%s %s %s: %s without an open bump pull request", e.Connector, e.API, e.Pinned, e.Status))
			}
		}

		if !e.Newer() {
			continue
		}

		if !reRefSafe.MatchString(e.Latest) || strings.Contains(e.Latest, "..") {
			gh.logf("bump of %s to %q skipped: not a safe branch name", e.Connector, e.Latest)

			continue
		}

		taken, err := gh.BranchExists(ctx, branch)
		if err != nil {
			return nil, err
		}

		if !taken {
			if taken, err = gh.PullExists(ctx, branch, "all"); err != nil {
				return nil, err
			}
		}

		if taken {
			gh.logf("bump %s exists; skipped", branch)

			continue
		}

		bumps = append(bumps, Bump{
			Connector: e.Connector, API: e.API, From: e.Pinned, To: e.Latest, File: e.File, Docs: e.Docs,
			Hosts: strings.Join(hosts(e.Docs, e.Source), ","), Branch: branch, Issue: numbers[Key(e)],
		})
	}

	if polled > 0 && failures == polled {
		errs = append(errs, errors.New("every upstream version check failed"))
	}

	if bumps == nil {
		bumps = []Bump{}
	}

	return bumps, errors.Join(errs...)
}

// hosts lists the distinct hosts of the URLs, for the draft job's WebFetch allow list.
func hosts(urls ...string) []string {
	var out []string

	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || slices.Contains(out, u.Host) {
			continue
		}

		out = append(out, u.Host)
	}

	return out
}
