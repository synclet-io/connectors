// Package apiwatch polls upstream providers for the API versions connectors pin
// and reports newer and sunset versions.
package apiwatch

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Target is one pinned upstream API version of a connector.
type Target struct {
	Connector string
	API       string
	Pinned    string
	File      string // relative to the repository root
	Line      int
	Checker   string
	Params    map[string]string
	Docs      string
}

// Result is what a checker found upstream. Latest has the format of the
// pinned version. Hold keeps the entry ok although Latest is newer; Gone
// means upstream no longer serves the pinned version.
type Result struct {
	Latest string
	Sunset *time.Time
	Source string
	Note   string
	Hold   bool
	Gone   bool
}

// Checker finds the newest version of one provider's API and the sunset date
// of the pinned one. Compare orders two versions in the pinned format.
type Checker interface {
	ID() string
	Check(ctx context.Context, client *http.Client, pinned string, params map[string]string) (Result, error)
	Compare(a, b string) int
}

type Status string

const (
	StatusOK           Status = "ok"
	StatusNewVersion   Status = "new_version"
	StatusSunset90     Status = "sunset_90"
	StatusSunset30     Status = "sunset_30"
	StatusSunsetPassed Status = "sunset_passed"
	StatusCheckerError Status = "checker_error"
	StatusUnchecked    Status = "unchecked"
)

// Entry is one target's line of the report.
type Entry struct {
	Connector string `json:"connector"`
	API       string `json:"api"`
	Pinned    string `json:"pinned"`
	Latest    string `json:"latest,omitempty"`
	Sunset    string `json:"sunset,omitempty"`
	Days      *int   `json:"days,omitempty"`
	Status    Status `json:"status"`
	Checker   string `json:"checker"`
	Source    string `json:"source,omitempty"`
	Docs      string `json:"docs"`
	File      string `json:"file"`
	Line      int    `json:"line"`
	Note      string `json:"note,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Newer reports whether upstream has a version the connector could move to.
func (e Entry) Newer() bool {
	return e.Latest != "" && e.Latest != e.Pinned && e.Status != StatusCheckerError && e.Status != StatusUnchecked
}

type Report struct {
	Checked string  `json:"checked"`
	Entries []Entry `json:"entries"`
}

// None is the checker id of pins that are listed but never polled.
const None = "none"

// Checkers returns every checker by id.
func Checkers() map[string]Checker {
	all := []Checker{
		metaGraph{}, googleAds{}, linkedIn{}, shopify{}, tiktokProbe{}, stripe{}, klaviyo{},
		intercom{}, notion{}, github{}, salesforce{}, googleDiscovery{},
	}

	m := make(map[string]Checker, len(all))
	for _, c := range all {
		m[c.ID()] = c
	}

	return m
}

// Run checks every target in order; a failing checker becomes a
// checker_error entry and never stops the run.
func Run(ctx context.Context, client *http.Client, targets []Target, now time.Time) Report {
	checkers := Checkers()
	today := day(now)
	report := Report{Checked: today.Format(time.DateOnly)}

	for _, t := range targets {
		e := Entry{
			Connector: t.Connector, API: t.API, Pinned: t.Pinned, Checker: t.Checker,
			Docs: t.Docs, File: t.File, Line: t.Line,
		}

		c, ok := checkers[t.Checker]

		switch {
		case t.Checker == None:
			e.Status = StatusUnchecked
		case !ok:
			e.Status = StatusCheckerError
			e.Error = fmt.Sprintf("unknown checker %q", t.Checker)
		default:
			r, err := c.Check(ctx, client, t.Pinned, t.Params)
			if err != nil {
				e.Status = StatusCheckerError
				e.Error = err.Error()

				break
			}

			e.Latest, e.Source, e.Note = r.Latest, r.Source, r.Note
			newer := !r.Hold && c.Compare(t.Pinned, r.Latest) < 0
			e.Status = status(newer, r.Sunset, today)

			if r.Gone {
				e.Status = StatusSunsetPassed
			}

			if r.Sunset != nil {
				days := int(day(*r.Sunset).Sub(today).Hours() / 24)
				e.Sunset = r.Sunset.Format(time.DateOnly)
				e.Days = &days
			}
		}

		report.Entries = append(report.Entries, e)
	}

	return report
}

func status(newer bool, sunset *time.Time, today time.Time) Status {
	if sunset != nil {
		days := day(*sunset).Sub(today).Hours() / 24

		switch {
		case days < 0:
			return StatusSunsetPassed
		case days <= 30:
			return StatusSunset30
		case days <= 90:
			return StatusSunset90
		}
	}

	if newer {
		return StatusNewVersion
	}

	return StatusOK
}

func day(t time.Time) time.Time {
	y, m, d := t.UTC().Date()

	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// Markdown renders the report as a table for the job summary.
func Markdown(r Report) string {
	var b strings.Builder

	fmt.Fprintf(&b, "### Upstream API versions (%s)\n\n", r.Checked)
	b.WriteString("| Connector | API | Pinned | Latest | Sunset | Status |\n|---|---|---|---|---|---|\n")

	entries := append([]Entry(nil), r.Entries...)
	sort.SliceStable(entries, func(i, j int) bool { return rank(entries[i].Status) < rank(entries[j].Status) })

	for _, e := range entries {
		st := string(e.Status)
		if e.Error != "" {
			st += ": " + e.Error
		} else if e.Note != "" {
			st += " (" + e.Note + ")"
		}

		fmt.Fprintf(&b, "| %s | [%s](%s) | `%s` | %s | %s | %s |\n",
			e.Connector, e.API, e.Docs, e.Pinned, code(e.Latest), e.Sunset, strings.ReplaceAll(st, "|", `\|`))
	}

	return b.String()
}

func code(s string) string {
	if s == "" {
		return ""
	}

	return "`" + s + "`"
}

func rank(s Status) int {
	return map[Status]int{
		StatusSunsetPassed: 0, StatusSunset30: 1, StatusSunset90: 2, StatusNewVersion: 3,
		StatusCheckerError: 4, StatusOK: 5, StatusUnchecked: 6,
	}[s]
}
