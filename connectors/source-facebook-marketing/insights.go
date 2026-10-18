package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultPollInterval = 2 * time.Second
	// jobRetries is how many times a failed or skipped report job is created
	// again before its stream fails.
	jobRetries = 3
	// maxPolls is how many times a report job's status is asked before the
	// job counts as stuck and is created again.
	maxPolls = 1800

	statusCompleted = "Job Completed"
	statusFailed    = "Job Failed"
	statusSkipped   = "Job Skipped"
)

// insightsState is the STREAM state of an insights stream.
type insightsState struct {
	Accounts map[string]accountState `json:"accounts"`
}

// accountState is the saved progress of one ad account. DateStart is the
// latest date_start emitted. EndDate, set by hand together with an earlier
// DateStart, ends the next read there to re-sync a past range; it is not
// saved again.
type accountState struct {
	DateStart string `json:"date_start,omitempty"`
	EndDate   string `json:"end_date,omitempty"`
}

func loadInsightsState(desc synclet.StreamDescriptor, state *synclet.InputState) (insightsState, error) {
	st := insightsState{Accounts: map[string]accountState{}}
	raw := state.Stream(desc)
	if raw == nil {
		return st, nil
	}
	var saved insightsState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return st, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	for id, a := range saved.Accounts {
		st.Accounts[stripAct(id)] = a
	}
	return st, nil
}

// insightsRange returns the days to read for one account: from the saved
// date_start minus lookback days, never before start_date, or from start_date
// without a saved date; up to the saved end_date, else end_date, else today.
func insightsRange(cfg Config, saved accountState, today time.Time) (since, until time.Time, err error) {
	since, err = cfg.start()
	if err != nil {
		return since, until, err
	}
	if saved.DateStart != "" {
		d, err := parseDate("saved date_start", saved.DateStart)
		if err != nil {
			return since, until, err
		}
		if d = d.AddDate(0, 0, -cfg.lookback()); d.After(since) {
			since = d
		}
	}
	until = today
	end, err := cfg.end()
	if err != nil {
		return since, until, err
	}
	if saved.EndDate != "" {
		if end, err = parseDate("saved end_date", saved.EndDate); err != nil {
			return since, until, err
		}
	}
	if !end.IsZero() && end.Before(until) {
		until = end
	}
	return since, until, nil
}

// readInsights reads an insights stream one account at a time. An
// incremental read saves the state of every account after each account.
func (s Source) readInsights(ctx context.Context, c *apiClient, cfg Config, d streamDef, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	st := insightsState{Accounts: map[string]accountState{}}
	if incremental {
		var err error
		if st, err = loadInsightsState(desc, state); err != nil {
			return err
		}
	}
	today := s.today()
	for _, acct := range cfg.accounts() {
		saved := st.Accounts[acct]
		since, until, err := insightsRange(cfg, saved, today)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("%s: account act_%s: %w", d.name, acct, err))
		}
		latest := saved.DateStart
		if !since.After(until) {
			first, last := since.Format(dateLayout), until.Format(dateLayout)
			err = s.readReport(ctx, c, d, acct, first, last, func(obj map[string]any) error {
				day, _ := obj["date_start"].(string)
				if day < first || day > last {
					return nil
				}
				latest = max(latest, day)
				return em.Record(desc, record(d.fields, obj))
			})
			if err != nil {
				return err
			}
		}
		if incremental {
			st.Accounts[acct] = accountState{DateStart: latest}
			if err := em.StreamState(desc, st); err != nil {
				return err
			}
		}
	}
	return nil
}

// readReport runs an async insights report job for one account and range
// and calls fn with every row. A failed, skipped or stuck job is created
// again up to jobRetries times.
func (s Source) readReport(ctx context.Context, c *apiClient, d streamDef, acct, since, until string, fn func(map[string]any) error) error {
	var status string
	for attempt := 0; attempt <= jobRetries; attempt++ {
		id, st, err := s.runJob(ctx, c, d, acct, since, until)
		if err != nil {
			return fmt.Errorf("%s: account act_%s %s..%s: %w", d.name, acct, since, until, err)
		}
		if st == statusCompleted {
			return c.list(ctx, id+"/insights", nil, fn)
		}
		status = st
	}
	return fmt.Errorf("%s: account act_%s %s..%s: report job ended with %q %d times", d.name, acct, since, until, status, jobRetries+1)
}

// runJob creates a report job and polls it until it completes, fails or is
// skipped, and returns its id and last status.
func (s Source) runJob(ctx context.Context, c *apiClient, d streamDef, acct, since, until string) (string, string, error) {
	timeRange, err := json.Marshal(map[string]string{"since": since, "until": until})
	if err != nil {
		return "", "", err
	}
	form := url.Values{
		"level":          {d.level},
		"time_increment": {"1"},
		"time_range":     {string(timeRange)},
		"fields":         {strings.Join(d.requestFields(), ",")},
	}
	if len(d.breakdowns) > 0 {
		form.Set("breakdowns", strings.Join(d.breakdowns, ","))
	}
	var created struct {
		ReportRunID string `json:"report_run_id"`
	}
	if err := c.post(ctx, "act_"+acct+"/insights", form, &created); err != nil {
		return "", "", err
	}
	if created.ReportRunID == "" {
		return "", "", fmt.Errorf("report job created without a report_run_id")
	}
	interval := s.pollInterval
	if interval == 0 {
		interval = defaultPollInterval
	}
	for range maxPolls {
		var run struct {
			Status string `json:"async_status"`
		}
		q := url.Values{"fields": {"async_status,async_percent_completion"}}
		if err := c.get(ctx, created.ReportRunID, q, &run); err != nil {
			return "", "", err
		}
		switch run.Status {
		case statusCompleted, statusFailed, statusSkipped:
			return created.ReportRunID, run.Status, nil
		}
		if err := c.wait(ctx, interval); err != nil {
			return "", "", err
		}
	}
	return created.ReportRunID, "not completed after polling", nil
}
