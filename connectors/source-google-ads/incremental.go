package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// dailyState is the STREAM state of a daily stream: per customer, the last
// day of its last read range.
type dailyState struct {
	Customers map[string]customerState `json:"customers"`
}

type customerState struct {
	Date string `json:"date"`
}

func loadDailyState(desc synclet.StreamDescriptor, state *synclet.InputState) (dailyState, error) {
	st := dailyState{Customers: map[string]customerState{}}
	raw := state.Stream(desc)
	if raw == nil {
		return st, nil
	}
	var saved dailyState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return st, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	for id, c := range saved.Customers {
		st.Customers[customerID(id)] = c
	}
	return st, nil
}

// dailyRange returns the days to read for one customer: from the saved date
// minus lookback_days, never before start_date nor before the 37 months of
// history Google Ads keeps, or from start_date without a saved date; up to
// end_date, else today.
func dailyRange(cfg Config, saved string, today time.Time) (since, until time.Time, err error) {
	if since, err = cfg.start(); err != nil {
		return since, until, err
	}
	if saved != "" {
		d, err := parseDate("saved date", saved)
		if err != nil {
			return since, until, err
		}
		if d = d.AddDate(0, 0, -cfg.lookback()); d.After(since) {
			since = d
		}
	}
	if floor := today.AddDate(0, -historyMonths, 0); floor.After(since) {
		since = floor
	}
	until = today
	end, err := cfg.end()
	if err != nil {
		return since, until, err
	}
	if !end.IsZero() && end.Before(until) {
		until = end
	}
	return since, until, nil
}

// readDaily reads a daily stream one account at a time over its date range
// and emits only rows whose segments.date is in that range. An incremental
// read saves the state of every account after each account.
func (s Source) readDaily(ctx context.Context, c *apiClient, cfg Config, d streamDef, fs []field, cs synclet.ConfiguredStream,
	accts []account, state *synclet.InputState, em *synclet.Emitter,
) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	st := dailyState{Customers: map[string]customerState{}}
	if incremental {
		var err error
		if st, err = loadDailyState(desc, state); err != nil {
			return err
		}
	}
	today := s.today()
	for _, a := range accts {
		saved := st.Customers[a.id]
		since, until, err := dailyRange(cfg, saved.Date, today)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("%s: customer %s: %w", d.name, a.id, err))
		}
		if since.After(until) {
			continue
		}
		first, last := since.Format(dateLayout), until.Format(dateLayout)
		err = c.search(ctx, a.id, a.login, d.query.withDateRange(first, last), func(row map[string]any) error {
			day, _ := lookup(row, dateField)
			if s, ok := day.(string); !ok || s < first || s > last {
				return nil
			}
			return em.Record(desc, record(fs, row))
		})
		if err != nil {
			return fmt.Errorf("%s: customer %s: %w", d.name, a.id, err)
		}
		if incremental {
			st.Customers[a.id] = customerState{Date: last}
			if err := em.StreamState(desc, st); err != nil {
				return err
			}
		}
	}
	return nil
}
