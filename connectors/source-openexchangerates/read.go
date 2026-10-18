package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// ratesResponse is the body of latest.json and historical/<date>.json.
type ratesResponse struct {
	Timestamp int64                  `json:"timestamp"`
	Base      string                 `json:"base"`
	Rates     map[string]json.Number `json:"rates"`
}

type currencyRecord struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type rateRecord struct {
	Date      string      `json:"date,omitempty"`
	Base      string      `json:"base"`
	Currency  string      `json:"currency"`
	Rate      json.Number `json:"rate"`
	Timestamp int64       `json:"timestamp"`
}

// historicalState is the STREAM state of historical_rates: the last day
// whose rates were all emitted.
type historicalState struct {
	Date string `json:"date"`
}

// Read emits every configured stream in catalog order.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		var err error
		switch desc.Name {
		case streamCurrencies:
			err = readCurrencies(ctx, c, desc, em)
		case streamLatestRates:
			err = readLatest(ctx, c, cfg, desc, em)
		case streamHistoricalRates:
			err = s.readHistorical(ctx, c, cfg, cs, state, em)
		default:
			err = synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func readCurrencies(ctx context.Context, c *apiClient, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	var names map[string]string
	if err := c.get(ctx, "currencies.json", nil, &names); err != nil {
		return err
	}
	for _, code := range slices.Sorted(maps.Keys(names)) {
		if err := em.Record(desc, currencyRecord{Code: code, Name: names[code]}); err != nil {
			return err
		}
	}
	return nil
}

func readLatest(ctx context.Context, c *apiClient, cfg Config, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	var r ratesResponse
	if err := c.get(ctx, "latest.json", cfg.query(), &r); err != nil {
		return err
	}
	return emitRates(desc, "", r, em)
}

// emitRates emits one record per currency of r, sorted by currency.
func emitRates(desc synclet.StreamDescriptor, date string, r ratesResponse, em *synclet.Emitter) error {
	for _, cur := range slices.Sorted(maps.Keys(r.Rates)) {
		rec := rateRecord{Date: date, Base: r.Base, Currency: cur, Rate: r.Rates[cur], Timestamp: r.Timestamp}
		if err := em.Record(desc, rec); err != nil {
			return err
		}
	}
	return nil
}

// readHistorical requests one day at a time from the day after the saved
// date, or from start_date, up to yesterday UTC; an incremental read saves
// each day once its records are out.
func (s Source) readHistorical(ctx context.Context, c *apiClient, cfg Config, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	from, err := cfg.start()
	if err != nil {
		return synclet.ConfigError(err)
	}
	if raw := state.Stream(desc); incremental && raw != nil {
		var st historicalState
		if err := json.Unmarshal(raw, &st); err != nil {
			return synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
		}
		saved, err := time.Parse(dateLayout, st.Date)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("state of %s: date %q is not a YYYY-MM-DD date", desc.Name, st.Date))
		}
		from = saved.AddDate(0, 0, 1)
	}
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	today := now().UTC().Truncate(24 * time.Hour)
	query := cfg.query()
	for day := from; day.Before(today); day = day.AddDate(0, 0, 1) {
		if err := ctx.Err(); err != nil {
			return err
		}
		date := day.Format(dateLayout)
		var r ratesResponse
		if err := c.get(ctx, "historical/"+date+".json", query, &r); err != nil {
			return err
		}
		if err := emitRates(desc, date, r, em); err != nil {
			return err
		}
		if incremental {
			if err := em.StreamState(desc, historicalState{Date: date}); err != nil {
				return err
			}
		}
	}
	return nil
}
