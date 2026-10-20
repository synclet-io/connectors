package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// annotations emits the annotations of the whole date range in one request.
func (r *reader) annotations(ctx context.Context, desc synclet.StreamDescriptor) error {
	var resp struct {
		Annotations []map[string]any `json:"annotations"`
	}
	params := url.Values{
		"from_date": {r.days.start.Format(dateLayout)},
		"to_date":   {r.days.end.Format(dateLayout)},
	}
	if err := r.c.getJSON(ctx, get(queryAPI, "annotations", params), &resp); err != nil {
		return err
	}
	for _, a := range resp.Annotations {
		if err := r.em.Record(desc, a); err != nil {
			return err
		}
	}
	return nil
}

// funnelsState is the state of funnels: the last day read of each funnel,
// by funnel_id.
type funnelsState struct {
	Funnels map[string]dayCursor `json:"funnels"`
}

type funnelInfo struct {
	FunnelID int64  `json:"funnel_id"`
	Name     string `json:"name"`
}

type funnelDay struct {
	Steps    []any          `json:"steps"`
	Analysis map[string]any `json:"analysis"`
}

// funnels emits one record per saved funnel and day, reading each funnel
// window by window with unit=day. An incremental read of a funnel starts
// attribution_window days before its saved day and emits the state after
// each window.
func (r *reader) funnels(ctx context.Context, desc synclet.StreamDescriptor, inc bool, saved json.RawMessage) error {
	st := funnelsState{Funnels: map[string]dayCursor{}}
	if err := decodeState(desc, saved, &st); err != nil {
		return err
	}
	if st.Funnels == nil {
		st.Funnels = map[string]dayCursor{}
	}
	var list []funnelInfo
	if err := r.c.getJSON(ctx, get(queryAPI, "funnels/list", nil), &list); err != nil {
		return err
	}
	for _, f := range list {
		key := strconv.FormatInt(f.FunnelID, 10)
		from, err := r.since(st.Funnels[key].Date)
		if err != nil {
			return err
		}
		for _, w := range r.windowsFrom(from) {
			var resp struct {
				Data map[string]funnelDay `json:"data"`
			}
			params := url.Values{
				"funnel_id": {key},
				"from_date": {w.fromDate()},
				"to_date":   {w.toDate()},
				"unit":      {"day"},
			}
			if err := r.c.getJSON(ctx, get(queryAPI, "funnels", params), &resp); err != nil {
				return fmt.Errorf("funnel %d, %s to %s: %w", f.FunnelID, w.fromDate(), w.toDate(), err)
			}
			for _, day := range sortedDays(resp.Data) {
				d := resp.Data[day]
				rec := map[string]any{"funnel_id": f.FunnelID, "name": f.Name, "date": day, "steps": d.Steps, "analysis": d.Analysis}
				if err := r.em.Record(desc, rec); err != nil {
					return err
				}
				if day > st.Funnels[key].Date {
					st.Funnels[key] = dayCursor{Date: day}
				}
			}
			if inc {
				if err := r.em.StreamState(desc, st); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// sortedDays returns the YYYY-MM-DD keys of m in order, leaving out the
// totals Mixpanel keys with $.
func sortedDays[V any](m map[string]V) []string {
	days := make([]string, 0, len(m))
	for k := range m {
		if _, err := optionalDate("day", k); err == nil && len(k) == len(dateLayout) {
			days = append(days, k)
		}
	}
	sort.Strings(days)
	return days
}

type revenueDay struct {
	Amount    json.Number `json:"amount"`
	Count     json.Number `json:"count"`
	PaidCount json.Number `json:"paid_count"`
}

// revenue emits the daily revenue of engage/revenue window by window. An
// incremental read starts attribution_window days before the saved day and
// emits the state after each window.
func (r *reader) revenue(ctx context.Context, desc synclet.StreamDescriptor, inc bool, saved json.RawMessage) error {
	var st dayCursor
	if err := decodeState(desc, saved, &st); err != nil {
		return err
	}
	from, err := r.since(st.Date)
	if err != nil {
		return err
	}
	for _, w := range r.windowsFrom(from) {
		var resp struct {
			Results map[string]revenueDay `json:"results"`
		}
		params := url.Values{"from_date": {w.fromDate()}, "to_date": {w.toDate()}, "unit": {"day"}}
		if err := r.c.getJSON(ctx, get(queryAPI, "engage/revenue", params), &resp); err != nil {
			return fmt.Errorf("%s to %s: %w", w.fromDate(), w.toDate(), err)
		}
		for _, day := range sortedDays(resp.Results) {
			d := resp.Results[day]
			rec := map[string]any{"date": day, "amount": number(d.Amount), "count": number(d.Count), "paid_count": number(d.PaidCount)}
			if err := r.em.Record(desc, rec); err != nil {
				return err
			}
			st.Date = max(st.Date, day)
		}
		if inc && st.Date != "" {
			if err := r.em.StreamState(desc, st); err != nil {
				return err
			}
		}
	}
	return nil
}

// number returns n as a JSON number, nil when it is absent.
func number(n json.Number) any {
	if n == "" {
		return nil
	}
	return n
}
