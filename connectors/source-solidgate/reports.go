package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// reportLimit is the most entities a reports API page holds.
const reportLimit = 2000

// report is a reports API path and how a page's body yields the API objects
// of a stream.
type report struct {
	path    string
	objects func(body map[string]json.RawMessage) ([]map[string]any, error)
}

// reportRequest is the body of a reports API request: one window of
// date_from (inclusive) to date_to (exclusive), in UTC.
type reportRequest struct {
	DateFrom         string `json:"date_from"`
	DateTo           string `json:"date_to"`
	Filter           string `json:"filter"`
	Limit            int    `json:"limit,omitempty"`
	NextPageIterator string `json:"next_page_iterator,omitempty"`
}

type pageMetadata struct {
	NextPageIterator string `json:"next_page_iterator"`
}

// dayState is the STREAM state of an incremental stream: per account, the
// last UTC day read to its end. The report API is asked for one day at a
// time, so a day that is read is done whether it had records or not.
type dayState struct {
	Accounts map[string]accountDay `json:"accounts"`
}

// accountDay is the saved progress of one account.
type accountDay struct {
	Day string `json:"day"`
}

// last returns the last day read, zero when nothing is saved.
func (a accountDay) last() (time.Time, error) {
	if a.Day == "" {
		return time.Time{}, nil
	}
	return time.Parse(dateLayout, a.Day)
}

func loadDayState(desc synclet.StreamDescriptor, state *synclet.InputState) (dayState, error) {
	st := dayState{Accounts: map[string]accountDay{}}
	raw := state.Stream(desc)
	if raw == nil {
		return st, nil
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
	}
	if st.Accounts == nil {
		st.Accounts = map[string]accountDay{}
	}
	for name, a := range st.Accounts {
		if _, err := a.last(); err != nil {
			return st, synclet.ConfigError(fmt.Errorf("state of %s: day of account %q is not a YYYY-MM-DD date", desc.Name, name))
		}
	}
	return st, nil
}

// windowReader emits the records of one account for the UTC day starting at
// day.
type windowReader func(ctx context.Context, c *apiClient, day time.Time) error

// readWindows reads one account a UTC day at a time, from start_date, or in an
// incremental read with a saved day from lookback_days before it but never
// before start_date, to today. An incremental read saves each day once all of
// its records are out, so a day that fails is read again by the next read.
func (s Source) readWindows(ctx context.Context, cfg Config, d streamDef, cs synclet.ConfiguredStream, st dayState, em *synclet.Emitter, read windowReader) error {
	desc := cs.Stream.Descriptor()
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	today := day(s.clock())
	for _, a := range cfg.Accounts {
		c := s.apiClient(a)
		from := cfg.start(s.clock())
		if incremental {
			last, err := st.Accounts[a.Name].last()
			if err != nil {
				return synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
			}
			if resume := last.AddDate(0, 0, -cfg.lookback()); !last.IsZero() && resume.After(from) {
				from = resume
			}
		}
		for w := from; !w.After(today); w = w.AddDate(0, 0, 1) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := read(ctx, c, w); err != nil {
				return fmt.Errorf("%s: account %q, day %s: %w", d.name, a.Name, w.Format(dateLayout), err)
			}
			if !incremental {
				continue
			}
			st.Accounts[a.Name] = accountDay{Day: w.Format(dateLayout)}
			if err := em.StreamState(desc, dayState{Accounts: maps.Clone(st.Accounts)}); err != nil {
				return err
			}
		}
	}
	return nil
}

// reportWindow reads every page of d's report for one day, following
// metadata.next_page_iterator until it is empty.
func reportWindow(d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) windowReader {
	return func(ctx context.Context, c *apiClient, w time.Time) error {
		req := reportRequest{
			DateFrom: w.Format(apiTimeLayout),
			DateTo:   w.AddDate(0, 0, 1).Format(apiTimeLayout),
			Filter:   "updated_at",
			Limit:    reportLimit,
		}
		for {
			var body map[string]json.RawMessage
			if err := c.postJSON(ctx, c.reportsURL, d.report.path, req, &body); err != nil {
				return err
			}
			objs, err := d.report.objects(body)
			if err != nil {
				return fmt.Errorf("POST %s: %w", d.report.path, err)
			}
			for _, obj := range objs {
				rec, err := record(d.fields, obj, c.account.Name)
				if err != nil {
					return err
				}
				if err := em.Record(desc, rec); err != nil {
					return err
				}
			}
			var meta pageMetadata
			if raw, ok := body["metadata"]; ok {
				if err := json.Unmarshal(raw, &meta); err != nil {
					return fmt.Errorf("POST %s: metadata: %w", d.report.path, err)
				}
			}
			if meta.NextPageIterator == "" {
				return nil
			}
			req.NextPageIterator = meta.NextPageIterator
		}
	}
}

func decodeObject(raw json.RawMessage) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// orderedValues returns the elements of a JSON array, or the values of a
// JSON object in the order the body lists them; Solidgate sends some lists
// as objects keyed by id.
func orderedValues(raw json.RawMessage) ([]json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	if raw[0] == '[' {
		var list []json.RawMessage
		err := json.Unmarshal(raw, &list)
		return list, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	var values []json.RawMessage
	for dec.More() {
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	if _, err := dec.Token(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return values, nil
}

func decodeAll(raws []json.RawMessage) ([]map[string]any, error) {
	objs := make([]map[string]any, 0, len(raws))
	for _, r := range raws {
		obj, err := decodeObject(r)
		if err != nil {
			return nil, err
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

// listObjects yields the objects listed under key.
func listObjects(key string) func(map[string]json.RawMessage) ([]map[string]any, error) {
	return func(body map[string]json.RawMessage) ([]map[string]any, error) {
		raws, err := orderedValues(body[key])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		return decodeAll(raws)
	}
}

var subscriptionObjects = listObjects("subscriptions")

// invoiceObjects yields the invoices of every subscription, each with the
// subscription's id in subscription_id and its orders as a list.
func invoiceObjects(body map[string]json.RawMessage) ([]map[string]any, error) {
	subs, err := orderedValues(body["subscriptions"])
	if err != nil {
		return nil, fmt.Errorf("subscriptions: %w", err)
	}
	var objs []map[string]any
	for _, raw := range subs {
		var sub struct {
			ID       string          `json:"id"`
			Invoices json.RawMessage `json:"invoices"`
		}
		if err := json.Unmarshal(raw, &sub); err != nil {
			return nil, fmt.Errorf("subscriptions: %w", err)
		}
		invoices, err := orderedValues(sub.Invoices)
		if err != nil {
			return nil, fmt.Errorf("subscription %s: invoices: %w", sub.ID, err)
		}
		for _, inv := range invoices {
			obj, err := decodeObject(inv)
			if err != nil {
				return nil, fmt.Errorf("subscription %s: invoices: %w", sub.ID, err)
			}
			var withOrders struct {
				Orders json.RawMessage `json:"orders"`
			}
			if err := json.Unmarshal(inv, &withOrders); err != nil {
				return nil, err
			}
			orders, err := orderedValues(withOrders.Orders)
			if err != nil {
				return nil, fmt.Errorf("subscription %s: invoice orders: %w", sub.ID, err)
			}
			list, err := decodeAll(orders)
			if err != nil {
				return nil, fmt.Errorf("subscription %s: invoice orders: %w", sub.ID, err)
			}
			if sub.ID != "" {
				obj["subscription_id"] = sub.ID
			}
			obj["orders"] = list
			objs = append(objs, obj)
		}
	}
	return objs, nil
}

// chargebackObjects yields the chargebacks of every order, each with the
// order's id in order_id.
func chargebackObjects(body map[string]json.RawMessage) ([]map[string]any, error) {
	orders, err := listObjects("orders")(body)
	if err != nil {
		return nil, err
	}
	var objs []map[string]any
	for _, o := range orders {
		list, _ := o["chargebacks"].([]any)
		for _, item := range list {
			cb, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("order %v: chargeback is not an object", o["order_id"])
			}
			cb["order_id"] = o["order_id"]
			objs = append(objs, cb)
		}
	}
	return objs, nil
}
