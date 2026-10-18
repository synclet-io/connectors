package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func readAdvertisers(ctx context.Context, c *apiClient, cfg Config, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	for batch := range slices.Chunk(cfg.advertisers(), advertiserBatch) {
		err := c.advertiserInfo(ctx, batch, d.requestFields(), func(obj map[string]any) error {
			return em.Record(desc, record(d.fields, obj))
		})
		if err != nil {
			return fmt.Errorf("%s: %w", d.name, err)
		}
	}
	return nil
}

func readEntity(ctx context.Context, c *apiClient, cfg Config, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	for _, adv := range cfg.advertisers() {
		q := url.Values{"advertiser_id": {adv}, "fields": {jsonList(d.requestFields()...)}}
		err := c.list(ctx, d.path, q, func(obj map[string]any) error {
			return em.Record(desc, record(d.fields, obj))
		})
		if err != nil {
			return fmt.Errorf("%s: advertiser %s: %w", d.name, adv, err)
		}
	}
	return nil
}

// record returns the schema fields of obj converted to their schema types.
// A number sent as a string becomes a JSON number. A field that is missing
// or null is left out; a nullable field whose value does not convert is null.
func record(fs []field, obj map[string]any) map[string]any {
	rec := make(map[string]any, len(fs))
	for _, f := range fs {
		v, ok := convert(f, obj[f.name])
		switch {
		case ok:
			rec[f.name] = v
		case slices.Contains(f.typ, synclet.Null) && obj[f.name] != nil:
			rec[f.name] = nil
		}
	}
	return rec
}

func convert(f field, v any) (any, bool) {
	if v == nil {
		return nil, false
	}
	for _, typ := range f.typ {
		switch typ {
		case synclet.String:
			return stringValue(v)
		case synclet.Number, synclet.Integer:
			s, ok := numberText(v)
			if !ok {
				return nil, false
			}
			if typ == synclet.Integer {
				if _, err := strconv.ParseInt(s, 10, 64); err != nil {
					return nil, false
				}
			} else if _, err := strconv.ParseFloat(s, 64); err != nil {
				return nil, false
			}
			return json.Number(s), true
		}
	}
	return nil, false
}

func stringValue(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case bool:
		return strconv.FormatBool(t), true
	}
	return "", false
}

func numberText(v any) (string, bool) {
	switch t := v.(type) {
	case json.Number:
		return t.String(), true
	case string:
		return strings.TrimSpace(t), true
	}
	return "", false
}
