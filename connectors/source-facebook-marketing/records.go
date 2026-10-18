package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// graphTimeLayout is how the Graph API writes times, such as
// 2026-10-01T08:30:00+0000.
const graphTimeLayout = "2006-01-02T15:04:05-0700"

// record returns the schema fields of obj converted to their schema types.
// The Graph API sends numbers as strings; they become JSON numbers. A field
// that is missing, null or not convertible is left out.
func record(fs []field, obj map[string]any) map[string]any {
	rec := make(map[string]any, len(fs))
	for _, f := range fs {
		if v, ok := convert(f, obj[f.name]); ok {
			rec[f.name] = v
		}
	}
	return rec
}

func convert(f field, v any) (any, bool) {
	if v == nil {
		return nil, false
	}
	switch f.typ {
	case synclet.String:
		var s string
		switch t := v.(type) {
		case string:
			s = t
		case json.Number:
			s = t.String()
		case bool:
			s = strconv.FormatBool(t)
		default:
			return nil, false
		}
		if f.format == synclet.DateTime {
			if ts, err := time.Parse(graphTimeLayout, s); err == nil {
				s = ts.UTC().Format(time.RFC3339)
			}
		}
		return s, true
	case synclet.Number, synclet.Integer:
		s, ok := numberText(v)
		if !ok {
			return nil, false
		}
		if f.typ == synclet.Integer {
			if _, err := strconv.ParseInt(s, 10, 64); err != nil {
				return nil, false
			}
		} else if _, err := strconv.ParseFloat(s, 64); err != nil {
			return nil, false
		}
		return json.Number(s), true
	case synclet.Array:
		a, ok := v.([]any)
		return a, ok
	case boolean:
		b, ok := v.(bool)
		return b, ok
	}
	return nil, false
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

// parseAmount reads a formatted money amount such as "1,234.50", "1234,50",
// "1.234,50" or "1 234,50" as a decimal number. With both separators the
// last one is the decimal point; a lone comma followed by exactly three
// digits, or a separator that repeats, groups thousands, and every group
// after the first must then have three digits.
func parseAmount(s string) (json.Number, bool) {
	s = strings.NewReplacer(" ", "", "\u00a0", "").Replace(strings.TrimSpace(s))
	comma, dot := strings.LastIndex(s, ","), strings.LastIndex(s, ".")
	var group, decimal string
	switch {
	case comma >= 0 && dot >= 0 && comma > dot:
		group, decimal = ".", ","
	case comma >= 0 && dot >= 0:
		group, decimal = ",", "."
	case comma >= 0 && (strings.Count(s, ",") > 1 || len(s)-comma-1 == 3):
		group = ","
	case comma >= 0:
		decimal = ","
	case strings.Count(s, ".") > 1:
		group = "."
	}
	whole, frac, hasFrac := s, "", false
	if decimal != "" {
		whole, frac, hasFrac = strings.Cut(s, decimal)
	}
	if group != "" && strings.Contains(whole, group) {
		parts := strings.Split(whole, group)
		for i, p := range parts {
			if p == "" || len(p) > 3 || (i > 0 && len(p) != 3) {
				return "", false
			}
		}
		whole = strings.Join(parts, "")
	}
	s = whole
	if hasFrac {
		s += "." + frac
	}
	if _, err := strconv.ParseFloat(s, 64); err != nil || s == "" {
		return "", false
	}
	return json.Number(s), true
}
