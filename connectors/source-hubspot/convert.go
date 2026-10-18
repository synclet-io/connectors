package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema type the SDK has no constant for.
const boolean synclet.PropType = "boolean"

// propKind is how the string value of a HubSpot property becomes a JSON
// value.
type propKind int

const (
	kindString propKind = iota
	kindNumber
	kindBool
	kindDateTime
	kindDate
)

// kindOf maps the type of the HubSpot properties API to a kind. string,
// phone_number, enumeration, json, object_coordinates and any type HubSpot
// adds later stay strings.
func kindOf(hubspotType string) propKind {
	switch hubspotType {
	case "number":
		return kindNumber
	case "bool":
		return kindBool
	case "datetime":
		return kindDateTime
	case "date":
		return kindDate
	}
	return kindString
}

func nullable(t synclet.PropType) synclet.PropertySpec {
	return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Null, t}}}
}

func (k propKind) spec() synclet.PropertySpec {
	switch k {
	case kindNumber:
		return nullable(synclet.Number)
	case kindBool:
		return nullable(boolean)
	case kindDateTime:
		p := nullable(synclet.String)
		p.Format = synclet.DateTime
		return p
	case kindDate:
		p := nullable(synclet.String)
		p.Format = synclet.Date
		return p
	}
	return nullable(synclet.String)
}

// schemaField is a property of a stream's JSON schema as the configured
// catalog carries it.
type schemaField struct {
	Type   synclet.PropTypes  `json:"type"`
	Format synclet.FormatType `json:"format"`
}

// kindOfField is the inverse of spec: the kind a discovered property was
// declared with.
func kindOfField(f schemaField) propKind {
	switch {
	case slices.Contains(f.Type, synclet.Number):
		return kindNumber
	case slices.Contains(f.Type, boolean):
		return kindBool
	case f.Format == synclet.DateTime:
		return kindDateTime
	case f.Format == synclet.Date:
		return kindDate
	}
	return kindString
}

// convert turns a property value of the HubSpot API, a JSON string or
// null, into the JSON value of its kind. "" is null for every kind but
// string. A value that does not parse as its kind is an error.
func convert(k propKind, raw json.RawMessage) (any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("value %s is not a string", raw)
	}
	if k == kindString {
		return s, nil
	}
	if s == "" {
		return nil, nil
	}
	switch k {
	case kindNumber:
		var n json.Number
		if json.Unmarshal([]byte(s), &n) != nil || !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("value %q is not a number", s)
		}
		return json.RawMessage(s), nil
	case kindBool:
		b, err := strconv.ParseBool(s)
		if err != nil || (s != "true" && s != "false") {
			return nil, fmt.Errorf("value %q is not a boolean", s)
		}
		return b, nil
	case kindDateTime:
		t, err := parseTime(s)
		if err != nil {
			return nil, fmt.Errorf("value %q is not a date-time", s)
		}
		return t.Format(time.RFC3339Nano), nil
	case kindDate:
		return toDate(s)
	}
	return s, nil
}

// parseTime reads an RFC 3339 time or Unix milliseconds.
func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

// toDate reads a date property: YYYY-MM-DD, or a datetime or Unix
// milliseconds at midnight UTC, which is how HubSpot stores dates.
func toDate(s string) (string, error) {
	if _, err := time.Parse(dateLayout, s); err == nil {
		return s, nil
	}
	t, err := parseTime(s)
	if err != nil || !t.Equal(t.Truncate(24*time.Hour)) {
		return "", fmt.Errorf("value %q is not a date", s)
	}
	return t.Format(dateLayout), nil
}

// propertyColumn is the record field of HubSpot property name.
func propertyColumn(name string) synclet.PropertyName {
	return synclet.PropertyName("properties_" + name)
}

// propertyName is the HubSpot property of a record field, if it is one.
func propertyName(column string) (string, bool) {
	return strings.CutPrefix(column, "properties_")
}
