package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	tokenNamespace = "namespace"
	tokenStream    = "stream"
	tokenYear      = "yyyy"
	tokenMonth     = "mm"
	tokenDay       = "dd"
	tokenHour      = "hh"
	tokenPart      = "part"
	tokenField     = "field:"

	nullPartition  = "__null__"
	emptyPartition = "__empty__"
)

// templatePart is a literal or a {token} of a path template segment.
type templatePart struct {
	literal string
	token   string // "" for a literal; a field token keeps its "field:" prefix
}

// pathTemplate is a parsed path template: segments separated by "/".
// streamSegment is the index of the segment holding {stream}; the segments
// up to and including it render the stream prefix.
type pathTemplate struct {
	segments      [][]templatePart
	streamSegment int
	fields        []string
}

// parsePathTemplate parses and validates a path template: {stream} is
// required in a directory segment that, like every segment before it, holds
// only literals, {namespace} and {stream}; {part} is required in the last
// segment.
func parsePathTemplate(s string) (*pathTemplate, error) {
	s = strings.Trim(s, "/")
	if s == "" {
		return nil, errors.New("template is empty")
	}
	t := &pathTemplate{streamSegment: -1}
	hasPart := false
	rawSegments := strings.Split(s, "/")
	for i, raw := range rawSegments {
		parts, err := parseSegment(raw)
		if err != nil {
			return nil, err
		}
		for _, p := range parts {
			switch {
			case p.token == tokenStream:
				if t.streamSegment >= 0 {
					return nil, errors.New("{stream} may appear only once")
				}
				t.streamSegment = i
			case p.token == tokenPart:
				if i != len(rawSegments)-1 {
					return nil, errors.New("{part} must be in the last segment")
				}
				if hasPart {
					return nil, errors.New("{part} may appear only once")
				}
				hasPart = true
			case strings.HasPrefix(p.token, tokenField):
				t.fields = append(t.fields, strings.TrimPrefix(p.token, tokenField))
			}
		}
		t.segments = append(t.segments, parts)
	}
	if t.streamSegment < 0 {
		return nil, errors.New("{stream} is required")
	}
	if t.streamSegment == len(t.segments)-1 {
		return nil, errors.New("{stream} must be in a directory segment, not in the file name segment")
	}
	if !hasPart {
		return nil, errors.New("{part} is required in the last segment")
	}
	for i := 0; i <= t.streamSegment; i++ {
		for _, p := range t.segments[i] {
			if p.token != "" && p.token != tokenNamespace && p.token != tokenStream {
				return nil, fmt.Errorf("only {namespace} and {stream} may come before the end of the {stream} segment, found {%s}", p.token)
			}
		}
	}
	return t, nil
}

func parseSegment(raw string) ([]templatePart, error) {
	var parts []templatePart
	for raw != "" {
		open := strings.IndexByte(raw, '{')
		if open < 0 {
			if strings.ContainsRune(raw, '}') {
				return nil, fmt.Errorf("unmatched } in %q", raw)
			}
			parts = append(parts, templatePart{literal: raw})
			break
		}
		if open > 0 {
			lit := raw[:open]
			if strings.ContainsRune(lit, '}') {
				return nil, fmt.Errorf("unmatched } in %q", raw)
			}
			parts = append(parts, templatePart{literal: lit})
		}
		end := strings.IndexByte(raw[open:], '}')
		if end < 0 {
			return nil, fmt.Errorf("unclosed { in %q", raw)
		}
		token := raw[open+1 : open+end]
		if err := validateToken(token); err != nil {
			return nil, err
		}
		parts = append(parts, templatePart{token: token})
		raw = raw[open+end+1:]
	}
	return parts, nil
}

func validateToken(token string) error {
	switch token {
	case tokenNamespace, tokenStream, tokenYear, tokenMonth, tokenDay, tokenHour, tokenPart:
		return nil
	}
	if name, ok := strings.CutPrefix(token, tokenField); ok {
		if name == "" || strings.ContainsAny(name, "{}") {
			return fmt.Errorf("invalid field token {%s}", token)
		}
		return nil
	}
	return fmt.Errorf("unknown token {%s}", token)
}

// renderContext holds what a rendered key depends on besides record fields.
type renderContext struct {
	namespace string
	stream    string
	start     time.Time
}

// streamPrefix renders the stream's directory: the segments up to and
// including the {stream} segment, ending with "/".
func (t *pathTemplate) streamPrefix(rc renderContext) string {
	var segs []string
	for i := 0; i <= t.streamSegment; i++ {
		s, _ := renderSegment(t.segments[i], rc, nil, "")
		if s != "" {
			segs = append(segs, s)
		}
	}
	return strings.Join(segs, "/") + "/"
}

// render returns the key of a file below the stream prefix's parent with
// part as {part}; segments that render empty are dropped.
func (t *pathTemplate) render(rc renderContext, fields map[string]json.RawMessage, part string) (string, error) {
	segs := make([]string, 0, len(t.segments))
	for _, seg := range t.segments {
		s, err := renderSegment(seg, rc, fields, part)
		if err != nil {
			return "", err
		}
		if s != "" {
			segs = append(segs, s)
		}
	}
	return strings.Join(segs, "/"), nil
}

func renderSegment(parts []templatePart, rc renderContext, fields map[string]json.RawMessage, part string) (string, error) {
	var b strings.Builder
	start := rc.start.UTC()
	for _, p := range parts {
		switch p.token {
		case "":
			b.WriteString(p.literal)
		case tokenNamespace:
			b.WriteString(escapeSegment(rc.namespace))
		case tokenStream:
			b.WriteString(escapeSegment(rc.stream))
		case tokenYear:
			fmt.Fprintf(&b, "%04d", start.Year())
		case tokenMonth:
			fmt.Fprintf(&b, "%02d", int(start.Month()))
		case tokenDay:
			fmt.Fprintf(&b, "%02d", start.Day())
		case tokenHour:
			fmt.Fprintf(&b, "%02d", start.Hour())
		case tokenPart:
			b.WriteString(part)
		default:
			name := strings.TrimPrefix(p.token, tokenField)
			v, err := partitionValue(fields[name])
			if err != nil {
				return "", fmt.Errorf("partition field %q: %w", name, err)
			}
			b.WriteString(v)
		}
	}
	return b.String(), nil
}

// partitionValue renders a top-level record value as one path segment.
func partitionValue(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nullPartition, nil
	}
	switch raw[0] {
	case '{', '[':
		return "", fmt.Errorf("value %s is an object or array and cannot be a path segment", raw)
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("value %s: %w", raw, err)
		}
		if s == "" {
			return emptyPartition, nil
		}
		return escapeSegment(s), nil
	default:
		return escapeSegment(string(raw)), nil
	}
}

// escapeSegment path-escapes s so it stays one segment; "." and ".." are
// escaped too, so a value never navigates the key.
func escapeSegment(s string) string {
	switch s {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	}
	return url.PathEscape(s)
}
