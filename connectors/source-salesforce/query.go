package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	// defaultMaxSelectLen is the longest URL-encoded SOQL query sent as one
	// request. Salesforce rejects a REST URI longer than 16 384 characters;
	// the rest is left for the host, the path and the q= prefix.
	defaultMaxSelectLen = 14000
	// soqlTimeLayout is a SOQL dateTime literal in UTC, and the form every
	// datetime value and saved cursor takes.
	soqlTimeLayout = "2006-01-02T15:04:05.000Z07:00"
	// apiTimeLayout is how the REST API writes datetime values.
	apiTimeLayout = "2006-01-02T15:04:05.000-0700"
)

// idPattern matches a 15 or 18 character Salesforce record id.
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$`)

// position is where an incremental read continues: after the record with
// cursor value Cursor and id ID, in the order of cursor, then Id.
type position struct {
	Cursor string `json:"cursor"`
	ID     string `json:"id"`
}

// soql is the query of one stream. cursor is empty for an object without a
// cursor field; after is nil unless an incremental read continues from a
// saved position; ordered sorts by cursor, then Id.
type soql struct {
	object  string
	cursor  string
	start   time.Time
	after   *position
	ordered bool
}

// build returns the query selecting fields.
func (q soql) build(fields []string) string {
	var b strings.Builder
	b.WriteString("SELECT " + strings.Join(fields, ", ") + " FROM " + q.object)
	var conds []string
	if q.after != nil {
		t := q.after.Cursor
		conds = append(conds, fmt.Sprintf("(%[1]s > %[2]s OR (%[1]s = %[2]s AND Id > '%[3]s'))", q.cursor, t, q.after.ID))
	}
	if q.cursor != "" && !q.start.IsZero() {
		conds = append(conds, q.cursor+" >= "+soqlTime(q.start))
	}
	if len(conds) > 0 {
		b.WriteString(" WHERE " + strings.Join(conds, " AND "))
	}
	if q.ordered {
		b.WriteString(" ORDER BY " + q.cursor + ", Id")
	}
	return b.String()
}

// byIDs returns the query selecting fields after Id of the records ids.
func byIDs(object string, fields, ids []string) string {
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = "'" + id + "'"
	}
	return "SELECT Id, " + strings.Join(fields, ", ") + " FROM " + object + " WHERE Id IN (" + strings.Join(quoted, ", ") + ")"
}

func soqlTime(t time.Time) string { return t.UTC().Format(soqlTimeLayout) }

// parsePosition checks a saved position, which goes into a query as
// literals, and returns it with its cursor in SOQL form.
func parsePosition(p position) (position, error) {
	t, err := time.Parse(time.RFC3339Nano, p.Cursor)
	if err != nil {
		return position{}, fmt.Errorf("cursor %q is not an RFC 3339 time", p.Cursor)
	}
	if !idPattern.MatchString(p.ID) {
		return position{}, fmt.Errorf("id %q is not a Salesforce record id", p.ID)
	}
	return position{Cursor: soqlTime(t), ID: p.ID}, nil
}

// normalizeTime rewrites a datetime value of the REST API, such as
// 2026-10-01T12:00:00.000+0000, as RFC 3339 in UTC with milliseconds.
func normalizeTime(v string) (string, error) {
	t, err := time.Parse(apiTimeLayout, v)
	if err != nil {
		if t, err = time.Parse(time.RFC3339Nano, v); err != nil {
			return "", fmt.Errorf("datetime %q is in neither the API's form nor RFC 3339", v)
		}
	}
	return soqlTime(t), nil
}

// pack splits items into consecutive groups whose URL-encoded length,
// joined by ", ", is at most budget. An item longer than budget alone fails.
func pack(items []string, budget int) ([][]string, error) {
	sep := len(encodeQuery(", "))
	var groups [][]string
	var cur []string
	size := 0
	for _, it := range items {
		n := len(encodeQuery(it))
		if n > budget {
			return nil, fmt.Errorf("%s does not fit into a query of at most %d encoded characters", it, budget)
		}
		if len(cur) > 0 && size+sep+n > budget {
			groups, cur, size = append(groups, cur), nil, 0
		}
		if len(cur) > 0 {
			size += sep
		}
		cur, size = append(cur, it), size+n
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups, nil
}
