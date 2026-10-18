package main

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// dateField is the segment that makes a query daily and its stream
// incremental.
const dateField = "segments.date"

var fieldPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

var resourcePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// gaqlQuery is a parsed GAQL query: its text, the fields of its SELECT
// clause, its FROM resource and where its trailing clauses start.
type gaqlQuery struct {
	text     string
	fields   []string
	resource string
	// where is the index of the WHERE keyword, or -1.
	where int
	// tail is the index of the first ORDER BY, LIMIT or PARAMETERS keyword
	// after FROM, or len(text).
	tail int
}

// clauseKeywords are the GAQL keywords that start a clause.
var clauseKeywords = []string{"SELECT", "FROM", "WHERE", "ORDER BY", "LIMIT", "PARAMETERS"}

// keywordIndexes returns the index of the first occurrence of each clause
// keyword outside string literals, matched case-insensitively at word
// boundaries.
func keywordIndexes(q string) map[string]int {
	idx := map[string]int{}
	var quote byte
	for i := 0; i < len(q); i++ {
		c := q[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if i > 0 && isWordByte(q[i-1]) {
			continue
		}
		for _, kw := range clauseKeywords {
			if _, seen := idx[kw]; seen {
				continue
			}
			if n := matchKeyword(q[i:], kw); n > 0 {
				idx[kw] = i
				i += n - 1
				break
			}
		}
	}
	return idx
}

// matchKeyword returns the length of kw at the start of s, with any run of
// whitespace between its words, or 0 when s does not start with kw followed
// by a word boundary.
func matchKeyword(s, kw string) int {
	n := 0
	for i, word := range strings.Fields(kw) {
		if i > 0 {
			ws := len(s[n:]) - len(strings.TrimLeft(s[n:], " \t\r\n"))
			if ws == 0 {
				return 0
			}
			n += ws
		}
		if len(s[n:]) < len(word) || !strings.EqualFold(s[n:n+len(word)], word) {
			return 0
		}
		n += len(word)
	}
	if n < len(s) && isWordByte(s[n]) {
		return 0
	}
	return n
}

func isWordByte(c byte) bool {
	return c == '_' || c == '.' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// parseQuery reads the SELECT fields and FROM resource of a GAQL query.
func parseQuery(text string) (gaqlQuery, error) {
	text = strings.TrimSpace(text)
	idx := keywordIndexes(text)
	if sel, ok := idx["SELECT"]; !ok || sel != 0 {
		return gaqlQuery{}, errors.New("query must start with SELECT")
	}
	from, ok := idx["FROM"]
	if !ok {
		return gaqlQuery{}, errors.New("query has no FROM clause")
	}
	q := gaqlQuery{text: text, where: -1, tail: len(text)}
	for f := range strings.SplitSeq(text[len("SELECT"):from], ",") {
		f = strings.TrimSpace(f)
		if !fieldPattern.MatchString(f) {
			return gaqlQuery{}, fmt.Errorf("%q is not a field name such as campaign.id", f)
		}
		if !slices.Contains(q.fields, f) {
			q.fields = append(q.fields, f)
		}
	}
	words := strings.Fields(text[from+len("FROM"):])
	if len(words) == 0 || !resourcePattern.MatchString(words[0]) {
		return gaqlQuery{}, errors.New("FROM names no resource")
	}
	q.resource = words[0]
	if w, ok := idx["WHERE"]; ok && w > from {
		q.where = w
	}
	for _, kw := range []string{"ORDER BY", "LIMIT", "PARAMETERS"} {
		if i, ok := idx[kw]; ok && i > from && i < q.tail {
			q.tail = i
		}
	}
	if q.where > q.tail {
		return gaqlQuery{}, errors.New("WHERE must come before ORDER BY, LIMIT and PARAMETERS")
	}
	return q, nil
}

// selects reports whether the query selects field.
func (q gaqlQuery) selects(field string) bool { return slices.Contains(q.fields, field) }

// withDateRange returns the query with segments.date limited to since..until,
// AND-ed into its WHERE clause, or as its WHERE clause, before ORDER BY,
// LIMIT and PARAMETERS.
func (q gaqlQuery) withDateRange(since, until string) string {
	cond := fmt.Sprintf("%s BETWEEN '%s' AND '%s'", dateField, since, until)
	head := strings.TrimSpace(q.text[:q.tail])
	if q.where >= 0 {
		head += " AND " + cond
	} else {
		head += " WHERE " + cond
	}
	if rest := strings.TrimSpace(q.text[q.tail:]); rest != "" {
		head += " " + rest
	}
	return head
}
