package main

import (
	"fmt"
	"strings"
	"time"
)

// restliEscape percent-encodes a leaf value of a Rest.li 2.0 query: every
// byte but the unreserved characters of RFC 3986, so ( ) , : ' and spaces
// inside a value cannot end the structure around it.
func restliEscape(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}

// restliList encodes items as a Rest.li 2.0 List, escaping each item.
func restliList(items ...string) string {
	escaped := make([]string, len(items))
	for i, it := range items {
		escaped[i] = restliEscape(it)
	}
	return "List(" + strings.Join(escaped, ",") + ")"
}

// restliDate encodes the day of d as a Rest.li Date record.
func restliDate(d time.Time) string {
	return fmt.Sprintf("(year:%d,month:%d,day:%d)", d.Year(), int(d.Month()), d.Day())
}

// restliDateRange encodes the days start to end, both included, as a Rest.li
// DateRange record.
func restliDateRange(start, end time.Time) string {
	return "(start:" + restliDate(start) + ",end:" + restliDate(end) + ")"
}

// param is one query parameter. A raw value is already Rest.li encoded and is
// sent as it is; any other value is escaped.
type param struct {
	key, value string
	raw        bool
}

// query is an ordered query string.
type query []param

func (q query) encode() string {
	parts := make([]string, len(q))
	for i, p := range q {
		v := p.value
		if !p.raw {
			v = restliEscape(v)
		}
		parts[i] = restliEscape(p.key) + "=" + v
	}
	return strings.Join(parts, "&")
}

// with returns q with key set to value, replacing an earlier value of key.
func (q query) with(key, value string) query {
	out := make(query, 0, len(q)+1)
	for _, p := range q {
		if p.key != key {
			out = append(out, p)
		}
	}
	return append(out, param{key: key, value: value})
}
