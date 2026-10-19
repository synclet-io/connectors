package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRestliEscape(t *testing.T) {
	assert.Equal(t, "urn%3Ali%3AsponsoredAccount%3A1001", restliEscape("urn:li:sponsoredAccount:1001"))
	assert.Equal(t, "a%20b%2Cc%28d%29%27e", restliEscape("a b,c(d)'e"), "structure characters inside a value are escaped")
	assert.Equal(t, "Az09-_.~", restliEscape("Az09-_.~"))
}

func TestRestliList(t *testing.T) {
	assert.Equal(t, "List(urn%3Ali%3AsponsoredAccount%3A1001,urn%3Ali%3AsponsoredAccount%3A1002)",
		restliList("urn:li:sponsoredAccount:1001", "urn:li:sponsoredAccount:1002"))
	assert.Equal(t, "List()", restliList())
}

func TestRestliDateRange(t *testing.T) {
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, "(year:2026,month:8,day:1)", restliDate(start))
	assert.Equal(t, "(start:(year:2026,month:8,day:1),end:(year:2026,month:8,day:30))", restliDateRange(start, end))
}

func TestQueryEncode(t *testing.T) {
	q := query{
		{key: "q", value: "analytics"},
		{key: "accounts", value: restliList("urn:li:sponsoredAccount:1001"), raw: true},
		{key: "pageToken", value: "a b&c=d"},
	}
	assert.Equal(t, "q=analytics&accounts=List(urn%3Ali%3AsponsoredAccount%3A1001)&pageToken=a%20b%26c%3Dd", q.encode())
	assert.Empty(t, query{}.encode())
}
