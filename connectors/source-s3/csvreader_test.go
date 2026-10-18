package main

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func readAllCells(t *testing.T, r *csvReader) ([][]cell, []error) {
	t.Helper()
	var recs [][]cell
	var errs []error
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			return recs, errs
		}
		if err != nil {
			var se *csvSyntaxError
			require.ErrorAs(t, err, &se)
			errs = append(errs, err)
			continue
		}
		recs = append(recs, rec)
	}
}

func TestCSVReader(t *testing.T) {
	q := func(s string) cell { return cell{text: s, quoted: true} }
	u := func(s string) cell { return cell{text: s} }
	cases := []struct {
		name, in             string
		delim, quote, escape string
		want                 [][]cell
		wantErrs             []string
	}{
		{
			name: "quoted newline, doubled quote, empty vs null", in: "a,\"b\nc\",\"x\"\"y\"\n\"\",,\r\n",
			delim: ",", quote: `"`,
			want: [][]cell{{u("a"), q("b\nc"), q(`x"y`)}, {q(""), u(""), u("")}},
		},
		{
			name: "CRLF inside and after quotes", in: "\"a\r\nb\"\r\nc\r\n",
			delim: ",", quote: `"`,
			want: [][]cell{{q("a\r\nb")}, {u("c")}},
		},
		{
			name: "no final newline, blank lines skipped", in: "\n\na;b\n\nc;d",
			delim: ";", quote: `"`,
			want: [][]cell{{u("a"), u("b")}, {u("c"), u("d")}},
		},
		{
			name: "escape character inside and outside quotes", in: `'a\'b',c\,d` + "\n",
			delim: ",", quote: "'", escape: `\`,
			want: [][]cell{{q("a'b"), u("c,d")}},
		},
		{
			name: "no quoting keeps quotes", in: "\"a\"\t\"b\n",
			delim: "\t",
			want:  [][]cell{{u(`"a"`), u(`"b`)}},
		},
		{
			name: "text after a closing quote", in: "\"a\"b,c\nd,e\n",
			delim: ",", quote: `"`,
			want:     [][]cell{{u("d"), u("e")}},
			wantErrs: []string{`line 1: unexpected 'b' after a closing quote`},
		},
		{
			name: "unclosed quote", in: "a,b\n\"c,d\n",
			delim: ",", quote: `"`,
			want:     [][]cell{{u("a"), u("b")}},
			wantErrs: []string{"line 2: quoted field is not closed before the end of the file"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			recs, errs := readAllCells(t, newCSVReader(strings.NewReader(c.in), c.delim, c.quote, c.escape))
			require.Equal(t, c.want, recs)
			var msgs []string
			for _, e := range errs {
				msgs = append(msgs, e.Error())
			}
			require.Equal(t, c.wantErrs, msgs)
		})
	}
}

func TestCSVReaderSkipLines(t *testing.T) {
	r := newCSVReader(strings.NewReader("junk \"\nmore\nh\nv\n"), ",", `"`, "")
	require.NoError(t, r.skipLines(2))
	recs, errs := readAllCells(t, r)
	require.Empty(t, errs)
	require.Equal(t, [][]cell{{{text: "h"}}, {{text: "v"}}}, recs)
}
