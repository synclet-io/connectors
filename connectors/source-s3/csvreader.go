package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// cell is one field of a delimited record. Quoted tells a quoted empty
// string from an unquoted empty field, which may stand for null.
type cell struct {
	text   string
	quoted bool
}

// csvReader reads delimited records. A quoted field may hold the delimiter
// and line breaks; inside it a doubled quote, or the escape character
// followed by any character, stands for that character. Outside quotes the
// escape character makes the next character literal. With no quote
// character every field is unquoted. Empty lines are skipped.
type csvReader struct {
	r      *bufio.Reader
	delim  rune
	quote  rune // 0: no quoting
	escape rune // 0: no escape character
	line   int  // lines consumed so far
}

func newCSVReader(r io.Reader, delim, quote, escape string) *csvReader {
	c := &csvReader{r: bufio.NewReaderSize(r, 64*1024)}
	c.delim = []rune(delim)[0]
	if quote != "" {
		c.quote = []rune(quote)[0]
	}
	if escape != "" {
		c.escape = []rune(escape)[0]
	}
	return c
}

// csvSyntaxError is a record that cannot be parsed; the reader has skipped
// to the next line and can go on.
type csvSyntaxError struct {
	line int
	msg  string
}

func (e *csvSyntaxError) Error() string { return fmt.Sprintf("line %d: %s", e.line, e.msg) }

// skipLines discards n physical lines.
func (c *csvReader) skipLines(n int) error {
	for range n {
		if _, err := c.r.ReadString('\n'); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		c.line++
	}
	return nil
}

// Read returns the next record, io.EOF after the last one, or a
// *csvSyntaxError for a record it skipped.
func (c *csvReader) Read() ([]cell, error) {
	for {
		rec, err := c.read()
		if err != nil {
			return nil, err
		}
		if len(rec) == 1 && rec[0].text == "" && !rec[0].quoted {
			continue
		}
		return rec, nil
	}
}

func (c *csvReader) next() (rune, error) {
	r, _, err := c.r.ReadRune()
	return r, err
}

func (c *csvReader) read() ([]cell, error) {
	start := c.line + 1
	if _, err := c.r.Peek(1); err != nil {
		return nil, err
	}
	var rec []cell
	var b strings.Builder
	for {
		b.Reset()
		quoted := false
		r, err := c.next()
		if c.quote != 0 && err == nil && r == c.quote {
			quoted = true
			if err := c.readQuoted(&b, start); err != nil {
				return nil, err
			}
			r, err = c.next()
		} else {
			for err == nil && r != c.delim && r != '\n' {
				if c.escape != 0 && r == c.escape {
					r, err = c.next()
					if err != nil {
						break
					}
				}
				b.WriteRune(r)
				r, err = c.next()
			}
		}
		if err != nil && err != io.EOF {
			return nil, err
		}
		switch {
		case err == io.EOF:
			c.line++
			text := b.String()
			if !quoted {
				text = strings.TrimSuffix(text, "\r")
			}
			return append(rec, cell{text: text, quoted: quoted}), nil
		case r == c.delim:
			rec = append(rec, cell{text: b.String(), quoted: quoted})
		case r == '\n':
			c.line++
			text := b.String()
			if !quoted {
				text = strings.TrimSuffix(text, "\r")
			}
			return append(rec, cell{text: text, quoted: quoted}), nil
		case quoted && r == '\r':
			if r2, err := c.next(); err == io.EOF || (err == nil && r2 == '\n') {
				c.line++
				return append(rec, cell{text: b.String(), quoted: true}), nil
			}
			return nil, c.syntaxError(start, "unexpected character after a closing quote")
		default:
			return nil, c.syntaxError(start, fmt.Sprintf("unexpected %q after a closing quote", r))
		}
	}
}

// readQuoted reads a quoted field up to and including its closing quote.
func (c *csvReader) readQuoted(b *strings.Builder, start int) error {
	for {
		r, err := c.next()
		if err == io.EOF {
			return &csvSyntaxError{line: start, msg: "quoted field is not closed before the end of the file"}
		}
		if err != nil {
			return err
		}
		switch {
		case c.escape != 0 && c.escape != c.quote && r == c.escape:
			r, err = c.next()
			if err == io.EOF {
				return &csvSyntaxError{line: start, msg: "escape character at the end of the file"}
			}
			if err != nil {
				return err
			}
			if r == '\n' {
				c.line++
			}
			b.WriteRune(r)
		case r == c.quote:
			r2, err := c.next()
			if err == nil && r2 == c.quote {
				b.WriteRune(c.quote)
				continue
			}
			if err == nil {
				if uerr := c.r.UnreadRune(); uerr != nil {
					return uerr
				}
			} else if err != io.EOF {
				return err
			}
			return nil
		default:
			if r == '\n' {
				c.line++
			}
			b.WriteRune(r)
		}
	}
}

// syntaxError skips the rest of the current line and reports start.
func (c *csvReader) syntaxError(start int, msg string) error {
	if _, err := c.r.ReadString('\n'); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	c.line++
	return &csvSyntaxError{line: start, msg: msg}
}
