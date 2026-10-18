package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
)

const (
	decimalBytes = 16
	// rowGroupBytes is the record volume after which a row group is flushed,
	// bounding the memory an open Parquet file holds.
	rowGroupBytes = 16 << 20
)

var (
	decimalLimit = new(big.Int).Exp(big.NewInt(10), big.NewInt(maxDecimalPrecision), nil)
	twoTo128     = new(big.Int).Lsh(big.NewInt(1), 8*decimalBytes)

	timestampLayoutsTZ = []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
	}
	timestampLayoutsNTZ = []string{
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
	}
)

// parquetColumn is a declared property with its leaf column index.
type parquetColumn struct {
	column
	index int
}

type parquetWriter struct {
	layout       *streamLayout
	format       Format
	w            *parquet.GenericWriter[any]
	columns      []parquetColumn
	rawID        int
	extractedAt  int
	additional   int
	width        int
	n            int64
	sinceFlush   int64
	decimalScale int
}

func newParquetWriter(f Format, layout *streamLayout, out io.Writer) (*parquetWriter, error) {
	schema, err := parquetSchema(f, layout)
	if err != nil {
		return nil, err
	}
	codec, err := parquetCodec(f.Compression)
	if err != nil {
		return nil, err
	}
	pw := &parquetWriter{
		layout:       layout,
		format:       f,
		w:            parquet.NewGenericWriter[any](out, schema, parquet.Compression(codec)),
		width:        len(schema.Columns()),
		decimalScale: *f.DecimalScale,
	}
	index := func(name string) int {
		leaf, _ := schema.Lookup(name)
		return leaf.ColumnIndex
	}
	pw.rawID = index(colRawID)
	pw.extractedAt = index(colExtractedAt)
	pw.additional = index(colAdditional)
	for _, c := range layout.columns {
		pw.columns = append(pw.columns, parquetColumn{column: c, index: index(c.name)})
	}
	return pw, nil
}

func parquetCodec(name string) (compress.Codec, error) {
	switch name {
	case compressionSnappy:
		return &snappy.Codec{}, nil
	case compressionZstd:
		return &parquet.Zstd, nil
	case compressionGzip:
		return &parquet.Gzip, nil
	case compressionNone:
		return &uncompressed.Codec{}, nil
	default:
		return nil, fmt.Errorf("unsupported parquet compression %q", name)
	}
}

// parquetSchema maps the stream's columns to logical types: dates, times,
// timestamps and decimals are annotated leaves, never groups.
func parquetSchema(f Format, layout *streamLayout) (*parquet.Schema, error) {
	group := parquet.Group{
		colRawID:       parquet.String(),
		colExtractedAt: parquet.TimestampAdjusted(parquet.Millisecond, true),
		colAdditional:  parquet.Optional(parquet.JSON()),
	}
	for _, c := range layout.columns {
		group[c.name] = parquet.Optional(parquetNode(f, c.kind))
	}
	return parquet.NewSchema(layout.name, group), nil
}

func parquetNode(f Format, kind columnKind) parquet.Node {
	switch kind {
	case kindString, kindTimeTZ:
		return parquet.String()
	case kindBoolean:
		return parquet.Leaf(parquet.BooleanType)
	case kindInteger:
		return parquet.Int(64)
	case kindNumber:
		if f.ParquetNumberType == numberTypeString {
			return parquet.String()
		}
		return parquet.Decimal(*f.DecimalScale, maxDecimalPrecision, parquet.FixedLenByteArrayType(decimalBytes))
	case kindBigInteger:
		return parquet.Decimal(0, maxDecimalPrecision, parquet.FixedLenByteArrayType(decimalBytes))
	case kindDate:
		return parquet.Date()
	case kindTimestampTZ:
		return parquet.TimestampAdjusted(parquet.Microsecond, true)
	case kindTimestampNTZ:
		return parquet.TimestampAdjusted(parquet.Microsecond, false)
	case kindTimeNTZ:
		return parquet.TimeAdjusted(parquet.Microsecond, false)
	default:
		return parquet.JSON()
	}
}

func (w *parquetWriter) write(r *record) error {
	row := make(parquet.Row, w.width)
	row[w.rawID] = parquet.ByteArrayValue([]byte(r.id)).Level(0, 0, w.rawID)
	row[w.extractedAt] = parquet.Int64Value(r.extractedAt).Level(0, 0, w.extractedAt)
	for _, c := range w.columns {
		raw := r.fields[c.name]
		if isNull(raw) {
			row[c.index] = parquet.NullValue().Level(0, 0, c.index)
			continue
		}
		v, err := w.value(c.kind, bytes.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("stream %q field %q value %s: %w", w.layout.name, c.name, raw, err)
		}
		row[c.index] = v.Level(0, 1, c.index)
	}
	extra, err := additionalProperties(w.layout, r.fields)
	if err != nil {
		return fmt.Errorf("stream %q: %w", w.layout.name, err)
	}
	if extra == nil {
		row[w.additional] = parquet.NullValue().Level(0, 0, w.additional)
	} else {
		row[w.additional] = parquet.ByteArrayValue(extra).Level(0, 1, w.additional)
	}
	if _, err := w.w.WriteRows([]parquet.Row{row}); err != nil {
		return err
	}
	w.n += int64(len(r.data))
	w.sinceFlush += int64(len(r.data))
	if w.sinceFlush >= rowGroupBytes {
		w.sinceFlush = 0
		return w.w.Flush()
	}
	return nil
}

func (w *parquetWriter) value(kind columnKind, raw json.RawMessage) (parquet.Value, error) {
	switch kind {
	case kindBoolean:
		switch string(raw) {
		case "true":
			return parquet.BooleanValue(true), nil
		case "false":
			return parquet.BooleanValue(false), nil
		}
		return parquet.Value{}, errors.New("is not a boolean")
	case kindInteger:
		n, err := integerValue(raw)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(n), nil
	case kindNumber:
		if w.format.ParquetNumberType == numberTypeString {
			if !isNumber(raw) {
				return parquet.Value{}, errors.New("is not a number")
			}
			return parquet.ByteArrayValue(raw), nil
		}
		b, err := decimalValue(raw, w.decimalScale)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.FixedLenByteArrayValue(b), nil
	case kindBigInteger:
		b, err := decimalValue(raw, 0)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.FixedLenByteArrayValue(b), nil
	case kindJSON:
		var b bytes.Buffer
		if err := json.Compact(&b, raw); err != nil {
			return parquet.Value{}, err
		}
		return parquet.ByteArrayValue(b.Bytes()), nil
	}

	var s string
	if raw[0] != '"' || json.Unmarshal(raw, &s) != nil {
		return parquet.Value{}, errors.New("is not a string")
	}
	switch kind {
	case kindDate:
		t, err := time.Parse(time.DateOnly, s)
		if err != nil {
			return parquet.Value{}, errors.New("is not a date (YYYY-MM-DD)")
		}
		return parquet.Int32Value(int32(t.Unix() / 86400)), nil
	case kindTimestampTZ:
		t, err := parseTimestamp(s, true)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(t.UnixMicro()), nil
	case kindTimestampNTZ:
		t, err := parseTimestamp(s, false)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(t.UnixMicro()), nil
	case kindTimeNTZ:
		t, err := time.Parse("15:04:05.999999999", s)
		if err != nil {
			return parquet.Value{}, errors.New("is not a time without time zone (HH:MM:SS[.ffffff])")
		}
		if t.Nanosecond()%1000 != 0 {
			return parquet.Value{}, errors.New("has digits below microseconds")
		}
		midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		return parquet.Int64Value(t.Sub(midnight).Microseconds()), nil
	default:
		return parquet.ByteArrayValue([]byte(s)), nil
	}
}

// parseTimestamp parses a date-time; withTZ accepts an offset and reads a
// value without one as UTC, !withTZ accepts only values without an offset.
func parseTimestamp(s string, withTZ bool) (time.Time, error) {
	layouts := timestampLayoutsNTZ
	if withTZ {
		layouts = append(append([]string{}, timestampLayoutsTZ...), timestampLayoutsNTZ...)
	}
	for _, l := range layouts {
		t, err := time.Parse(l, s)
		if err != nil {
			continue
		}
		if t.Nanosecond()%1000 != 0 {
			return time.Time{}, errors.New("has digits below microseconds")
		}
		return t, nil
	}
	if withTZ {
		return time.Time{}, errors.New("is not a date-time (RFC 3339)")
	}
	return time.Time{}, errors.New("is not a date-time without time zone (YYYY-MM-DDTHH:MM:SS[.ffffff])")
}

func isNumber(raw json.RawMessage) bool {
	if len(raw) == 0 || (raw[0] != '-' && (raw[0] < '0' || raw[0] > '9')) {
		return false
	}
	var n json.Number
	return json.Unmarshal(raw, &n) == nil
}

func parseRat(raw json.RawMessage) (*big.Rat, error) {
	if !isNumber(raw) {
		return nil, errors.New("is not a number")
	}
	r, ok := new(big.Rat).SetString(string(raw))
	if !ok {
		return nil, errors.New("is not a number")
	}
	return r, nil
}

func integerValue(raw json.RawMessage) (int64, error) {
	r, err := parseRat(raw)
	if err != nil {
		return 0, err
	}
	if !r.IsInt() {
		return 0, errors.New("is not an integer")
	}
	if !r.Num().IsInt64() {
		return 0, errors.New("does not fit INT64")
	}
	return r.Num().Int64(), nil
}

// decimalValue encodes a number as the 16-byte big-endian two's complement
// unscaled value of DECIMAL(38, scale).
func decimalValue(raw json.RawMessage, scale int) ([]byte, error) {
	r, err := parseRat(raw)
	if err != nil {
		return nil, err
	}
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
	if !r.IsInt() {
		return nil, fmt.Errorf("has more than %d decimal places", scale)
	}
	u := new(big.Int).Set(r.Num())
	if new(big.Int).Abs(u).Cmp(decimalLimit) >= 0 {
		return nil, fmt.Errorf("does not fit DECIMAL(%d, %d)", maxDecimalPrecision, scale)
	}
	if u.Sign() < 0 {
		u.Add(u, twoTo128)
	}
	return u.FillBytes(make([]byte, decimalBytes)), nil
}

func (w *parquetWriter) size() int64 { return w.n }

func (w *parquetWriter) close() error { return w.w.Close() }
