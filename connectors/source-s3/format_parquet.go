package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
)

// parquetRows reads a Parquet file spooled to a temporary file, since the
// format needs random access. Nested groups become objects, LIST groups and
// repeated fields arrays, MAP groups with string keys objects and other
// MAP groups arrays of {"key", "value"} objects.
type parquetRows struct {
	schema *parquet.Schema
	groups []parquet.RowGroup
	rows   parquet.Rows // rows of the current row group
	leaves []leafPath
	buf    []parquet.Row
	pos    int
	close  func() error
}

// leafPath is the chain of fields from the schema root to one leaf column.
type leafPath []parquet.Field

func openParquetRows(r io.Reader, closeFn func() error) (rowSource, error) {
	f, size, err := spool(r)
	if err != nil {
		return nil, fmt.Errorf("spool parquet: %w", err)
	}
	removeFile := func() error { return errors.Join(f.Close(), os.Remove(f.Name())) }
	pf, err := parquet.OpenFile(f, size)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("parquet: %w", err), removeFile())
	}
	p := &parquetRows{
		schema: pf.Schema(),
		groups: pf.RowGroups(),
		buf:    make([]parquet.Row, 0, 64),
		close: func() error {
			return errors.Join(removeFile(), closeFn())
		},
	}
	collectLeaves(pf.Schema(), nil, &p.leaves)
	return p, nil
}

func collectLeaves(n parquet.Node, path leafPath, out *[]leafPath) {
	for _, f := range n.Fields() {
		p := append(path[:len(path):len(path)], f)
		if f.Leaf() {
			*out = append(*out, p)
			continue
		}
		collectLeaves(f, p, out)
	}
}

func (p *parquetRows) Close() error {
	var err error
	if p.rows != nil {
		err = p.rows.Close()
	}
	return errors.Join(err, p.close())
}

func (p *parquetRows) Next() (map[string]any, error) {
	for p.pos == len(p.buf) {
		if p.rows == nil {
			if len(p.groups) == 0 {
				return nil, io.EOF
			}
			p.rows, p.groups = p.groups[0].Rows(), p.groups[1:]
		}
		p.buf = p.buf[:cap(p.buf)]
		n, err := p.rows.ReadRows(p.buf)
		p.buf, p.pos = p.buf[:n], 0
		switch {
		case errors.Is(err, io.EOF), n == 0 && err == nil:
			if cerr := p.rows.Close(); cerr != nil {
				return nil, cerr
			}
			p.rows = nil
		case err != nil:
			return nil, err
		}
	}
	row := p.buf[p.pos]
	p.pos++
	return p.assemble(row)
}

// group and list are the containers of a row being assembled.
type (
	group struct{ m map[string]any }
	list  struct{ items []any }
)

// assemble rebuilds a record from its column values, using each value's
// repetition and definition levels.
func (p *parquetRows) assemble(row parquet.Row) (map[string]any, error) {
	root := &group{m: map[string]any{}}
	var convErr error
	row.Range(func(col int, vals []parquet.Value) bool {
		if col >= len(p.leaves) {
			convErr = fmt.Errorf("column %d is not in the schema", col)
			return false
		}
		if err := assembleColumn(root, p.leaves[col], vals); err != nil {
			convErr = fmt.Errorf("column %s: %w", leafName(p.leaves[col]), err)
			return false
		}
		return true
	})
	if convErr != nil {
		return nil, &badRowError{err: convErr}
	}
	out := make(map[string]any, len(root.m))
	for _, f := range p.schema.Fields() {
		out[f.Name()] = finish(f, root.m[f.Name()])
	}
	return out, nil
}

func leafName(path leafPath) string {
	names := make([]string, len(path))
	for i, f := range path {
		names[i] = f.Name()
	}
	return strings.Join(names, ".")
}

func assembleColumn(root *group, path leafPath, vals []parquet.Value) error {
	idx := make([]int, len(path))
	for i := range idx {
		idx[i] = -1
	}
	for _, v := range vals {
		r, d := v.RepetitionLevel(), v.DefinitionLevel()
		var reps, defs int
		parent := root
	walk:
		for i, f := range path {
			last := i == len(path)-1
			name := f.Name()
			switch {
			case f.Repeated():
				reps++
				defs++
				l, _ := parent.m[name].(*list)
				if l == nil {
					l = &list{}
					parent.m[name] = l
				}
				if d < defs {
					break walk
				}
				if reps >= r {
					idx[i]++
					for j := i + 1; j < len(idx); j++ {
						idx[j] = -1
					}
				}
				if idx[i] < 0 {
					idx[i] = 0
				}
				for len(l.items) <= idx[i] {
					l.items = append(l.items, nil)
				}
				if last {
					cv, err := convertLeaf(f.Type(), v)
					if err != nil {
						return err
					}
					l.items[idx[i]] = cv
					break walk
				}
				g, _ := l.items[idx[i]].(*group)
				if g == nil {
					g = &group{m: map[string]any{}}
					l.items[idx[i]] = g
				}
				parent = g
			default:
				if f.Optional() {
					defs++
					if d < defs {
						if _, ok := parent.m[name]; !ok {
							parent.m[name] = nil
						}
						break walk
					}
				}
				if last {
					cv, err := convertLeaf(f.Type(), v)
					if err != nil {
						return err
					}
					parent.m[name] = cv
					break walk
				}
				g, _ := parent.m[name].(*group)
				if g == nil {
					g = &group{m: map[string]any{}}
					parent.m[name] = g
				}
				parent = g
			}
		}
	}
	return nil
}

func logical(t parquet.Type) any {
	if lt := t.LogicalType(); lt != nil {
		return lt.Value
	}
	return nil
}

// finish turns the assembled containers of field f into JSON values,
// unwrapping LIST and MAP groups.
func finish(f parquet.Field, v any) any {
	switch v := v.(type) {
	case *list:
		items := make([]any, len(v.items))
		for i, it := range v.items {
			items[i] = finishGroupFields(f, it)
		}
		return items
	case *group:
		switch logical(f.Type()).(type) {
		case *format.ListType:
			return finishList(f, v)
		case *format.MapType:
			return finishMap(f, v)
		}
		return finishGroupFields(f, v)
	}
	return v
}

// finishGroupFields renders a struct group, or passes a leaf value through.
func finishGroupFields(f parquet.Field, v any) any {
	g, ok := v.(*group)
	if !ok {
		return v
	}
	out := make(map[string]any, len(g.m))
	for _, c := range f.Fields() {
		out[c.Name()] = finish(c, g.m[c.Name()])
	}
	return out
}

// finishList unwraps LIST: a group with one repeated child whose elements
// hold the values, directly or in their single field.
func finishList(f parquet.Field, g *group) any {
	children := f.Fields()
	if len(children) != 1 {
		return finishGroupFields(f, g)
	}
	rep := children[0]
	l, _ := g.m[rep.Name()].(*list)
	if l == nil {
		return []any{}
	}
	items := make([]any, len(l.items))
	for i, it := range l.items {
		eg, ok := it.(*group)
		if ok && len(rep.Fields()) == 1 {
			el := rep.Fields()[0]
			items[i] = finish(el, eg.m[el.Name()])
			continue
		}
		items[i] = finishGroupFields(rep, it)
	}
	return items
}

// finishMap unwraps MAP: string keys give an object, other keys an array
// of {"key", "value"} objects.
func finishMap(f parquet.Field, g *group) any {
	children := f.Fields()
	if len(children) != 1 || len(children[0].Fields()) != 2 {
		return finishGroupFields(f, g)
	}
	kv := children[0]
	keyF, valF := kv.Fields()[0], kv.Fields()[1]
	l, _ := g.m[kv.Name()].(*list)
	if l == nil {
		return map[string]any{}
	}
	if _, ok := logical(keyF.Type()).(*format.StringType); ok {
		out := make(map[string]any, len(l.items))
		for _, it := range l.items {
			eg, _ := it.(*group)
			if eg == nil {
				continue
			}
			k, _ := eg.m[keyF.Name()].(string)
			out[k] = finish(valF, eg.m[valF.Name()])
		}
		return out
	}
	items := make([]any, 0, len(l.items))
	for _, it := range l.items {
		eg, _ := it.(*group)
		if eg == nil {
			continue
		}
		items = append(items, map[string]any{
			"key":   finish(keyF, eg.m[keyF.Name()]),
			"value": finish(valF, eg.m[valF.Name()]),
		})
	}
	return items
}

// convertLeaf renders one primitive value as JSON: integers and decimals as
// exact numbers, dates and times as ISO 8601 strings, binary as base64.
func convertLeaf(t parquet.Type, v parquet.Value) (any, error) {
	if v.IsNull() {
		return nil, nil
	}
	switch lt := logical(t).(type) {
	case *format.StringType, *format.EnumType, *format.JsonType:
		return string(v.ByteArray()), nil
	case *format.UUIDType:
		b := v.ByteArray()
		if len(b) != 16 {
			return nil, fmt.Errorf("UUID of %d bytes", len(b))
		}
		h := hex.EncodeToString(b)
		return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
	case *format.DecimalType:
		var unscaled *big.Int
		switch v.Kind() {
		case parquet.Int32:
			unscaled = big.NewInt(int64(v.Int32()))
		case parquet.Int64:
			unscaled = big.NewInt(v.Int64())
		default:
			unscaled = twosComplement(v.ByteArray())
		}
		return json.Number(formatDecimal(unscaled, int(lt.Scale))), nil
	case *format.DateType:
		return time.Unix(int64(v.Int32())*86400, 0).UTC().Format(time.DateOnly), nil
	case *format.TimestampType:
		ts := unitTime(v.Int64(), unitDuration(lt.Unit))
		if lt.IsAdjustedToUTC {
			return ts.Format(time.RFC3339Nano), nil
		}
		return ts.Format("2006-01-02T15:04:05.999999999"), nil
	case *format.TimeType:
		var n int64
		if v.Kind() == parquet.Int32 {
			n = int64(v.Int32())
		} else {
			n = v.Int64()
		}
		s := time.Unix(0, 0).UTC().Add(time.Duration(n) * unitDuration(lt.Unit)).Format("15:04:05.999999999")
		if lt.IsAdjustedToUTC {
			s += "Z"
		}
		return s, nil
	case *format.IntType:
		if !lt.IsSigned {
			if v.Kind() == parquet.Int32 {
				return json.Number(strconv.FormatUint(uint64(v.Uint32()), 10)), nil
			}
			return json.Number(strconv.FormatUint(v.Uint64(), 10)), nil
		}
	}
	if ct := t.ConvertedType(); ct != nil {
		switch *ct {
		case deprecated.UTF8, deprecated.Enum, deprecated.Json:
			return string(v.ByteArray()), nil
		}
	}
	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean(), nil
	case parquet.Int32:
		return json.Number(strconv.FormatInt(int64(v.Int32()), 10)), nil
	case parquet.Int64:
		return json.Number(strconv.FormatInt(v.Int64(), 10)), nil
	case parquet.Int96:
		return int96Time(v.Int96()).Format(time.RFC3339Nano), nil
	case parquet.Float:
		return floatNumber(float64(v.Float()), 32)
	case parquet.Double:
		return floatNumber(v.Double(), 64)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return base64.StdEncoding.EncodeToString(v.ByteArray()), nil
	}
	return nil, fmt.Errorf("unsupported parquet type %s", t)
}

func unitDuration(u format.TimeUnit) time.Duration {
	if u.Value == nil {
		return time.Millisecond
	}
	return u.Value.Duration()
}

// unitTime converts n units since the epoch without overflowing the
// nanosecond range of time.Duration.
func unitTime(n int64, unit time.Duration) time.Time {
	perSec := int64(time.Second / unit)
	sec, rem := n/perSec, n%perSec
	if rem < 0 {
		sec--
		rem += perSec
	}
	return time.Unix(sec, rem*int64(unit)).UTC()
}

func floatNumber(f float64, bits int) (any, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("%v is not a JSON number", f)
	}
	return json.Number(strconv.FormatFloat(f, 'g', -1, bits)), nil
}

// twosComplement reads a big-endian two's complement integer.
func twosComplement(b []byte) *big.Int {
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b))*8))
	}
	return n
}

// formatDecimal writes unscaled × 10^-scale with every digit.
func formatDecimal(unscaled *big.Int, scale int) string {
	if scale <= 0 {
		return new(big.Int).Mul(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil)).String()
	}
	neg := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	s := digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	if neg {
		s = "-" + s
	}
	return s
}

// int96Time decodes the legacy INT96 timestamp: nanoseconds of the day in
// the first eight bytes, the Julian day in the last four.
func int96Time(i deprecated.Int96) time.Time {
	nanos := int64(uint64(i[1])<<32 | uint64(i[0]))
	days := int64(i[2]) - 2440588
	return time.Unix(days*86400, nanos).UTC()
}

// declare adds the file's schema to the inference.
func (p *parquetRows) declare(in *inference) {
	for _, f := range p.schema.Fields() {
		typ, fmtName, abType := jsonTypeOf(f)
		in.declare(f.Name(), typ, fmtName, abType)
	}
}

// jsonTypeOf maps a Parquet field to a JSON schema type, format and
// airbyte_type.
func jsonTypeOf(f parquet.Field) (string, string, string) {
	if f.Repeated() {
		return typeArray, "", ""
	}
	t := f.Type()
	if !f.Leaf() {
		switch logical(t).(type) {
		case *format.ListType:
			return typeArray, "", ""
		case *format.MapType:
			if len(f.Fields()) == 1 && len(f.Fields()[0].Fields()) == 2 {
				if _, ok := logical(f.Fields()[0].Fields()[0].Type()).(*format.StringType); !ok {
					return typeArray, "", ""
				}
			}
		}
		return typeObject, "", ""
	}
	switch lt := logical(t).(type) {
	case *format.StringType, *format.EnumType, *format.JsonType:
		return typeString, "", ""
	case *format.UUIDType:
		return typeString, "", ""
	case *format.DecimalType:
		return typeNumber, "", ""
	case *format.DateType:
		return typeString, "date", ""
	case *format.TimestampType:
		if lt.IsAdjustedToUTC {
			return typeString, "date-time", "timestamp_with_timezone"
		}
		return typeString, "date-time", "timestamp_without_timezone"
	case *format.TimeType:
		if lt.IsAdjustedToUTC {
			return typeString, "time", "time_with_timezone"
		}
		return typeString, "time", "time_without_timezone"
	case *format.IntType:
		return typeInteger, "", ""
	}
	if ct := t.ConvertedType(); ct != nil {
		switch *ct {
		case deprecated.UTF8, deprecated.Enum, deprecated.Json:
			return typeString, "", ""
		}
	}
	switch t.Kind() {
	case parquet.Boolean:
		return typeBoolean, "", ""
	case parquet.Int32, parquet.Int64:
		return typeInteger, "", ""
	case parquet.Int96:
		return typeString, "date-time", "timestamp_with_timezone"
	case parquet.Float, parquet.Double:
		return typeNumber, "", ""
	}
	return typeString, "", ""
}
