package main

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustRaw(t *testing.T, doc any) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(doc)
	require.NoError(t, err)
	return b
}

func mustDecimal(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	require.NoError(t, err)
	return d
}

func TestValues(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("65a1b2c3d4e5f60718293a4b")
	require.NoError(t, err)
	when := time.Date(2024, 2, 29, 13, 45, 30, 123_000_000, time.UTC)
	cases := []struct {
		name   string
		value  any
		json   string
		schema string
	}{
		{"double", 1.5, `1.5`, `{"type":["null","number"]}`},
		{"double integral", 2.0, `2`, `{"type":["null","number"]}`},
		{"double NaN", math.NaN(), `null`, `{"type":["null","string"]}`},
		{"double +Inf", math.Inf(1), `null`, `{"type":["null","string"]}`},
		{"string", "héllo \"q\"\n<tag>", `"héllo \"q\"\n<tag>"`, `{"type":["null","string"]}`},
		{"date-like string", "2024-01-02T03:04:05Z", `"2024-01-02T03:04:05Z"`, `{"type":["null","string"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`},
		{"document", bson.D{{Key: "a", Value: int32(1)}}, `{"a":1}`, `{"type":["null","object"],"properties":{"a":{"type":["null","integer"]}}}`},
		{"array", bson.A{int32(1), "x"}, `[1,"x"]`, `{"type":["null","array"],"items":{"type":["null","integer","string"]}}`},
		{"empty array", bson.A{}, `[]`, `{"type":["null","array"]}`},
		{"binary", bson.Binary{Subtype: 0, Data: []byte{0, 1, 2, 0xff}}, `"AAEC/w=="`, `{"type":["null","string"]}`},
		{"uuid binary", bson.Binary{Subtype: 4, Data: bytes.Repeat([]byte{0xab}, 16)}, `"q6urq6urq6urq6urq6urqw=="`, `{"type":["null","string"]}`},
		{"undefined", bson.Undefined{}, `null`, `{"type":["null","string"]}`},
		{"object id", oid, `"65a1b2c3d4e5f60718293a4b"`, `{"type":["null","string"]}`},
		{"boolean", true, `true`, `{"type":["null","boolean"]}`},
		{"date", bson.NewDateTimeFromTime(when), `"2024-02-29T13:45:30.123Z"`, `{"type":["null","string"],"format":"date-time","airbyte_type":"timestamp_with_timezone"}`},
		{"date after 9999", bson.DateTime(253402300800000), `"{\"$date\":{\"$numberLong\":\"253402300800000\"}}"`, `{"type":["null","string"]}`},
		{"null", bson.Null{}, `null`, `{"type":["null","string"]}`},
		{"regex", bson.Regex{Pattern: "^a", Options: "i"}, `"{\"$regularExpression\":{\"pattern\":\"^a\",\"options\":\"i\"}}"`, `{"type":["null","string"]}`},
		{"javascript", bson.JavaScript("function(){}"), `"{\"$code\":\"function(){}\"}"`, `{"type":["null","string"]}`},
		{"symbol", bson.Symbol("sym"), `"sym"`, `{"type":["null","string"]}`},
		{"int32", int32(-7), `-7`, `{"type":["null","integer"]}`},
		{"timestamp", bson.Timestamp{T: 1700000000, I: 3}, `"{\"$timestamp\":{\"t\":1700000000,\"i\":3}}"`, `{"type":["null","string"]}`},
		{"int64 beyond 2^53", int64(9007199254740993), `9007199254740993`, `{"type":["null","integer"]}`},
		{"decimal128", mustDecimal(t, "12345678901234567890.123456789"), `12345678901234567890.123456789`, `{"type":["null","number"]}`},
		{"decimal128 exponent", mustDecimal(t, "1.5E+40"), `1.5E+40`, `{"type":["null","number"]}`},
		{"decimal128 NaN", decimal("NaN"), `null`, `{"type":["null","string"]}`},
		{"decimal128 -Inf", mustDecimal(t, "-Infinity"), `null`, `{"type":["null","string"]}`},
		{"min key", bson.MinKey{}, `"{\"$minKey\":1}"`, `{"type":["null","string"]}`},
		{"max key", bson.MaxKey{}, `"{\"$maxKey\":1}"`, `{"type":["null","string"]}`},
		{"db pointer", bson.DBPointer{DB: "db.c", Pointer: oid}, `"{\"$dbPointer\":{\"$ref\":\"db.c\",\"$id\":{\"$oid\":\"65a1b2c3d4e5f60718293a4b\"}}}"`, `{"type":["null","string"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := mustRaw(t, bson.D{{Key: "v", Value: tc.value}})
			got, err := appendDocument(nil, doc)
			require.NoError(t, err)
			require.JSONEq(t, `{"v":`+tc.json+`}`, string(got))
			require.True(t, json.Valid(got))
			require.Equal(t, `{"v":`+tc.json+`}`, string(got), "exact digits and text")

			sh := &shape{}
			require.NoError(t, sh.addDocument(doc))
			schema, err := json.Marshal(sh.props["v"].schema())
			require.NoError(t, err)
			require.JSONEq(t, tc.schema, string(schema))
			validate(t, sh.streamSchema(), got)
		})
	}
}

func TestAppendString(t *testing.T) {
	got := appendString(nil, "a\x00\x1f\\\"\xffé ")
	require.Equal(t, `"a\u0000\u001f\\\"�é`+" "+`"`, string(got))
	var back string
	require.NoError(t, json.Unmarshal(got, &back))
}

// validate checks that record is valid against schema, with format
// assertion on.
func validate(t *testing.T, schema json.RawMessage, record []byte) {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft7)
	c.AssertFormat()
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	require.NoError(t, err)
	require.NoError(t, c.AddResource("schema.json", doc))
	s, err := c.Compile("schema.json")
	require.NoError(t, err)
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(record))
	require.NoError(t, err)
	require.NoError(t, s.Validate(inst), "record %s against schema %s", record, schema)
}

// decimal parses a Decimal128 literal of a test fixture.
func decimal(s string) bson.Decimal128 {
	d, err := bson.ParseDecimal128(s)
	if err != nil {
		panic(err)
	}
	return d
}

// mixedSample is a sample in which the same fields hold values of
// different BSON types, or are missing.
func mixedSample() []bson.D {
	when := bson.NewDateTimeFromTime(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))
	return []bson.D{
		{{Key: "_id", Value: int32(1)}, {Key: "v", Value: int32(1)}, {Key: "n", Value: int64(5)}, {Key: "at", Value: when}, {Key: "tags", Value: bson.A{"a"}}},
		{{Key: "_id", Value: "two"}, {Key: "v", Value: "text"}, {Key: "n", Value: 2.5}, {Key: "at", Value: "2024-01-03T00:00:00Z"}, {Key: "tags", Value: bson.A{int32(1), bson.D{{Key: "k", Value: true}}}}},
		{{Key: "_id", Value: bson.NewObjectID()}, {Key: "v", Value: bson.D{{Key: "x", Value: int32(1)}, {Key: "y", Value: bson.A{when}}}}, {Key: "n", Value: decimal("0.1")}},
		{{Key: "_id", Value: int64(4)}, {Key: "v", Value: bson.A{int32(1), "two", bson.A{3.5}}}, {Key: "n", Value: bson.Null{}}, {Key: "at", Value: bson.Null{}}},
		{{Key: "_id", Value: int32(5)}, {Key: "v", Value: when}, {Key: "n", Value: math.NaN()}, {Key: "only_here", Value: bson.D{{Key: "deep", Value: bson.D{{Key: "er", Value: decimal("NaN")}}}}}},
		{{Key: "_id", Value: int32(6)}, {Key: "v", Value: bson.Null{}}, {Key: "at", Value: bson.Undefined{}}, {Key: "tags", Value: bson.A{}}},
		{{Key: "_id", Value: int32(7)}, {Key: "v", Value: bson.Binary{Data: []byte("x")}}, {Key: "v2", Value: bson.Timestamp{T: 1, I: 1}}},
	}
}

func mixedDocuments(t *testing.T) []bson.Raw {
	t.Helper()
	docs := mixedSample()
	raws := make([]bson.Raw, len(docs))
	for i, d := range docs {
		raws[i] = mustRaw(t, d)
	}
	return raws
}

func TestInferMixed(t *testing.T) {
	docs := mixedDocuments(t)
	sh := &shape{}
	for _, d := range docs {
		require.NoError(t, sh.addDocument(d))
	}
	schema := sh.streamSchema()
	for _, d := range docs {
		record, err := appendDocument(nil, d)
		require.NoError(t, err)
		validate(t, schema, record)
	}

	var got struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(schema, &got))
	require.Equal(t, []any{"null", "integer", "string"}, got.Properties["_id"]["type"])
	require.Equal(t, []any{"null", "integer", "string", "object", "array"}, got.Properties["v"]["type"])
	require.Equal(t, []any{"null", "number"}, got.Properties["n"]["type"], "integer and number mix to number")
	require.Equal(t, "date-time", got.Properties["at"]["format"], "dates and date strings")
	require.NotContains(t, got.Properties["v"], "format", "a date among other strings is no date-time field")
	require.Contains(t, got.Properties, "only_here")
	require.Contains(t, got.Properties, "v2")
}

func TestCursorExtJSON(t *testing.T) {
	oid := bson.NewObjectID()
	when := bson.NewDateTimeFromTime(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))
	for _, v := range []any{oid, when, int32(3), int64(1) << 60, 2.5, "s", mustDecimal(t, "1.10")} {
		doc := mustRaw(t, bson.D{{Key: "v", Value: v}})
		rv := doc.Lookup("v")
		raw, err := canonicalExtJSON(rv)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(string(raw), "{") || strings.HasPrefix(string(raw), `"`), string(raw))
		back, err := parseCanonicalExtJSON(raw)
		require.NoError(t, err)
		require.Equal(t, rv.Type, back.Type)
		require.True(t, rv.Equal(back), "%s round-trips", raw)
	}
}

func TestInferDateTimeFormat(t *testing.T) {
	cases := map[string]bool{
		"2024-01-02T03:04:05Z":          true,
		"2024-01-02T03:04:05.123+02:00": true,
		"2024-01-02T03:04:05,123Z":      false,
		"2024-01-02 03:04:05Z":          false,
		"2024-01-02T03:04:05+24:00":     false,
		"2024-01-02T03:04:05+23:60":     false,
		"2024-12-31T23:59:60Z":          false,
	}
	for value, isDate := range cases {
		t.Run(value, func(t *testing.T) {
			doc := mustRaw(t, bson.D{{Key: "a", Value: value}})
			sh := &shape{}
			require.NoError(t, sh.addDocument(doc))
			schema := sh.streamSchema()
			record, err := appendDocument(nil, doc)
			require.NoError(t, err)
			validate(t, schema, record)
			var got struct {
				Properties map[string]map[string]any `json:"properties"`
			}
			require.NoError(t, json.Unmarshal(schema, &got))
			if isDate {
				require.Equal(t, "date-time", got.Properties["a"]["format"])
			} else {
				require.NotContains(t, got.Properties["a"], "format")
			}
		})
	}
}

// FuzzIsDateTime checks that every string isDateTime accepts passes the
// date-time format check of the schema validator the tests use.
func FuzzIsDateTime(f *testing.F) {
	for _, seed := range []string{"2024-01-02T03:04:05.123+02:00", "0000-01-01T00:00:00+24:00", "2024-01-02t03:04:05z", "2024-12-31T23:59:60Z"} {
		f.Add(seed)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	require.NoError(f, c.AddResource("s.json", map[string]any{"type": "string", "format": "date-time"}))
	sch := c.MustCompile("s.json")
	f.Fuzz(func(t *testing.T, s string) {
		if isDateTime(s) {
			require.NoError(t, sch.Validate(s), "isDateTime accepts %q", s)
		}
	})
}
