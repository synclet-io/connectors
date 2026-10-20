package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/oauth2"
)

func TestCheckSucceeds(t *testing.T) {
	f := startFakeGCS(t)
	bucket := f.newBucket(t)
	require.NoError(t, f.destination(nil).Check(t.Context(), f.config(bucket, "")))
	keys, _ := f.objects(t, bucket)
	assert.Empty(t, keys, "the probe object is deleted")
}

func TestCheckFailures(t *testing.T) {
	f := startFakeGCS(t)
	bucket := f.newBucket(t)
	deny := func(match func(*http.Request) bool) func(*http.Request) *http.Response {
		return func(r *http.Request) *http.Response {
			if match(r) {
				return apiError(http.StatusForbidden, "forbidden", "synclet@example.iam.gserviceaccount.com does not have access")
			}
			return nil
		}
	}
	cases := map[string]struct {
		bucket string
		rt     func(*http.Request) *http.Response
		want   string
	}{
		"invalid credentials": {bucket, func(*http.Request) *http.Response {
			return apiError(http.StatusUnauthorized, "required", "Invalid Credentials")
		}, "invalid credentials: Invalid Credentials"},
		"missing bucket": {"no-such-bucket", nil, `bucket "no-such-bucket" does not exist`},
		"no write": {bucket, deny(func(r *http.Request) bool {
			return strings.HasPrefix(r.URL.Path, "/upload/")
		}), "no write permission on gs://" + bucket + "/sync/_synclet_check_"},
		"no delete": {bucket, deny(func(r *http.Request) bool { return r.Method == http.MethodDelete }), "no delete permission"},
		"no list": {bucket, deny(func(r *http.Request) bool {
			return r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/o")
		}), "no list permission on gs://" + bucket + "/sync/"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := f.destination(tc.rt).Check(t.Context(), f.config(tc.bucket, ""))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestDescribeTokenError(t *testing.T) {
	err := describeError(fmt.Errorf("upload: %w", &oauth2.RetrieveError{ErrorCode: "invalid_grant"}), "b", "no write permission", "k")
	assert.Contains(t, err.Error(), "invalid credentials")
}

// writeFake runs Write against the fake server with lines as input and
// returns the echoed states and the error.
func writeFake(t *testing.T, f *fakeGCS, cfg Config, catalog *synclet.ConfiguredCatalog, start time.Time, lines []string) ([]synclet.StateMessage, error) {
	t.Helper()
	d := f.destination(nil)
	d.now = func() time.Time { return start }
	var out bytes.Buffer
	in := synclet.NewMessageReader(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	werr := d.Write(t.Context(), cfg, catalog, in, synclet.NewEmitter(&out))
	var states []synclet.StateMessage
	mr := synclet.NewMessageReader(&out)
	for {
		msg, err := mr.Next()
		if err != nil {
			break
		}
		if msg.Type == synclet.MessageTypeState {
			states = append(states, *msg.State)
		}
	}
	return states, werr
}

func fixtureLines(stream, namespace string) []string {
	lines := make([]string, 0, len(fixtureRecords)+1)
	for _, r := range fixtureRecords {
		lines = append(lines, recordMessage(stream, namespace, r))
	}
	return append(lines, stateMessage(stream, namespace, 1))
}

func TestReadBackFromFake(t *testing.T) {
	m := startFakeGCS(t)
	formats := map[string]string{
		"jsonl":          `{"format_type":"jsonl"}`,
		"jsonl gzip":     `{"format_type":"jsonl","compression":"gzip"}`,
		"csv":            `{"format_type":"csv"}`,
		"csv gzip":       `{"format_type":"csv","compression":"gzip"}`,
		"parquet":        `{"format_type":"parquet"}`,
		"parquet zstd":   `{"format_type":"parquet","compression":"zstd"}`,
		"parquet as str": `{"format_type":"parquet","parquet_number_type":"string"}`,
	}
	for name, f := range formats {
		t.Run(name, func(t *testing.T) {
			bucket := m.newBucket(t)
			cfg := m.config(bucket, `"format":`+f)
			catalog := fixtureCatalog(t, fixtureStream("orders", "shop", synclet.DestinationSyncModeAppend))
			states, err := writeFake(t, m, cfg, catalog, testStart, fixtureLines("orders", "shop"))
			require.NoError(t, err)
			require.Len(t, states, 1)

			keys, content := m.objects(t, bucket)
			require.Len(t, keys, 1)
			key := keys[0]
			assert.Equal(t, fmt.Sprintf("sync/shop/orders/2026_10_18/%d_RUN_0%s", testStart.UnixMilli(), cfg.Format.extension()), withoutRunID(keys)[0])
			b := content[key]
			if strings.HasSuffix(key, ".gz") {
				b = gunzip(t, b)
			}
			cols := fixtureColumns(t)
			switch cfg.Format.FormatType {
			case formatJSONL:
				data := readJSONL(t, b)
				require.Len(t, data, len(fixtureRecords))
				for i, raw := range fixtureRecords {
					assert.Equal(t, raw, string(data[i]))
				}
			case formatCSV:
				assert.Equal(t, expectedRecords(t), readCSV(t, cols, b))
			case formatParquet:
				got, _ := readParquet(t, cols, *cfg.Format.DecimalScale, b)
				assert.Equal(t, expectedRecords(t), got)
			}
		})
	}
}

func TestPathTemplateKeysInFake(t *testing.T) {
	m := startFakeGCS(t)
	bucket := m.newBucket(t)
	cfg := m.config(bucket, `"path_template":"{namespace}/{stream}/country={field:country}/{yyyy}/{mm}/{dd}/{part}","format":{"format_type":"parquet"}`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "shop", synclet.DestinationSyncModeAppend))
	_, err := writeFake(t, m, cfg, catalog, testStart, fixtureLines("orders", "shop"))
	require.NoError(t, err)

	keys, content := m.objects(t, bucket)
	ms := testStart.UnixMilli()
	assert.Equal(t, []string{
		fmt.Sprintf("sync/shop/orders/country=PL/2026/10/18/%d_RUN_1.parquet", ms),
		fmt.Sprintf("sync/shop/orders/country=UA/2026/10/18/%d_RUN_0.parquet", ms),
		fmt.Sprintf("sync/shop/orders/country=__null__/2026/10/18/%d_RUN_2.parquet", ms),
	}, withoutRunID(keys))
	got, _ := readParquet(t, fixtureColumns(t), defaultDecimalScale, content[keys[1]])
	require.Len(t, got, 1)
	assert.Equal(t, "UA", got[0]["country"])
}

func TestAppendTwiceKeepsBothInFake(t *testing.T) {
	m := startFakeGCS(t)
	bucket := m.newBucket(t)
	cfg := m.config(bucket, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	_, err := writeFake(t, m, cfg, catalog, testStart, fixtureLines("orders", ""))
	require.NoError(t, err)
	_, err = writeFake(t, m, cfg, catalog, testStart.Add(time.Hour), fixtureLines("orders", ""))
	require.NoError(t, err)
	keys, _ := m.objects(t, bucket)
	assert.Len(t, keys, 2)
}

func TestOverwriteInFake(t *testing.T) {
	m := startFakeGCS(t)
	bucket := m.newBucket(t)
	cfg := m.config(bucket, "")
	m.putObject(bucket, "sync/orders/2026_01_01/old.jsonl")
	m.putObject(bucket, "sync/orders2/2026_01_01/other.jsonl")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeOverwrite))

	// A sync that fails after a STATE uploaded its first file keeps the
	// previous objects and echoes no state.
	failing := []string{
		recordMessage("orders", "", fixtureRecords[0]),
		stateMessage("orders", "", 1),
		recordMessage("orders", "", `"not an object"`),
	}
	states, err := writeFake(t, m, cfg, catalog, testStart, failing)
	require.Error(t, err)
	assert.Empty(t, states)
	keys, _ := m.objects(t, bucket)
	assert.Contains(t, keys, "sync/orders/2026_01_01/old.jsonl")
	assert.Len(t, keys, 3, "the old object, the other stream and the failed sync's first file")

	start := testStart.Add(time.Hour)
	_, err = writeFake(t, m, cfg, catalog, start, fixtureLines("orders", ""))
	require.NoError(t, err)
	keys, _ = m.objects(t, bucket)
	assert.Equal(t, []string{
		fmt.Sprintf("sync/orders/2026_10_18/%d_RUN_0.jsonl", start.UnixMilli()),
		"sync/orders2/2026_01_01/other.jsonl",
	}, withoutRunID(keys))
}

func TestRotationByRecordCountInFake(t *testing.T) {
	m := startFakeGCS(t)
	bucket := m.newBucket(t)
	cfg := m.config(bucket, `"max_records_per_file":1,"format":{"format_type":"csv"}`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	_, err := writeFake(t, m, cfg, catalog, testStart, fixtureLines("orders", ""))
	require.NoError(t, err)
	keys, content := m.objects(t, bucket)
	require.Len(t, keys, len(fixtureRecords))
	var got []map[string]any
	for _, k := range keys {
		got = append(got, readCSV(t, fixtureColumns(t), content[k])...)
	}
	assert.Equal(t, expectedRecords(t), got)
}

func TestStoreListAndDelete(t *testing.T) {
	m := startFakeGCS(t)
	bucket := m.newBucket(t)
	var keys []string
	for i := range 3 * deleteConcurrency {
		k := fmt.Sprintf("sync/a/%03d.jsonl", i)
		m.putObject(bucket, k)
		keys = append(keys, k)
	}
	m.putObject(bucket, "sync/b/keep.jsonl")
	client, err := m.client(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	store := newGCSStore(client, bucket)

	listed, err := store.list(t.Context(), "sync/a/")
	require.NoError(t, err)
	assert.Equal(t, keys, listed)

	require.NoError(t, store.delete(t.Context(), append(listed, "sync/a/gone.jsonl")), "a missing object is not an error")
	left, _ := m.objects(t, bucket)
	assert.Equal(t, []string{"sync/b/keep.jsonl"}, left)
}
