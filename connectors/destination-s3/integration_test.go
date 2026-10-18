package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestCheckSucceeds(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	require.NoError(t, NewS3Destination().Check(t.Context(), m.rootConfig(bucket, "")))
	keys, _ := m.objects(t, bucket)
	assert.Empty(t, keys, "the probe object is deleted")
}

func TestCheckFailures(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	readOnlyKey, readOnlySecret := m.restrictedUser(t, bucket, "s3:GetObject", "s3:ListBucket")
	noDeleteKey, noDeleteSecret := m.restrictedUser(t, bucket, "s3:GetObject", "s3:ListBucket", "s3:PutObject")
	noListKey, noListSecret := m.restrictedUser(t, bucket, "s3:PutObject", "s3:DeleteObject")

	cases := map[string]struct {
		cfg  Config
		want string
	}{
		"wrong secret":       {m.config(bucket, m.user, "wrong-secret", ""), "invalid credentials"},
		"unknown access key": {m.config(bucket, "nobody", "wrong-secret", ""), "invalid credentials"},
		"missing bucket":     {m.rootConfig("no-such-bucket", ""), `bucket "no-such-bucket" does not exist`},
		"no write":           {m.config(bucket, readOnlyKey, readOnlySecret, ""), "no write permission"},
		"no delete":          {m.config(bucket, noDeleteKey, noDeleteSecret, ""), "no delete permission"},
		"no list":            {m.config(bucket, noListKey, noListSecret, ""), "no list permission"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := NewS3Destination().Check(t.Context(), tc.cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// writeMinio runs Write against MinIO with lines as input and returns the
// echoed states and the error.
func writeMinio(t *testing.T, cfg Config, catalog *synclet.ConfiguredCatalog, start time.Time, lines []string) ([]synclet.StateMessage, error) {
	t.Helper()
	d := &S3Destination{now: func() time.Time { return start }}
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

func TestReadBackFromMinio(t *testing.T) {
	m := startMinio(t)
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
			cfg := m.rootConfig(bucket, `"format":`+f)
			catalog := fixtureCatalog(t, fixtureStream("orders", "shop", synclet.DestinationSyncModeAppend))
			states, err := writeMinio(t, cfg, catalog, testStart, fixtureLines("orders", "shop"))
			require.NoError(t, err)
			require.Len(t, states, 1)

			keys, content := m.objects(t, bucket)
			require.Len(t, keys, 1)
			key := keys[0]
			assert.Equal(t, fmt.Sprintf("sync/shop/orders/2026_10_18/%d_0%s", testStart.UnixMilli(), cfg.Format.extension()), key)
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

func TestPathTemplateKeysInMinio(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	cfg := m.rootConfig(bucket, `"path_template":"{namespace}/{stream}/country={field:country}/{yyyy}/{mm}/{dd}/{part}","format":{"format_type":"parquet"}`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "shop", synclet.DestinationSyncModeAppend))
	_, err := writeMinio(t, cfg, catalog, testStart, fixtureLines("orders", "shop"))
	require.NoError(t, err)

	keys, content := m.objects(t, bucket)
	ms := testStart.UnixMilli()
	assert.Equal(t, []string{
		fmt.Sprintf("sync/shop/orders/country=PL/2026/10/18/%d_1.parquet", ms),
		fmt.Sprintf("sync/shop/orders/country=UA/2026/10/18/%d_0.parquet", ms),
		fmt.Sprintf("sync/shop/orders/country=__null__/2026/10/18/%d_2.parquet", ms),
	}, keys)
	got, _ := readParquet(t, fixtureColumns(t), defaultDecimalScale, content[keys[1]])
	require.Len(t, got, 1)
	assert.Equal(t, "UA", got[0]["country"])
}

func TestAppendTwiceKeepsBothInMinio(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	cfg := m.rootConfig(bucket, "")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	_, err := writeMinio(t, cfg, catalog, testStart, fixtureLines("orders", ""))
	require.NoError(t, err)
	_, err = writeMinio(t, cfg, catalog, testStart.Add(time.Hour), fixtureLines("orders", ""))
	require.NoError(t, err)
	keys, _ := m.objects(t, bucket)
	assert.Len(t, keys, 2)
}

func TestOverwriteInMinio(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	cfg := m.rootConfig(bucket, "")
	m.putObject(t, bucket, "sync/orders/2026_01_01/old.jsonl")
	m.putObject(t, bucket, "sync/orders2/2026_01_01/other.jsonl")
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeOverwrite))

	// A sync that fails after a STATE uploaded its first file keeps the
	// previous objects and echoes no state.
	failing := []string{
		recordMessage("orders", "", fixtureRecords[0]),
		stateMessage("orders", "", 1),
		recordMessage("orders", "", `"not an object"`),
	}
	states, err := writeMinio(t, cfg, catalog, testStart, failing)
	require.Error(t, err)
	assert.Empty(t, states)
	keys, _ := m.objects(t, bucket)
	assert.Contains(t, keys, "sync/orders/2026_01_01/old.jsonl")
	assert.Len(t, keys, 3, "the old object, the other stream and the failed sync's first file")

	start := testStart.Add(time.Hour)
	_, err = writeMinio(t, cfg, catalog, start, fixtureLines("orders", ""))
	require.NoError(t, err)
	keys, _ = m.objects(t, bucket)
	assert.Equal(t, []string{
		fmt.Sprintf("sync/orders/2026_10_18/%d_0.jsonl", start.UnixMilli()),
		"sync/orders2/2026_01_01/other.jsonl",
	}, keys)
}

func TestRotationByRecordCountInMinio(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	cfg := m.rootConfig(bucket, `"max_records_per_file":1,"format":{"format_type":"csv"}`)
	catalog := fixtureCatalog(t, fixtureStream("orders", "", synclet.DestinationSyncModeAppend))
	_, err := writeMinio(t, cfg, catalog, testStart, fixtureLines("orders", ""))
	require.NoError(t, err)
	keys, content := m.objects(t, bucket)
	require.Len(t, keys, len(fixtureRecords))
	var got []map[string]any
	for _, k := range keys {
		got = append(got, readCSV(t, fixtureColumns(t), content[k])...)
	}
	assert.Equal(t, expectedRecords(t), got)
}
