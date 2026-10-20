package main

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"google.golang.org/api/googleapi"
)

func TestCheckErrorMapping(t *testing.T) {
	api := func(code int) error { return &googleapi.Error{Code: code, Message: "m"} }
	token := &auth.Error{Response: &http.Response{StatusCode: http.StatusBadRequest}, Err: errors.New("invalid_grant")}
	require.ErrorContains(t, describeListError(token, "b", "p/"), "invalid credentials")
	require.ErrorContains(t, describeListError(api(http.StatusUnauthorized), "b", "p/"), "invalid credentials")
	require.ErrorContains(t, describeReadError(api(http.StatusUnauthorized), "k"), "invalid credentials")
	require.ErrorContains(t, describeListError(api(http.StatusNotFound), "b", "p/"), `bucket "b" does not exist`)
	require.ErrorContains(t, describeListError(storage.ErrBucketNotExist, "b", "p/"), `bucket "b" does not exist`)
	require.ErrorContains(t, describeListError(api(http.StatusForbidden), "b", "p/"), `no list permission on prefix "p/"`)
	require.ErrorContains(t, describeReadError(api(http.StatusForbidden), "p/k"), `no read permission on "p/k"`)
	require.ErrorContains(t, describeListError(errors.New("dial tcp: refused"), "b", "p/"), `list prefix "p/" of bucket "b": dial tcp`)
	require.ErrorContains(t, describeCredentialError(errors.New("metadata: unreachable")), "cannot fetch an access token")
}

func TestCheck(t *testing.T) {
	f := newFakeGCS(t)
	f.createBucket("check")
	f.put("check", "data/a.csv", []byte(datasetCSV), t0)
	f.put("check", "empty/zero.csv", nil, t0)

	stream := StreamConfig{Name: "s", Glob: "data/*.csv", Format: Format{Filetype: formatCSV}}
	cases := []struct {
		name   string
		user   string
		mutate func(*Config)
		want   string
	}{
		{"ok", "reader", func(*Config) {}, ""},
		{"empty object", "reader", func(c *Config) { c.Streams[0].Glob = "empty/*.csv" }, ""},
		{"revoked key", "revoked", func(*Config) {}, "invalid credentials: the token request was refused"},
		{"malformed key", "reader", func(c *Config) {
			c.Credentials.ServiceAccountInfo = `{"type":"service_account","client_email":"reader@synclet-test.iam.gserviceaccount.com","private_key":"not a key"}`
		}, "cannot fetch an access token"},
		{"missing bucket", "reader", func(c *Config) { c.Bucket = "no-such-bucket" }, `bucket "no-such-bucket" does not exist`},
		{"no list permission", "nolist", func(*Config) {}, `no list permission on prefix "data/" of bucket "check" (storage.objects.list)`},
		{"no read permission", "noread", func(*Config) {}, `no read permission on "data/a.csv" (storage.objects.get)`},
		{"no matching object", "reader", func(c *Config) { c.Streams[0].Glob = "nothing/*.csv" }, `no object matches glob "nothing/*.csv"`},
		{"no matching object with a schema", "reader", func(c *Config) {
			c.Streams[0].Glob = "nothing/*.csv"
			c.Streams[0].Schema = `{"type":"object","properties":{"id":{"type":"integer"}}}`
		}, ""},
		{"invalid config", "reader", func(c *Config) { c.Bucket = "" }, "bucket is required"},
		{"key of another type", "reader", func(c *Config) { c.Credentials.ServiceAccountInfo = `{"type":"authorized_user"}` }, `has type "authorized_user"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := f.config(t, c.user, "check", stream)
			c.mutate(&cfg)
			err := f.source().Check(t.Context(), cfg)
			if c.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, c.want)
		})
	}
}

// fakeDataset uploads the dataset in every format and compression.
func fakeDataset(t *testing.T, f *fakeGCS, bucket string) []StreamConfig {
	t.Helper()
	mem := newMemStore()
	streams := datasetStreams(t, mem)
	for key, o := range mem.objs {
		f.put(bucket, key, o.data, o.modified)
	}
	return streams
}

func TestReadFakeGCS(t *testing.T) {
	f := newFakeGCS(t)
	f.createBucket("read")
	cfg := f.config(t, "reader", "read", fakeDataset(t, f, "read")...)
	streams := discoverStreams(t, f.source(), cfg)
	for _, name := range []string{"csv", "tsv", "jsonl", "parquet"} {
		t.Run(name, func(t *testing.T) {
			require.JSONEq(t, wantDatasetSchema, string(streams[name].JSONSchema))
			out := runRead(t, f.source(), cfg, catalogOf(synclet.SyncModeFullRefresh, streams[name]), nil)
			require.NoError(t, out.Err)
			recs := records(t, out, name)
			require.Len(t, recs, 3*len(wantDataset))
			for i := range 3 {
				data, _ := stripMetadata(recs[i*len(wantDataset) : (i+1)*len(wantDataset)])
				requireSameRecords(t, wantDataset, data)
			}
		})
	}

	t.Run("content-encoding gzip", func(t *testing.T) {
		f.createBucket("encoded")
		f.putObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "encoded", Name: "in/a.csv.gz", ContentEncoding: "gzip", ContentType: "text/csv", Updated: t0},
			Content:     gzipped(t, []byte(datasetCSV)),
		})
		cfg := f.config(t, "reader", "encoded", StreamConfig{Name: "s", Glob: "in/*.csv.gz", Format: Format{Filetype: formatCSV}})
		out := runRead(t, f.source(), cfg, catalogOf(synclet.SyncModeFullRefresh, discoverStreams(t, f.source(), cfg)["s"]), nil)
		require.NoError(t, out.Err)
		data, _ := stripMetadata(records(t, out, "s"))
		requireSameRecords(t, wantDataset, data)
	})

	t.Run("incremental", func(t *testing.T) {
		f.createBucket("incremental")
		f.put("incremental", "in/a.csv", csvFile("1,a"), t0)
		f.put("incremental", "in/b.csv", csvFile("2,b"), t0)
		f.put("incremental", "in/", nil, t0)
		cfg := f.config(t, "reader", "incremental", StreamConfig{Name: "s", Glob: "in/**", Format: Format{Filetype: formatCSV}})
		cat := catalogOf(synclet.SyncModeIncremental, discoverStreams(t, f.source(), cfg)["s"])
		first := runRead(t, f.source(), cfg, cat, nil)
		require.NoError(t, first.Err)
		require.Equal(t, []string{"in/a.csv:1", "in/b.csv:1"}, recordFiles(t, first, "s"))
		require.Equal(t, streamState{Cursor: "2026-10-01T12:00:00Z", Keys: []string{"in/a.csv", "in/b.csv"}}, lastState(t, first))

		again := runRead(t, f.source(), cfg, cat, first.NextState())
		require.NoError(t, again.Err)
		require.Empty(t, recordFiles(t, again, "s"))

		later := t0.Add(1500 * time.Millisecond)
		f.put("incremental", "in/c.csv", csvFile("3,c"), later)
		f.put("incremental", "in/a.csv", csvFile("1,a", "10,a2"), later)
		second := runRead(t, f.source(), cfg, cat, first.NextState())
		require.NoError(t, second.Err)
		require.Equal(t, []string{"in/a.csv:1", "in/a.csv:2", "in/c.csv:1"}, recordFiles(t, second, "s"))
		require.Equal(t, streamState{Cursor: "2026-10-01T12:00:01.5Z", Keys: []string{"in/a.csv", "in/c.csv"}}, lastState(t, second))
	})
}

func TestConformance(t *testing.T) {
	f := newFakeGCS(t)
	f.createBucket("conformance")
	cfg := f.config(t, "reader", "conformance", fakeDataset(t, f, "conformance")...)
	broken := f.config(t, "revoked", "conformance", cfg.Streams...)
	streams := discoverStreams(t, f.source(), cfg)
	var all []synclet.Stream
	for _, name := range []string{"csv", "tsv", "jsonl", "parquet"} {
		all = append(all, streams[name])
	}
	for _, mode := range []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			testkit.RunSourceSuite(t, f.source(), testkit.SourceSuite{
				Config:       mustJSON(t, cfg),
				BrokenConfig: mustJSON(t, broken),
				Catalog:      catalogOf(mode, all...),
			})
		})
	}
}
