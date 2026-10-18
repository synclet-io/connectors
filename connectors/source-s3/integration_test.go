package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestCheck(t *testing.T) {
	e := sharedMinIO(t)
	e.createBucket(t, "check")
	e.put(t, "check", "data/a.csv", []byte(datasetCSV))
	e.put(t, "check", "empty/zero.csv", nil)
	e.addUser(t, "nolist", "nolist-secret", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::check/*"]}]}`)
	e.addUser(t, "noread", "noread-secret", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::check"]}]}`)
	e.addUser(t, "roleuser", "roleuser-secret", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Resource":["arn:aws:s3:::check","arn:aws:s3:::check/*"]}]}`)

	stream := StreamConfig{Name: "s", Glob: "data/*.csv", Format: Format{Filetype: formatCSV}}
	const role = "arn:minio:iam:::role/synclet-reader"
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"ok", func(*Config) {}, ""},
		{"empty object", func(c *Config) { c.Streams[0].Glob = "empty/*.csv" }, ""},
		{"wrong secret", func(c *Config) { c.Credentials.SecretAccessKey = "wrong" }, "invalid credentials (SignatureDoesNotMatch)"},
		{"unknown key", func(c *Config) { c.Credentials.AccessKeyID = "nobody" }, "invalid credentials (InvalidAccessKeyId)"},
		{"missing bucket", func(c *Config) { c.Bucket = "no-such-bucket" }, `bucket "no-such-bucket" does not exist`},
		{"no list permission", func(c *Config) {
			c.Credentials.AccessKeyID, c.Credentials.SecretAccessKey = "nolist", "nolist-secret"
		}, `no list permission on prefix "data/"`},
		{"no read permission", func(c *Config) {
			c.Credentials.AccessKeyID, c.Credentials.SecretAccessKey = "noread", "noread-secret"
		}, `no read permission on "data/a.csv"`},
		{"assume role", func(c *Config) {
			c.Credentials = Credentials{AuthType: authAssumeRole, RoleARN: role, AccessKeyID: "roleuser", SecretAccessKey: "roleuser-secret"}
		}, ""},
		{"role not assumable", func(c *Config) {
			c.Credentials = Credentials{AuthType: authAssumeRole, RoleARN: role, AccessKeyID: "roleuser", SecretAccessKey: "wrong"}
		}, `cannot assume role "` + role + `"`},
		{"no matching object", func(c *Config) { c.Streams[0].Glob = "nothing/*.csv" }, `no object matches glob "nothing/*.csv"`},
		{"no matching object with a schema", func(c *Config) {
			c.Streams[0].Glob = "nothing/*.csv"
			c.Streams[0].Schema = `{"type":"object","properties":{"id":{"type":"integer"}}}`
		}, ""},
		{"invalid config", func(c *Config) { c.Bucket = "" }, "bucket is required"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := e.config("check", stream)
			c.mutate(&cfg)
			err := Source{}.Check(t.Context(), cfg)
			if c.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, c.want)
		})
	}

	// Discover and read work through the assumed role too.
	cfg := e.config("check", stream)
	cfg.Credentials = Credentials{AuthType: authAssumeRole, RoleARN: role, AccessKeyID: "roleuser", SecretAccessKey: "roleuser-secret"}
	s := discoverStreams(t, Source{}, cfg)["s"]
	out := runRead(t, Source{}, cfg, catalogOf(synclet.SyncModeFullRefresh, s), nil)
	require.NoError(t, out.Err)
	require.Len(t, records(t, out, "s"), len(wantDataset))
}

// minioDataset uploads the dataset in every format and compression.
func minioDataset(t *testing.T, e *minioEnv, bucket string) []StreamConfig {
	t.Helper()
	mem := newMemStore()
	streams := datasetStreams(t, mem)
	for key, o := range mem.objs {
		e.put(t, bucket, key, o.data)
	}
	return streams
}

func TestReadMinIO(t *testing.T) {
	e := sharedMinIO(t)
	e.createBucket(t, "read")
	cfg := e.config("read", minioDataset(t, e, "read")...)
	streams := discoverStreams(t, Source{}, cfg)
	for _, name := range []string{"csv", "tsv", "jsonl", "parquet"} {
		t.Run(name, func(t *testing.T) {
			require.JSONEq(t, wantDatasetSchema, string(streams[name].JSONSchema))
			out := runRead(t, Source{}, cfg, catalogOf(synclet.SyncModeFullRefresh, streams[name]), nil)
			require.NoError(t, out.Err)
			recs := records(t, out, name)
			require.Len(t, recs, 3*len(wantDataset))
			for i := range 3 {
				data, _ := stripMetadata(recs[i*len(wantDataset) : (i+1)*len(wantDataset)])
				requireSameRecords(t, wantDataset, data)
			}
		})
	}

	t.Run("incremental", func(t *testing.T) {
		e.createBucket(t, "incremental")
		e.put(t, "incremental", "in/a.csv", csvFile("1,a"))
		e.put(t, "incremental", "in/b.csv", csvFile("2,b"))
		cfg := e.config("incremental", StreamConfig{Name: "s", Glob: "in/*.csv", Format: Format{Filetype: formatCSV}})
		cat := catalogOf(synclet.SyncModeIncremental, discoverStreams(t, Source{}, cfg)["s"])
		first := runRead(t, Source{}, cfg, cat, nil)
		require.NoError(t, first.Err)
		require.Equal(t, []string{"in/a.csv:1", "in/b.csv:1"}, recordFiles(t, first, "s"))

		again := runRead(t, Source{}, cfg, cat, first.NextState())
		require.NoError(t, again.Err)
		require.Empty(t, recordFiles(t, again, "s"))

		waitNewSecond()
		e.put(t, "incremental", "in/c.csv", csvFile("3,c"))
		e.put(t, "incremental", "in/a.csv", csvFile("1,a", "10,a2"))
		second := runRead(t, Source{}, cfg, cat, first.NextState())
		require.NoError(t, second.Err)
		require.ElementsMatch(t, []string{"in/a.csv:1", "in/a.csv:2", "in/c.csv:1"}, recordFiles(t, second, "s"))
	})
}

func TestConformance(t *testing.T) {
	e := sharedMinIO(t)
	e.createBucket(t, "conformance")
	cfg := e.config("conformance", minioDataset(t, e, "conformance")...)
	broken := cfg
	broken.Credentials.SecretAccessKey = "wrong"
	streams := discoverStreams(t, Source{}, cfg)
	var all []synclet.Stream
	for _, name := range []string{"csv", "tsv", "jsonl", "parquet"} {
		all = append(all, streams[name])
	}
	for _, mode := range []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			testkit.RunSourceSuite(t, Source{}, testkit.SourceSuite{
				Config:       mustJSON(t, cfg),
				BrokenConfig: mustJSON(t, broken),
				Catalog:      catalogOf(mode, all...),
			})
		})
	}
}
