package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	apioption "google.golang.org/api/option"
)

// TestIntegration runs check, discover, read and the SDK suite against a real
// bucket. It needs GCS_TEST_BUCKET, and GCS_TEST_CREDENTIALS
// with a service account key (Application Default Credentials without it)
// that may create and delete objects in the bucket. The test writes the
// dataset under a fresh prefix and deletes it afterwards.
func TestIntegration(t *testing.T) {
	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("set GCS_TEST_BUCKET (and GCS_TEST_CREDENTIALS) to run against Cloud Storage")
	}
	creds := Credentials{AuthType: authApplicationDefault}
	var opts []apioption.ClientOption
	if key := os.Getenv("GCS_TEST_CREDENTIALS"); key != "" {
		creds = Credentials{AuthType: authService, ServiceAccountInfo: key}
		opts = append(opts, apioption.WithAuthCredentialsJSON(apioption.ServiceAccount, []byte(key)))
	}
	client, err := storage.NewClient(t.Context(), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	prefix := fmt.Sprintf("synclet-source-gcs-test/%d/", time.Now().UnixNano())
	t.Cleanup(func() { deletePrefix(t, client.Bucket(bucket), prefix) })

	mem := newMemStore()
	streams := datasetStreams(t, mem)
	for key, o := range mem.objs {
		w := client.Bucket(bucket).Object(prefix + key).NewWriter(t.Context())
		_, err := w.Write(o.data)
		require.NoError(t, errors.Join(err, w.Close()))
	}
	for i := range streams {
		streams[i].Glob = prefix + streams[i].Glob
	}
	cfg := Config{Bucket: bucket, Credentials: creds, Streams: streams}
	require.NoError(t, Source{}.Check(t.Context(), cfg))

	missing := cfg
	missing.Bucket = bucket + "-synclet-missing"
	require.ErrorContains(t, Source{}.Check(t.Context(), missing), "does not exist")

	discovered := discoverStreams(t, Source{}, cfg)
	var all []synclet.Stream
	for _, name := range []string{"csv", "tsv", "jsonl", "parquet"} {
		require.JSONEq(t, wantDatasetSchema, string(discovered[name].JSONSchema))
		out := runRead(t, Source{}, cfg, catalogOf(synclet.SyncModeFullRefresh, discovered[name]), nil)
		require.NoError(t, out.Err)
		recs := records(t, out, name)
		require.Len(t, recs, 3*len(wantDataset))
		for _, r := range recs {
			require.True(t, strings.HasPrefix(r[fieldFile].(string), prefix))
		}
		all = append(all, discovered[name])
	}
	testkit.RunSourceSuite(t, Source{}, testkit.SourceSuite{
		Config:       mustJSON(t, cfg),
		BrokenConfig: mustJSON(t, missing),
		Catalog:      catalogOf(synclet.SyncModeIncremental, all...),
	})
}

func deletePrefix(t *testing.T, b *storage.BucketHandle, prefix string) {
	ctx := context.Background()
	objs, err := (&gcsStore{bucket: b}).List(ctx, prefix)
	if err != nil {
		t.Logf("list %q for cleanup: %v", prefix, err)
		return
	}
	for _, o := range objs {
		if err := b.Object(o.Key).Delete(ctx); err != nil {
			t.Logf("delete %q: %v", o.Key, err)
		}
	}
}
