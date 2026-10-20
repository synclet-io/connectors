package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"google.golang.org/api/iterator"
)

// TestIntegration runs Check and two overwrite syncs against a real bucket.
// It needs DESTINATION_GCS_TEST_BUCKET and DESTINATION_GCS_TEST_CREDENTIALS
// (a service account key JSON with roles/storage.objectAdmin on the bucket);
// everything is written under a random prefix that the test removes.
func TestIntegration(t *testing.T) {
	bucket := os.Getenv("DESTINATION_GCS_TEST_BUCKET")
	key := os.Getenv("DESTINATION_GCS_TEST_CREDENTIALS")
	if bucket == "" || key == "" {
		t.Skip("set DESTINATION_GCS_TEST_BUCKET and DESTINATION_GCS_TEST_CREDENTIALS to run against Cloud Storage")
	}
	prefix := "synclet-test-" + uuid.NewString()
	raw := fmt.Sprintf(`{"bucket":%q,"bucket_path":%q,"credentials":{"auth_type":"service_account","service_account_info":%q},"format":{"format_type":"jsonl","compression":"gzip"}}`,
		bucket, prefix, key)
	var cfg Config
	require.NoError(t, cfg.UnmarshalJSON([]byte(raw)))

	client, err := newGCSClient(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		store := newGCSStore(client, bucket)
		keys, _ := store.list(t.Context(), prefix+"/")
		_ = store.delete(t.Context(), keys)
		_ = client.Close()
	})

	d := NewGCSDestination()
	require.NoError(t, d.Check(t.Context(), cfg))

	catalog := fixtureCatalog(t, fixtureStream("orders", "shop", synclet.DestinationSyncModeOverwrite))
	for i, start := range []time.Time{testStart, testStart.Add(time.Hour)} {
		d.now = func() time.Time { return start }
		var out bytes.Buffer
		in := synclet.NewMessageReader(strings.NewReader(strings.Join(fixtureLines("orders", "shop"), "\n") + "\n"))
		require.NoError(t, d.Write(t.Context(), cfg, catalog, in, synclet.NewEmitter(&out)), "sync %d", i)
		assert.Contains(t, out.String(), `"type":"STATE"`)
	}

	var names []string
	it := client.Bucket(bucket).Objects(t.Context(), &storage.Query{Prefix: prefix + "/"})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		names = append(names, attrs.Name)
	}
	start := testStart.Add(time.Hour)
	assert.Equal(t, []string{fmt.Sprintf("%s/shop/orders/2026_10_18/%d_RUN_0.jsonl.gz", prefix, start.UnixMilli())}, withoutRunID(names),
		"the second overwrite removed the first sync's file")

	r, err := client.Bucket(bucket).Object(names[0]).ReadCompressed(true).NewReader(t.Context())
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	data := readJSONL(t, gunzip(t, b))
	require.Len(t, data, len(fixtureRecords))
	for i, raw := range fixtureRecords {
		assert.Equal(t, raw, string(data[i]))
	}
}
