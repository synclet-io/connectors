package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// fakeGCS is an in-process Cloud Storage server for one test.
type fakeGCS struct {
	server *fakestorage.Server
}

func startFakeGCS(t *testing.T) *fakeGCS {
	t.Helper()
	s, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	require.NoError(t, err)
	t.Cleanup(s.Stop)
	return &fakeGCS{server: s}
}

// newBucket creates an empty bucket.
func (f *fakeGCS) newBucket(t *testing.T) string {
	t.Helper()
	bucket := "t-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:20]
	f.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})
	return bucket
}

// client returns a new client of the server; rt, when set, sees every
// request before the server does.
func (f *fakeGCS) client(ctx context.Context, rt func(*http.Request) *http.Response) (*storage.Client, error) {
	transport := f.server.HTTPClient().Transport
	if rt != nil {
		transport = interceptor{base: transport, rt: rt}
	}
	return storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: transport}))
}

// destination returns a destination writing to the server.
func (f *fakeGCS) destination(rt func(*http.Request) *http.Response) *GCSDestination {
	d := NewGCSDestination()
	d.newClient = func(ctx context.Context, _ Config) (*storage.Client, error) { return f.client(ctx, rt) }
	return d
}

// interceptor answers a request with rt's response, or passes it on when
// rt returns nil.
type interceptor struct {
	base http.RoundTripper
	rt   func(*http.Request) *http.Response
}

func (i interceptor) RoundTrip(r *http.Request) (*http.Response, error) {
	if resp := i.rt(r); resp != nil {
		resp.Request = r
		return resp, nil
	}
	return i.base.RoundTrip(r)
}

// apiError is a JSON API error response.
func apiError(code int, reason, message string) *http.Response {
	body := fmt.Sprintf(`{"error":{"code":%d,"message":%q,"errors":[{"reason":%q,"message":%q}]}}`, code, message, reason, message)
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// config returns a destination configuration for bucket; extra is
// appended to the JSON object.
func (f *fakeGCS) config(bucket, extra string) Config {
	raw := fmt.Sprintf(`{"bucket":%q,"bucket_path":"sync","credentials":{"auth_type":"application_default"}`, bucket)
	if extra != "" {
		raw += "," + extra
	}
	var cfg Config
	if err := cfg.UnmarshalJSON([]byte(raw + "}")); err != nil {
		panic(err)
	}
	return cfg
}

// objects returns every name of bucket with its content, sorted by name.
func (f *fakeGCS) objects(t *testing.T, bucket string) ([]string, map[string][]byte) {
	t.Helper()
	resp, err := f.server.ListObjectsWithOptionsPaginated(bucket, fakestorage.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, resp.NextPageToken)
	var keys []string
	content := map[string][]byte{}
	for _, a := range resp.Objects {
		obj, err := f.server.GetObject(bucket, a.Name)
		require.NoError(t, err)
		keys = append(keys, a.Name)
		content[a.Name] = obj.Content
	}
	sort.Strings(keys)
	return keys, content
}

func (f *fakeGCS) putObject(bucket, key string) {
	f.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: key},
		Content:     []byte("old"),
	})
}

var runIDPattern = regexp.MustCompile(`_[0-9a-f]{12}_(\d+)\.`)

// withoutRunID replaces the random sync id in part names with RUN.
func withoutRunID(keys []string) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = runIDPattern.ReplaceAllString(k, "_RUN_$1.")
	}
	return out
}
