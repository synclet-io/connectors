package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// minioImage is the Pigsty build of MinIO: MinIO no longer publishes
	// images.
	minioImage   = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
	catalogImage = "apache/iceberg-rest-fixture:1.10.1"
	lakeBucket   = "lake"
	warehouse    = "s3://" + lakeBucket + "/warehouse"
)

var (
	envOnce sync.Once
	env     *lakeEnv
	envErr  error
)

// lakeEnv is one MinIO and one Iceberg REST catalog every Docker test uses,
// each test in its own namespace.
type lakeEnv struct {
	network  *testcontainers.DockerNetwork
	minio    *minio.MinioContainer
	catalog  testcontainers.Container
	endpoint string
	user     string
	password string
	uri      string
}

func TestMain(m *testing.M) {
	code := m.Run()
	if env != nil {
		env.terminate()
	}
	os.Exit(code)
}

func (e *lakeEnv) terminate() {
	if e.catalog != nil {
		_ = testcontainers.TerminateContainer(e.catalog)
	}
	if e.minio != nil {
		_ = testcontainers.TerminateContainer(e.minio)
	}
	if e.network != nil {
		_ = e.network.Remove(context.Background())
	}
}

func needDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts MinIO and an Iceberg REST catalog in Docker; skipped under -short")
	}
}

func startLake(t *testing.T) *lakeEnv {
	t.Helper()
	needDocker(t)
	envOnce.Do(func() {
		env, envErr = runLake()
		if envErr != nil && env != nil {
			env.terminate()
			env = nil
		}
	})
	require.NoError(t, envErr)
	return env
}

func runLake() (*lakeEnv, error) {
	ctx := context.Background()
	e := &lakeEnv{}
	nw, err := network.New(ctx)
	if err != nil {
		return e, err
	}
	e.network = nw
	mc, err := minio.Run(ctx, minioImage, network.WithNetwork([]string{"minio"}, nw))
	if mc != nil {
		e.minio = mc
	}
	if err != nil {
		return e, err
	}
	addr, err := mc.ConnectionString(ctx)
	if err != nil {
		return e, err
	}
	e.endpoint, e.user, e.password = "http://"+addr, mc.Username, mc.Password
	if _, err := e.s3Client(ctx).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(lakeBucket)}); err != nil {
		return e, fmt.Errorf("create bucket: %w", err)
	}

	cat, err := testcontainers.Run(ctx, catalogImage,
		network.WithNetwork([]string{"catalog"}, nw),
		testcontainers.WithExposedPorts("8181/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CATALOG_WAREHOUSE":              warehouse,
			"CATALOG_IO__IMPL":               "org.apache.iceberg.aws.s3.S3FileIO",
			"CATALOG_S3_ENDPOINT":            "http://minio:9000",
			"CATALOG_S3_PATH__STYLE__ACCESS": "true",
			"AWS_ACCESS_KEY_ID":              mc.Username,
			"AWS_SECRET_ACCESS_KEY":          mc.Password,
			"AWS_REGION":                     "us-east-1",
		}),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/v1/config").WithPort("8181/tcp").WithStartupTimeout(3*time.Minute)),
	)
	if cat != nil {
		e.catalog = cat
	}
	if err != nil {
		return e, err
	}
	ep, err := cat.PortEndpoint(ctx, "8181/tcp", "http")
	if err != nil {
		return e, err
	}
	e.uri = ep
	return e, nil
}

func (e *lakeEnv) s3Client(ctx context.Context) *s3.Client {
	cfg := e.s3Config(e.user, e.password)
	awsCfg, err := awsConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return newS3Client(awsCfg, cfg)
}

func (e *lakeEnv) s3Config(key, secret string) S3Config {
	return S3Config{
		WarehouseLocation: warehouse,
		Region:            defaultRegion,
		Endpoint:          e.endpoint,
		ForcePathStyle:    true,
		Credentials:       Credentials{AuthType: authAccessKey, AccessKeyID: key, SecretAccessKey: secret},
	}
}

// config returns a configuration writing to a new namespace of its own.
func (e *lakeEnv) config(t *testing.T) Config {
	t.Helper()
	ns := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	cfg := Config{
		Catalog:   CatalogConfig{CatalogType: catalogREST, URI: e.uri},
		Namespace: ns,
		S3:        e.s3Config(e.user, e.password),
	}
	cfg.applyDefaults()
	l, err := openLake(t.Context(), cfg)
	require.NoError(t, err)
	defer l.close()
	require.NoError(t, l.cat.CreateNamespace(t.Context(), l.namespace, nil))
	return cfg
}

// catalogProxy forwards to the catalog. It answers 401 to requests without
// the bearer token when token is set, 409 to table commits while conflicts
// is above zero (or always when it is negative), and reject to every table
// commit when it is set.
type catalogProxy struct {
	*httptest.Server
	token     string
	conflicts atomic.Int64
	commits   atomic.Int64
	reject    atomic.Int64
}

var commitPath = regexp.MustCompile(`^/v1/namespaces/[^/]+/tables/[^/]+$`)

func newCatalogProxy(t *testing.T, target string) *catalogProxy {
	t.Helper()
	u, err := url.Parse(target)
	require.NoError(t, err)
	p := &catalogProxy{}
	rp := httputil.NewSingleHostReverseProxy(u)
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.token != "" && r.Header.Get("Authorization") != "Bearer "+p.token {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"message":"Not authorized","type":"NotAuthorizedException","code":401}}`)
			return
		}
		if r.Method == http.MethodPost && commitPath.MatchString(r.URL.Path) {
			p.commits.Add(1)
			if status := p.reject.Load(); status != 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(int(status))
				_, _ = io.WriteString(w, `{"error":{"message":"Invalid update","type":"BadRequestException","code":400}}`)
				return
			}
			if n := p.conflicts.Load(); n != 0 {
				if n > 0 {
					p.conflicts.Add(-1)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":{"message":"Requirement failed: branch main has changed","type":"CommitFailedException","code":409}}`)
				return
			}
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(p.Close)
	return p
}

func closedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// stream builds a configured stream from a JSON schema.
func stream(name, schema string, mode synclet.DestinationSyncMode, pk [][]string, cursor []string) synclet.ConfiguredStream {
	return synclet.ConfiguredStream{
		Stream:              synclet.Stream{Name: name, JSONSchema: json.RawMessage(schema)},
		SyncMode:            synclet.SyncModeIncremental,
		DestinationSyncMode: mode,
		PrimaryKey:          pk,
		CursorField:         cursor,
	}
}

func rec(streamName, data string) string {
	return `{"type":"RECORD","record":{"stream":"` + streamName + `","data":` + data + `,"emitted_at":1700000000000}}`
}

func state(streamName, value string) string {
	return `{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + streamName + `"},"stream_state":` + value + `}}}`
}

// runWrite runs a write through the SDK's transport with lines as stdin.
func runWrite(t *testing.T, d Destination, cfg Config, streams []synclet.ConfiguredStream, lines ...string) *testkit.Output {
	t.Helper()
	catalog, err := json.Marshal(synclet.ConfiguredCatalog{Streams: streams})
	require.NoError(t, err)
	return runRaw(t, d, cfg, catalog, strings.NewReader(strings.Join(lines, "\n")+"\n"))
}

func runRaw(t *testing.T, d Destination, cfg Config, catalog []byte, input io.Reader) *testkit.Output {
	t.Helper()
	config, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), d, synclet.Request{
		Command: synclet.CommandWrite,
		Config:  config,
		Catalog: catalog,
		Input:   input,
	})
	require.NoError(t, err)
	return out
}

// stateValues returns the stream_state of every echoed STREAM state.
func stateValues(out *testkit.Output) []string {
	var vals []string
	for _, st := range out.States() {
		if st.Stream != nil {
			vals = append(vals, string(st.Stream.StreamState))
		}
	}
	return vals
}

// loadTestTable loads a table of cfg's namespace with the connector's IO.
func loadTestTable(t *testing.T, cfg Config, name string) *table.Table {
	t.Helper()
	l, err := openLake(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(l.close)
	tbl, found, err := l.loadTable(t.Context(), name)
	require.NoError(t, err)
	require.True(t, found, "table %s", name)
	return tbl
}

// tableRows reads a table's current snapshot and returns the given columns
// of every row, rendered by Arrow and sorted.
func tableRows(t *testing.T, cfg Config, name string, cols ...string) [][]string {
	t.Helper()
	tbl := loadTestTable(t, cfg, name)
	if tbl.CurrentSnapshot() == nil {
		return nil
	}
	at, err := tbl.Scan(table.WithSelectedFields(cols...)).ToArrowTable(t.Context())
	require.NoError(t, err)
	defer at.Release()
	var rows [][]string
	tr := array.NewTableReader(at, 1024)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idx := make([]int, len(cols))
		for i, c := range cols {
			f := rec.Schema().FieldIndices(c)
			require.Len(t, f, 1, "column %s", c)
			idx[i] = f[0]
		}
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]string, len(cols))
			for i, ci := range idx {
				row[i] = renderValue(rec.Column(ci), r)
			}
			rows = append(rows, row)
		}
	}
	require.NoError(t, tr.Err())
	sort.Slice(rows, func(a, b int) bool { return strings.Join(rows[a], "\x00") < strings.Join(rows[b], "\x00") })
	return rows
}

func renderValue(a arrow.Array, i int) string {
	if a.IsNull(i) {
		return "<NULL>"
	}
	if d, ok := a.(*array.Decimal128); ok {
		return d.Value(i).ToString(d.DataType().(*arrow.Decimal128Type).Scale)
	}
	return a.ValueStr(i)
}

// tableTypes returns every column of a table's current schema with its
// Iceberg type.
func tableTypes(t *testing.T, cfg Config, name string) map[string]string {
	t.Helper()
	tbl := loadTestTable(t, cfg, name)
	got := map[string]string{}
	for _, f := range tbl.Schema().Fields() {
		got[f.Name] = f.Type.String()
	}
	return got
}

// snapshots returns the operation of every snapshot of a table, oldest
// first.
func snapshots(t *testing.T, cfg Config, name string) []string {
	t.Helper()
	tbl := loadTestTable(t, cfg, name)
	var ops []string
	for _, s := range tbl.Metadata().Snapshots() {
		op := ""
		if s.Summary != nil {
			op = string(s.Summary.Operation)
		}
		ops = append(ops, op)
	}
	return ops
}

// dataObjects lists the objects under a table's data directory.
func dataObjects(t *testing.T, e *lakeEnv, cfg Config, name string) []string {
	t.Helper()
	ns, err := cfg.namespace()
	require.NoError(t, err)
	_, prefix, err := cfg.S3.bucketAndPrefix()
	require.NoError(t, err)
	key := prefix + "/" + strings.Join(ns, "/") + "/" + name + "/data/"
	var keys []string
	p := s3.NewListObjectsV2Paginator(e.s3Client(t.Context()), &s3.ListObjectsV2Input{Bucket: aws.String(lakeBucket), Prefix: aws.String(key)})
	for p.HasMorePages() {
		page, err := p.NextPage(t.Context())
		require.NoError(t, err)
		for _, o := range page.Contents {
			keys = append(keys, "s3://"+lakeBucket+"/"+aws.ToString(o.Key))
		}
	}
	sort.Strings(keys)
	return keys
}

// liveFiles lists the data and delete files of a table's current snapshot.
func liveFiles(t *testing.T, cfg Config, name string) []string {
	t.Helper()
	tbl := loadTestTable(t, cfg, name)
	if tbl.CurrentSnapshot() == nil {
		return nil
	}
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	seen := map[string]bool{}
	add := func(f iceberg.DataFile) {
		seen[f.FilePath()] = true
	}
	for _, task := range tasks {
		add(task.File)
		for _, d := range task.EqualityDeleteFiles {
			add(d)
		}
		for _, d := range task.DeleteFiles {
			add(d)
		}
	}
	var out []string
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
