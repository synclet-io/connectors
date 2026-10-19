package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// minioImage is the Pigsty build of MinIO: MinIO no longer publishes
// images, and this one ships mc for creating restricted users.
const minioImage = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"

const (
	pgImage    = "postgres:16-alpine"
	pgDatabase = "synclet"
	pgUser     = "synclet"
	pgPassword = "synclet-test"
)

var (
	containersMu sync.Mutex
	containers   []testcontainers.Container

	minioOnce sync.Once
	minioSrv  *minioServer
	minioErr  error

	pgOnce sync.Once
	pgCfg  Config
	pgErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	containersMu.Lock()
	for _, c := range containers {
		_ = testcontainers.TerminateContainer(c)
	}
	containersMu.Unlock()
	os.Exit(code)
}

func track(c testcontainers.Container) {
	containersMu.Lock()
	containers = append(containers, c)
	containersMu.Unlock()
}

func needDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts MinIO or Postgres in Docker; skipped under -short")
	}
}

// minioServer is one MinIO container every test uses, each test in its own
// bucket.
type minioServer struct {
	container *minio.MinioContainer
	endpoint  string
	user      string
	password  string
	mu        sync.Mutex
}

func startMinio(t *testing.T) *minioServer {
	t.Helper()
	needDocker(t)
	minioOnce.Do(func() {
		ctx := context.Background()
		c, err := minio.Run(ctx, minioImage)
		if c != nil {
			track(c)
		}
		if err != nil {
			minioErr = err
			return
		}
		addr, err := c.ConnectionString(ctx)
		if err != nil {
			minioErr = err
			return
		}
		minioSrv = &minioServer{container: c, endpoint: "http://" + addr, user: c.Username, password: c.Password}
		minioErr = minioSrv.mc(ctx, "alias", "set", "local", "http://localhost:9000", c.Username, c.Password)
	})
	require.NoError(t, minioErr)
	return minioSrv
}

// mc runs the MinIO client inside the container.
func (m *minioServer) mc(ctx context.Context, args ...string) error {
	code, out, err := m.container.Exec(ctx, append([]string{"mc"}, args...), exec.Multiplexed())
	if err != nil {
		return err
	}
	if code != 0 {
		b, _ := io.ReadAll(out)
		return fmt.Errorf("mc %s: exit %d: %s", strings.Join(args, " "), code, b)
	}
	return nil
}

func (m *minioServer) s3(t *testing.T) *s3.Client {
	t.Helper()
	cfg := Config{UploadingMethod: UploadingMethod{BucketName: "x", BucketRegion: defaultRegion, AccessKeyID: m.user, SecretAccessKey: m.password}}
	st, err := newStaging(t.Context(), cfg, m.endpoint, "")
	require.NoError(t, err)
	return st.client
}

// newBucket creates an empty bucket for one test.
func (m *minioServer) newBucket(t *testing.T) string {
	t.Helper()
	bucket := "t-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:20]
	_, err := m.s3(t).CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	return bucket
}

// restrictedUser creates a user allowed only the given S3 actions on bucket
// and returns its access key and secret.
func (m *minioServer) restrictedUser(t *testing.T, bucket string, actions ...string) (string, string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	ctx := t.Context()
	name := "u" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	secret := "secret-" + name
	quoted := make([]string, len(actions))
	for i, a := range actions {
		quoted[i] = fmt.Sprintf("%q", a)
	}
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[%s],"Resource":["arn:aws:s3:::%s","arn:aws:s3:::%s/*"]}]}`,
		strings.Join(quoted, ","), bucket, bucket)
	path := "/tmp/" + name + ".json"
	require.NoError(t, m.container.CopyToContainer(ctx, []byte(policy), path, 0o644))
	require.NoError(t, m.mc(ctx, "admin", "user", "add", "local", name, secret))
	require.NoError(t, m.mc(ctx, "admin", "policy", "create", "local", name, path))
	require.NoError(t, m.mc(ctx, "admin", "policy", "attach", "local", name, "--user", name))
	return name, secret
}

// keys lists every object key of bucket.
func (m *minioServer) keys(t *testing.T, bucket string) []string {
	t.Helper()
	out, err := m.s3(t).ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	return keys
}

// csvRows downloads a staged .csv.gz object and parses its rows.
func (m *minioServer) csvRows(t *testing.T, bucket, key string) [][]string {
	t.Helper()
	out, err := m.s3(t).GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)
	defer func() { _ = out.Body.Close() }()
	zr, err := gzip.NewReader(out.Body)
	require.NoError(t, err)
	r := csv.NewReader(zr)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	require.NoError(t, err)
	return rows
}

// destConfig is a configuration staging in bucket of MinIO with the root
// user; the warehouse side is a recorder, so host and database are
// placeholders.
func (m *minioServer) destConfig(bucket string) Config {
	return Config{
		Host: "redshift.example", Database: "dev", Schema: "s", Username: "loader", Password: "pw",
		UploadingMethod: UploadingMethod{
			BucketName: bucket, BucketPath: "stage", BucketRegion: "eu-west-1",
			AccessKeyID: m.user, SecretAccessKey: m.password,
		},
	}
}

// recorder is a warehouse that runs nothing: it logs every statement and
// answers queries and failures from the test's script.
type recorder struct {
	mu      sync.Mutex
	stmts   []string
	queries []string
	// fail returns the error a statement fails with, or nil.
	fail func(sql string) error
	// rows answers a query.
	rows func(sql string) ([][]any, error)
	// onExec runs before a statement is logged, for example to read the
	// staged file a COPY names.
	onExec func(sql string)
}

func (r *recorder) exec(_ context.Context, sql string) error {
	if r.onExec != nil {
		r.onExec(sql)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stmts = append(r.stmts, sql)
	if r.fail != nil {
		return r.fail(sql)
	}
	return nil
}

func (r *recorder) query(_ context.Context, sql string) ([][]any, error) {
	r.mu.Lock()
	r.queries = append(r.queries, sql)
	r.mu.Unlock()
	if r.rows != nil {
		return r.rows(sql)
	}
	return nil, nil
}

func (r *recorder) close() {}

func (r *recorder) log() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stmts...)
}

// recordingDestination writes through rec and stages in MinIO.
func recordingDestination(m *minioServer, rec *recorder) Destination {
	d := NewDestination()
	d.open = func(context.Context, Config) (warehouse, error) { return rec, nil }
	d.s3Endpoint = m.endpoint
	d.newID = func() string { return "sync1" }
	return d
}

// copyInterceptor runs every statement on a real Postgres except the
// Redshift-only COPY from S3, which goes to the recorder.
type copyInterceptor struct {
	warehouse
	rec *recorder
}

func (c copyInterceptor) exec(ctx context.Context, sql string) error {
	if strings.HasPrefix(sql, "COPY ") {
		return c.rec.exec(ctx, sql)
	}
	return c.warehouse.exec(ctx, sql)
}

// runPostgres starts one postgres:16 container for every test that needs
// a real login and privilege checks.
func runPostgres(t *testing.T) Config {
	t.Helper()
	needDocker(t)
	pgOnce.Do(func() {
		ctx := context.Background()
		ctr, err := postgres.Run(ctx, pgImage,
			postgres.WithDatabase(pgDatabase),
			postgres.WithUsername(pgUser),
			postgres.WithPassword(pgPassword),
			postgres.BasicWaitStrategies(),
		)
		if ctr != nil {
			track(ctr)
		}
		if err != nil {
			pgErr = err
			return
		}
		host, err := ctr.Host(ctx)
		if err != nil {
			pgErr = err
			return
		}
		port, err := ctr.MappedPort(ctx, "5432/tcp")
		if err != nil {
			pgErr = err
			return
		}
		pgCfg = Config{Host: host, Port: int(port.Num()), Database: pgDatabase, Username: pgUser, Password: pgPassword, SSLMode: SSLMode{Mode: sslDisable}}
	})
	require.NoError(t, pgErr)
	return pgCfg
}

func execSQL(t *testing.T, cfg Config, sql string) {
	t.Helper()
	cfg.UploadingMethod = UploadingMethod{BucketName: "placeholder", BucketRegion: defaultRegion}
	w, err := connect(t.Context(), cfg)
	require.NoError(t, err)
	defer w.close()
	require.NoError(t, w.exec(t.Context(), sql))
}

// closedPort returns a local TCP port nothing listens on.
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
	return runWriteRaw(t, d, cfg, catalog, []byte(strings.Join(lines, "\n")+"\n"))
}

func runWriteRaw(t *testing.T, d Destination, cfg Config, catalog, input []byte) *testkit.Output {
	t.Helper()
	config, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), d, synclet.Request{
		Command: synclet.CommandWrite,
		Config:  config,
		Catalog: catalog,
		Input:   bytes.NewReader(input),
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

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return b
}
