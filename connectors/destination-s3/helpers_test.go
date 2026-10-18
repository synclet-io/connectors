package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

// minioImage is the Pigsty build of MinIO: MinIO no longer publishes
// images, and this one ships mc for creating restricted users.
const minioImage = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"

var (
	sharedOnce  sync.Once
	sharedMinio *minioServer
	sharedErr   error
)

// minioServer is one MinIO container every test uses, each test in its own
// bucket.
type minioServer struct {
	container *minio.MinioContainer
	endpoint  string
	user      string
	password  string
	mu        sync.Mutex
}

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedMinio != nil {
		_ = testcontainers.TerminateContainer(sharedMinio.container)
	}
	os.Exit(code)
}

func needDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts MinIO in Docker; skipped under -short")
	}
}

func startMinio(t *testing.T) *minioServer {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() {
		ctx := context.Background()
		c, err := minio.Run(ctx, minioImage)
		if err != nil {
			sharedErr = err
			if c != nil {
				_ = testcontainers.TerminateContainer(c)
			}
			return
		}
		addr, err := c.ConnectionString(ctx)
		if err != nil {
			sharedErr = err
			_ = testcontainers.TerminateContainer(c)
			return
		}
		sharedMinio = &minioServer{container: c, endpoint: "http://" + addr, user: c.Username, password: c.Password}
		sharedErr = sharedMinio.mc(ctx, "alias", "set", "local", "http://localhost:9000", c.Username, c.Password)
	})
	require.NoError(t, sharedErr)
	return sharedMinio
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

// newBucket creates an empty bucket for one test.
func (m *minioServer) newBucket(t *testing.T) string {
	t.Helper()
	bucket := "t-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:20]
	_, err := m.client(t, m.user, m.password).CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
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

func (m *minioServer) client(t *testing.T, key, secret string) *s3.Client {
	t.Helper()
	c, err := newS3Client(t.Context(), m.config(bucketless, key, secret, ""))
	require.NoError(t, err)
	return c
}

const bucketless = "-"

// config returns a destination configuration for bucket as the given user;
// extra is appended to the JSON object.
func (m *minioServer) config(bucket, key, secret, extra string) Config {
	raw := fmt.Sprintf(`{"bucket":%q,"bucket_path":"sync","endpoint":%q,"force_path_style":true,"credentials":{"auth_type":"access_key","access_key_id":%q,"secret_access_key":%q}`,
		bucket, m.endpoint, key, secret)
	if extra != "" {
		raw += "," + extra
	}
	var cfg Config
	if err := cfg.UnmarshalJSON([]byte(raw + "}")); err != nil {
		panic(err)
	}
	return cfg
}

func (m *minioServer) rootConfig(bucket, extra string) Config {
	return m.config(bucket, m.user, m.password, extra)
}

// objects returns every key of bucket with its content, sorted by key.
func (m *minioServer) objects(t *testing.T, bucket string) ([]string, map[string][]byte) {
	t.Helper()
	c := m.client(t, m.user, m.password)
	store := newS3Store(c, bucket)
	keys, err := store.list(t.Context(), "")
	require.NoError(t, err)
	sort.Strings(keys)
	content := map[string][]byte{}
	for _, k := range keys {
		out, err := c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(k)})
		require.NoError(t, err)
		var buf bytes.Buffer
		_, err = buf.ReadFrom(out.Body)
		require.NoError(t, err)
		require.NoError(t, out.Body.Close())
		content[k] = buf.Bytes()
	}
	return keys, content
}

func (m *minioServer) putObject(t *testing.T, bucket, key string) {
	t.Helper()
	_, err := m.client(t, m.user, m.password).PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("old"),
	})
	require.NoError(t, err)
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
