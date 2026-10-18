package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

// minioImage is the Bitnami archive build of MinIO, which ships mc as well;
// MinIO no longer publishes minio/minio images to Docker Hub or Quay.
const minioImage = "bitnamilegacy/minio:2025.7.23"

var (
	containersMu sync.Mutex
	containers   []testcontainers.Container
	sharedOnce   sync.Once
	shared       *minioEnv
	sharedErr    error
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

func needDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("starts MinIO in Docker; skipped under -short")
	}
}

// minioEnv is a MinIO server shared by the Docker-backed tests; each test
// works in its own bucket.
type minioEnv struct {
	container *tcminio.MinioContainer
	endpoint  string
	user      string
	password  string
	client    *s3.Client
	mcOnce    sync.Once
	mcErr     error
}

func sharedMinIO(t *testing.T) *minioEnv {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { shared, sharedErr = runMinIO() })
	require.NoError(t, sharedErr)
	return shared
}

func runMinIO() (*minioEnv, error) {
	ctx := context.Background()
	c, err := tcminio.Run(ctx, minioImage,
		testcontainers.WithEntrypoint("minio"),
		testcontainers.WithCmd("server", "/bitnami/minio/data"),
	)
	if c != nil {
		containersMu.Lock()
		containers = append(containers, c)
		containersMu.Unlock()
	}
	if err != nil {
		return nil, err
	}
	addr, err := c.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}
	e := &minioEnv{container: c, endpoint: "http://" + addr, user: c.Username, password: c.Password}
	e.client = s3.New(s3.Options{
		Region:       defaultRegion,
		BaseEndpoint: aws.String(e.endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(e.user, e.password, ""),
	})
	return e, nil
}

// config reaches bucket as the root user.
func (e *minioEnv) config(bucket string, streams ...StreamConfig) Config {
	return Config{
		Bucket:         bucket,
		Endpoint:       e.endpoint,
		ForcePathStyle: true,
		Credentials:    Credentials{AuthType: authAccessKey, AccessKeyID: e.user, SecretAccessKey: e.password},
		Streams:        streams,
	}
}

func (e *minioEnv) createBucket(t *testing.T, bucket string) {
	t.Helper()
	_, err := e.client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
}

func (e *minioEnv) put(t *testing.T, bucket, key string, data []byte) {
	t.Helper()
	_, err := e.client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})
	require.NoError(t, err)
}

// mc runs the MinIO client inside the container against the server.
func (e *minioEnv) mc(t *testing.T, args ...string) {
	t.Helper()
	e.mcOnce.Do(func() {
		e.mcErr = e.exec(context.Background(), "mc", "alias", "set", "local", "http://127.0.0.1:9000", e.user, e.password)
	})
	require.NoError(t, e.mcErr)
	require.NoError(t, e.exec(t.Context(), append([]string{"mc"}, args...)...))
}

func (e *minioEnv) exec(ctx context.Context, cmd ...string) error {
	code, out, err := e.container.Exec(ctx, cmd, tcexec.Multiplexed(), tcexec.WithEnv([]string{"MC_CONFIG_DIR=/tmp/.mc"}))
	if err != nil {
		return err
	}
	b, _ := io.ReadAll(out)
	if code != 0 {
		return fmt.Errorf("%v: exit %d: %s", cmd, code, b)
	}
	return nil
}

// addUser creates a user with its own policy document.
func (e *minioEnv) addUser(t *testing.T, name, secret, policy string) {
	t.Helper()
	path := "/tmp/" + name + ".json"
	require.NoError(t, e.container.CopyToContainer(t.Context(), []byte(policy), path, 0o644))
	e.mc(t, "admin", "user", "add", "local", name, secret)
	e.mc(t, "admin", "policy", "create", "local", name, path)
	e.mc(t, "admin", "policy", "attach", "local", name, "--user", name)
}

// waitNewSecond sleeps past the current second, so the next upload gets a
// later LastModified than the ones before.
func waitNewSecond() {
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(1100 * time.Millisecond)))
}
