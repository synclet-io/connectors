package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/dns"
	"golang.org/x/crypto/ssh"
)

const (
	mongoImage   = "mongo:7"
	rootUser     = "root"
	rootPassword = "root-test-pw"
	testDatabase = "synclet"
	testUser     = "synclet"
	testPassword = "synclet-test-pw"
	replicaSet   = "rs0"
)

var (
	containersMu sync.Mutex
	containers   []testcontainers.Container
	sharedOnce   sync.Once
	shared       mongoEnv
	sharedErr    error
)

// mongoEnv is a running server: addr is its mapped address on the test
// host, member the address the replica set announces for it.
type mongoEnv struct {
	addr   string
	member string
	cfg    Config
	root   Config
}

// src returns a Source that dials every server address at the container.
func (e mongoEnv) src() Source { return redirectSource(e.addr) }

func redirectSource(addr string) Source {
	return Source{
		dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
		selectTimeout: 5 * time.Second,
	}
}

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
		t.Skip("starts MongoDB in Docker; skipped under -short")
	}
}

func track(c testcontainers.Container) {
	if c == nil {
		return
	}
	containersMu.Lock()
	containers = append(containers, c)
	containersMu.Unlock()
}

// runMongo starts a single-node replica set with a root user and loads the
// test users and collections.
func runMongo() (mongoEnv, error) {
	ctx := context.Background()
	ctr, err := mongodb.Run(ctx, mongoImage,
		mongodb.WithUsername(rootUser),
		mongodb.WithPassword(rootPassword),
		mongodb.WithReplicaSet(replicaSet),
	)
	if ctr != nil {
		track(ctr.Container)
	}
	if err != nil {
		return mongoEnv{}, err
	}
	addr, err := ctr.PortEndpoint(ctx, "27017/tcp", "")
	if err != nil {
		return mongoEnv{}, err
	}
	ip, err := ctr.ContainerIP(ctx)
	if err != nil {
		return mongoEnv{}, err
	}
	e := mongoEnv{addr: addr, member: net.JoinHostPort(ip, "27017")}
	uri := fmt.Sprintf("mongodb://%s/?replicaSet=%s", e.member, replicaSet)
	e.root = Config{DatabaseConfig: DatabaseConfig{ConnectionString: uri, Database: "admin", Username: rootUser, Password: rootPassword}}
	e.cfg = Config{DatabaseConfig: DatabaseConfig{ConnectionString: uri, Database: testDatabase, Username: testUser, Password: testPassword}}
	if err := seed(ctx, e); err != nil {
		return mongoEnv{}, fmt.Errorf("seed: %w", err)
	}
	return e, nil
}

func seed(ctx context.Context, e mongoEnv) error {
	var c *client
	var err error
	for deadline := time.Now().Add(time.Minute); ; {
		if c, err = e.src().connect(ctx, e.root); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Second)
	}
	defer c.close()
	admin := c.Database("admin")
	cmds := []bson.D{
		{{Key: "createRole", Value: "lister"}, {Key: "privileges", Value: bson.A{
			bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: testDatabase}, {Key: "collection", Value: ""}}}, {Key: "actions", Value: bson.A{"listCollections"}}},
		}}, {Key: "roles", Value: bson.A{}}},
		{{Key: "createUser", Value: testUser}, {Key: "pwd", Value: testPassword}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: testDatabase}}}}},
		{{Key: "createUser", Value: "noperm"}, {Key: "pwd", Value: "noperm-pw"}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "read"}, {Key: "db", Value: "other"}}}}},
		{{Key: "createUser", Value: "lister"}, {Key: "pwd", Value: "lister-pw"}, {Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "lister"}, {Key: "db", Value: "admin"}}}}},
	}
	for _, cmd := range cmds {
		if err := admin.RunCommand(ctx, cmd).Err(); err != nil {
			return err
		}
	}
	db := c.Database(testDatabase)
	day := func(d int) bson.DateTime {
		return bson.NewDateTimeFromTime(time.Date(2024, 1, d, 0, 0, 0, 0, time.UTC))
	}
	var users []any
	for i, name := range []string{"ada", "brian", "chen", "dana"} {
		users = append(users, bson.D{{Key: "_id", Value: int32(i + 1)}, {Key: "name", Value: name}, {Key: "email", Value: name + "@example.com"}, {Key: "updated_at", Value: day(i + 1)}})
	}
	if _, err := db.Collection("users").InsertMany(ctx, users); err != nil {
		return err
	}
	if _, err := db.Collection("types").InsertMany(ctx, []any{typesDocument()}); err != nil {
		return err
	}
	if _, err := db.Collection("events").InsertMany(ctx, eventDocs(1, 2, 3)); err != nil {
		return err
	}
	var mixed []any
	for _, d := range mixedSample() {
		mixed = append(mixed, d)
	}
	if _, err := db.Collection("mixed").InsertMany(ctx, mixed); err != nil {
		return err
	}
	if err := db.CreateView(ctx, "user_names", "users", mongo.Pipeline{{{Key: "$project", Value: bson.D{{Key: "name", Value: 1}}}}}); err != nil {
		return err
	}
	_, err = c.Database("other").Collection("things").InsertOne(ctx, bson.D{{Key: "x", Value: 1}})
	return err
}

// eventDocs are events whose n is one of ns and whose updated_at is the
// n-th of January 2024.
func eventDocs(ns ...int) []any {
	docs := make([]any, len(ns))
	for i, n := range ns {
		docs[i] = bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "n", Value: int32(n)},
			{Key: "updated_at", Value: bson.NewDateTimeFromTime(time.Date(2024, 1, n, 0, 0, 0, 0, time.UTC))}}
	}
	return docs
}

// typesDocument holds one field per BSON type.
func typesDocument() bson.D {
	oid, _ := bson.ObjectIDFromHex("65a1b2c3d4e5f60718293a4b")
	dec, _ := bson.ParseDecimal128("12345678901234567890.123456789")
	nan, _ := bson.ParseDecimal128("NaN")
	return bson.D{
		{Key: "_id", Value: oid},
		{Key: "c_double", Value: 1.5},
		{Key: "c_double_nan", Value: math.NaN()},
		{Key: "c_string", Value: "héllo"},
		{Key: "c_object", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: bson.A{"x"}}}},
		{Key: "c_array", Value: bson.A{int32(1), "two", 3.5}},
		{Key: "c_binary", Value: bson.Binary{Subtype: 0, Data: []byte{0, 1, 2, 0xff}}},
		{Key: "c_uuid", Value: bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}},
		{Key: "c_undefined", Value: bson.Undefined{}},
		{Key: "c_object_id", Value: oid},
		{Key: "c_bool", Value: true},
		{Key: "c_date", Value: bson.NewDateTimeFromTime(time.Date(2024, 2, 29, 13, 45, 30, 123_000_000, time.UTC))},
		{Key: "c_null", Value: bson.Null{}},
		{Key: "c_regex", Value: bson.Regex{Pattern: "^a", Options: "i"}},
		{Key: "c_javascript", Value: bson.JavaScript("function(){}")},
		{Key: "c_int32", Value: int32(-7)},
		{Key: "c_timestamp", Value: bson.Timestamp{T: 1700000000, I: 3}},
		{Key: "c_int64", Value: int64(9007199254740993)},
		{Key: "c_decimal", Value: dec},
		{Key: "c_decimal_nan", Value: nan},
		{Key: "c_min_key", Value: bson.MinKey{}},
		{Key: "c_max_key", Value: bson.MaxKey{}},
	}
}

func sharedMongo(t *testing.T) mongoEnv {
	t.Helper()
	needDocker(t)
	sharedOnce.Do(func() { shared, sharedErr = runMongo() })
	require.NoError(t, sharedErr)
	return shared
}

// rootClient connects to e as the root user.
func rootClient(t *testing.T, e mongoEnv) *client {
	t.Helper()
	c, err := e.src().connect(t.Context(), e.root)
	require.NoError(t, err)
	t.Cleanup(c.close)
	return c
}

// fakeDNS answers the driver's SRV and TXT lookups from srv and txt, keyed
// by host name, and fails every other lookup as NXDOMAIN.
func fakeDNS(t *testing.T, srv map[string][]*net.SRV, txt map[string][]string) {
	t.Helper()
	r := dns.DefaultResolver
	lookupSRV, lookupTXT := r.LookupSRV, r.LookupTXT
	t.Cleanup(func() { r.LookupSRV, r.LookupTXT = lookupSRV, lookupTXT })
	r.LookupSRV = func(service, proto, name string) (string, []*net.SRV, error) {
		if recs, ok := srv[name]; ok {
			return "", recs, nil
		}
		return "", nil, &net.DNSError{Err: "no such host", Name: "_" + service + "._" + proto + "." + name, IsNotFound: true}
	}
	r.LookupTXT = func(name string) ([]string, error) {
		if recs, ok := txt[name]; ok {
			return recs, nil
		}
		return nil, &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}
	}
}

func discoverStream(t *testing.T, src Source, cfg Config, name string) synclet.Stream {
	t.Helper()
	cat, err := src.Discover(t.Context(), cfg)
	require.NoError(t, err)
	for _, s := range cat.Streams {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("discover: no stream %s", name)
	return synclet.Stream{}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func runRead(t *testing.T, src Source, cfg Config, cat *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead,
		Config:  mustJSON(t, cfg),
		Catalog: mustJSON(t, cat),
		State:   state,
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	return out
}

func fullRefresh(streams ...synclet.Stream) *synclet.ConfiguredCatalog {
	cat := &synclet.ConfiguredCatalog{}
	for _, s := range streams {
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{
			Stream: s, SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
		})
	}
	return cat
}

// testCA is a certificate authority generated for one test.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM string
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "synclet test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{cert: cert, key: key, certPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

// serverPEM issues a server certificate for dnsName and returns it and its
// key in one PEM file, as mongod's tlsCertificateKeyFile takes them.
func (ca *testCA) serverPEM(t *testing.T, dnsName string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})) +
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}

// sshServer is an in-process SSH server with an Ed25519 and an ECDSA host
// key that serves direct-tcpip channels,
// dialing the address routes maps a requested address to.
type sshServer struct {
	addr     string
	hostKey  ssh.PublicKey
	ecdsaKey ssh.PublicKey
	password string
	key      ssh.PublicKey
	routes   map[string]string
}

func startSSHServer(t *testing.T, password string, key ssh.PublicKey, routes map[string]string) *sshServer {
	t.Helper()
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	hostKey, err := ssh.NewSignerFromKey(hostPriv)
	require.NoError(t, err)
	ecdsaPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ecdsaHostKey, err := ssh.NewSignerFromKey(ecdsaPriv)
	require.NoError(t, err)
	s := &sshServer{hostKey: hostKey.PublicKey(), ecdsaKey: ecdsaHostKey.PublicKey(), password: password, key: key, routes: routes}
	cfg := &ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, p []byte) (*ssh.Permissions, error) {
			if s.password != "" && string(p) == s.password {
				return nil, nil
			}
			return nil, os.ErrPermission
		},
		PublicKeyCallback: func(_ ssh.ConnMetadata, k ssh.PublicKey) (*ssh.Permissions, error) {
			if s.key != nil && string(k.Marshal()) == string(s.key.Marshal()) {
				return nil, nil
			}
			return nil, os.ErrPermission
		},
	}
	cfg.AddHostKey(hostKey)
	cfg.AddHostKey(ecdsaHostKey)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	s.addr = l.Addr().String()
	go func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			go s.serve(nc, cfg)
		}
	}()
	return s
}

func (s *sshServer) hostPort(t *testing.T) (string, int) {
	t.Helper()
	host, port, err := net.SplitHostPort(s.addr)
	require.NoError(t, err)
	p, err := strconv.Atoi(port)
	require.NoError(t, err)
	return host, p
}

func (s *sshServer) serve(nc net.Conn, cfg *ssh.ServerConfig) {
	conn, chans, reqs, err := ssh.NewServerConn(nc, cfg)
	if err != nil {
		_ = nc.Close()
		return
	}
	defer func() { _ = conn.Close() }()
	go ssh.DiscardRequests(reqs)
	for nch := range chans {
		if nch.ChannelType() != "direct-tcpip" {
			_ = nch.Reject(ssh.UnknownChannelType, "only direct-tcpip")
			continue
		}
		go s.forward(nch)
	}
}

func (s *sshServer) forward(nch ssh.NewChannel) {
	var req struct {
		Host     string
		Port     uint32
		OrigHost string
		OrigPort uint32
	}
	if err := ssh.Unmarshal(nch.ExtraData(), &req); err != nil {
		_ = nch.Reject(ssh.ConnectionFailed, err.Error())
		return
	}
	target := net.JoinHostPort(req.Host, strconv.Itoa(int(req.Port)))
	if r, ok := s.routes[target]; ok {
		target = r
	}
	up, err := net.Dial("tcp", target)
	if err != nil {
		_ = nch.Reject(ssh.ConnectionFailed, err.Error())
		return
	}
	ch, chReqs, err := nch.Accept()
	if err != nil {
		_ = up.Close()
		return
	}
	go ssh.DiscardRequests(chReqs)
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(ch, up); _ = ch.CloseWrite(); done <- struct{}{} }()
	go func() { _, _ = io.Copy(up, ch); done <- struct{}{} }()
	<-done
	_ = ch.Close()
	_ = up.Close()
}
