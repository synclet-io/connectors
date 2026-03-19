//go:build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testHost     = "127.0.0.1"
	testPort     = 3307
	testDB       = "testdb"
	testUser     = "root"
	testPassword = "testpassword"
)

func testConfig() Config {
	cfg := Config{
		Host:     testHost,
		Port:     testPort,
		Database: testDB,
		Username: testUser,
		Password: testPassword,
	}
	cfg.applyDefaults()
	return cfg
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", testUser, testPassword, testHost, testPort, testDB)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for MySQL to be ready
	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	require.NoError(t, db.PingContext(ctx))

	return db
}

func createTestTable(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec(`DROP TABLE IF EXISTS test_users`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE test_users (
		id INT AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		email VARCHAR(255),
		age INT,
		salary DECIMAL(10,2),
		active TINYINT(1) DEFAULT 1,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_users (name, email, age, salary) VALUES
		('Alice', 'alice@example.com', 30, 50000.50),
		('Bob', 'bob@example.com', 25, 60000.00),
		('Charlie', 'charlie@example.com', 35, 75000.25)`)
	require.NoError(t, err)
}

func writeTestConfig(t *testing.T, cfg Config) string {
	t.Helper()

	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	f, err := os.CreateTemp("", "mysql-config-*.json")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(f.Name()) })

	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return f.Name()
}

func TestIntegrationCheck(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := testConfig()
	cfgPath := writeTestConfig(t, cfg)

	source := NewMySQLSource()
	err := source.Check(cfgPath, newTestLogTracker())
	assert.NoError(t, err)
}

func TestIntegrationDiscover(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	createTestTable(t, db)

	cfg := testConfig()
	cfgPath := writeTestConfig(t, cfg)

	source := NewMySQLSource()
	catalog, err := source.Discover(cfgPath, newTestLogTracker())
	require.NoError(t, err)
	require.NotNil(t, catalog)

	// Find test_users stream
	var found bool
	for _, s := range catalog.Streams {
		if s.Name == "test_users" {
			found = true
			assert.NotEmpty(t, s.JSONSchema.Properties)
			assert.Contains(t, s.SupportedSyncModes, "full_refresh")
			assert.NotEmpty(t, s.SourceDefinedPrimaryKey)
			break
		}
	}
	assert.True(t, found, "test_users stream not found in catalog")
}

func TestIntegrationFullRefreshRead(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	createTestTable(t, db)

	cfg := testConfig()
	cfgPath := writeTestConfig(t, cfg)

	source := NewMySQLSource()
	catalog, err := source.Discover(cfgPath, newTestLogTracker())
	require.NoError(t, err)

	// Build configured catalog for full refresh
	configuredCatPath := writeConfiguredCatalog(t, catalog, "test_users", "full_refresh")

	tracker := newTestTracker()
	err = source.Read(cfgPath, "", configuredCatFromPath(t, configuredCatPath), tracker)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(tracker.records), 3)
}

func TestIntegrationCDCCheck(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	cfg := testConfig()
	cfg.Replication.Method = "CDC"
	cfgPath := writeTestConfig(t, cfg)

	source := NewMySQLSource()
	err := source.Check(cfgPath, newTestLogTracker())
	assert.NoError(t, err)
}

// Test helpers

type testLogTracker struct{}

func newTestLogTracker() testLogTracker { return testLogTracker{} }
func (testLogTracker) Log(level, msg string) error { return nil }

type testTracker struct {
	records []map[string]interface{}
	states  []interface{}
	logs    []string
}

func newTestTracker() *testTracker { return &testTracker{} }

func (t *testTracker) asMessageTracker() messageTrackerAdapter { return messageTrackerAdapter{t} }

type messageTrackerAdapter struct{ t *testTracker }

func writeConfiguredCatalog(t *testing.T, catalog interface{}, streamName, syncMode string) string {
	t.Helper()

	// Minimal configured catalog
	data := fmt.Sprintf(`{"streams":[{"stream":{"name":"%s","namespace":"%s"},"sync_mode":"%s","destination_sync_mode":"overwrite"}]}`,
		streamName, testDB, syncMode)

	f, err := os.CreateTemp("", "catalog-*.json")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(f.Name()) })

	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return f.Name()
}

func configuredCatFromPath(t *testing.T, path string) *configuredCatalogWrapper {
	t.Helper()
	// Read and parse the configured catalog
	// For integration tests, we build it inline
	return nil // Placeholder - actual integration tests use the real SDK
}

type configuredCatalogWrapper struct{}
