package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigUnmarshalDefaults(t *testing.T) {
	raw := `{"host":"localhost","database":"testdb","username":"root","password":"secret"}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	assert.Equal(t, "localhost", cfg.Host)
	assert.Equal(t, 3306, cfg.Port)
	assert.Equal(t, "testdb", cfg.Database)
	assert.Equal(t, "root", cfg.Username)
	assert.Equal(t, "secret", cfg.Password)
	assert.Equal(t, "preferred", cfg.SSL.Mode)
	assert.Equal(t, "no_tunnel", cfg.Tunnel.Method)
	assert.Equal(t, 22, cfg.Tunnel.Port)
	assert.Equal(t, "STANDARD", cfg.Replication.Method)
	assert.Equal(t, DefaultCheckpointInterval, cfg.Replication.CDC.CheckpointInterval)
}

func TestConfigUnmarshalFull(t *testing.T) {
	raw := `{
		"host": "db.example.com",
		"port": 3307,
		"database": "production",
		"username": "admin",
		"password": "pass123",
		"ssl": {"mode": "verify_ca", "ca_certificate": "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"},
		"tunnel": {"method": "ssh_key", "host": "bastion.example.com", "port": 2222, "user": "deploy", "ssh_key": "key_data"},
		"replication": {"method": "CDC", "cdc": {"server_id": 5500, "initial_wait_seconds": 600}},
		"table_filter": {"table_patterns": ["users%", "orders%"]}
	}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	assert.Equal(t, "db.example.com", cfg.Host)
	assert.Equal(t, 3307, cfg.Port)
	assert.Equal(t, "verify_ca", cfg.SSL.Mode)
	assert.Contains(t, cfg.SSL.CACert, "BEGIN CERTIFICATE")
	assert.Equal(t, "ssh_key", cfg.Tunnel.Method)
	assert.Equal(t, "bastion.example.com", cfg.Tunnel.Host)
	assert.Equal(t, 2222, cfg.Tunnel.Port)
	assert.Equal(t, "deploy", cfg.Tunnel.User)
	assert.Equal(t, "CDC", cfg.Replication.Method)
	assert.Equal(t, uint32(5500), cfg.Replication.CDC.ServerID)
	assert.Equal(t, 600, cfg.Replication.CDC.InitialWaitSeconds)
	assert.Equal(t, []string{"users%", "orders%"}, cfg.TableFilter.TablePatterns)
}

func TestConfigApplyDefaults(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()

	assert.Equal(t, 3306, cfg.Port)
	assert.Equal(t, "preferred", cfg.SSL.Mode)
	assert.Equal(t, "no_tunnel", cfg.Tunnel.Method)
	assert.Equal(t, 22, cfg.Tunnel.Port)
	assert.Equal(t, "STANDARD", cfg.Replication.Method)
	assert.Equal(t, 5*time.Minute, cfg.Replication.CDC.CheckpointInterval)
}
