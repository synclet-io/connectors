package main

import (
	"encoding/json"
	"time"
)

// Config is the top-level MySQL source configuration.
type Config struct {
	Host        string            `json:"host"`
	Port        int               `json:"port"`
	Database    string            `json:"database"`
	Username    string            `json:"username"`
	Password    string            `json:"password"`
	SSL         SSLConfig         `json:"ssl"`
	Tunnel      TunnelConfig      `json:"tunnel"`
	Replication ReplicationConfig `json:"replication"`
	TableFilter TableFilter       `json:"table_filter"`
}

// SSLConfig holds SSL/TLS connection settings.
type SSLConfig struct {
	Mode       string `json:"mode"` // preferred, required, verify_ca, verify_identity
	CACert     string `json:"ca_certificate"`
	ClientCert string `json:"client_certificate"`
	ClientKey  string `json:"client_key"`
}

// TunnelConfig holds SSH tunnel settings.
type TunnelConfig struct {
	Method   string `json:"method"` // no_tunnel, ssh_key, ssh_password
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	SSHKey   string `json:"ssh_key"`
	Password string `json:"password"`
}

// CDCConfig holds CDC-specific settings.
type CDCConfig struct {
	ServerID           uint32        `json:"server_id"`
	InitialWaitSeconds int           `json:"initial_wait_seconds"`
	CheckpointInterval time.Duration `json:"-"`
}

// ReplicationConfig holds replication method settings.
type ReplicationConfig struct {
	Method string    `json:"method"` // STANDARD, CDC
	CDC    CDCConfig `json:"cdc"`
}

// TableFilter constrains which tables are discovered/read.
type TableFilter struct {
	SchemaPatterns []string `json:"schema_patterns"` // LIKE patterns for schemas/databases
	TablePatterns  []string `json:"table_patterns"`  // LIKE patterns for tables
}

// MaxPartitionWorkers is the max concurrent partition readers.
const MaxPartitionWorkers = 4

// DefaultChunkSize is the number of rows per PK-based chunk read.
const DefaultChunkSize = 10000

// DefaultCheckpointInterval is how often CDC emits state checkpoints.
const DefaultCheckpointInterval = 5 * time.Minute

// applyDefaults fills in zero-valued fields with sensible defaults.
func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.SSL.Mode == "" {
		c.SSL.Mode = "preferred"
	}
	if c.Tunnel.Method == "" {
		c.Tunnel.Method = "no_tunnel"
	}
	if c.Tunnel.Port == 0 {
		c.Tunnel.Port = 22
	}
	if c.Replication.Method == "" {
		c.Replication.Method = "STANDARD"
	}
	if c.Replication.CDC.CheckpointInterval == 0 {
		c.Replication.CDC.CheckpointInterval = DefaultCheckpointInterval
	}
}

// UnmarshalJSON applies defaults after unmarshalling.
func (c *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	raw := &struct {
		*Alias
		CDC struct {
			ServerID           uint32 `json:"server_id"`
			InitialWaitSeconds int    `json:"initial_wait_seconds"`
			CheckpointSeconds  int    `json:"checkpoint_seconds"`
		} `json:"cdc"`
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, raw); err != nil {
		return err
	}

	// Parse checkpoint_seconds from the nested replication.cdc if present
	if raw.CDC.CheckpointSeconds > 0 {
		c.Replication.CDC.CheckpointInterval = time.Duration(raw.CDC.CheckpointSeconds) * time.Second
	}

	c.applyDefaults()
	return nil
}
