package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Config is the connector configuration. Its keys follow Airbyte's
// source-mysql; the keys of earlier versions of this connector (ssl, tunnel,
// replication) are read as aliases.
type Config struct {
	Host              string            `json:"host"`
	Port              int               `json:"port"`
	Database          string            `json:"database"`
	Username          string            `json:"username"`
	Password          string            `json:"password"`
	SSLMode           SSLMode           `json:"ssl_mode"`
	TunnelMethod      TunnelMethod      `json:"tunnel_method"`
	ReplicationMethod ReplicationMethod `json:"replication_method"`
	TableFilter       TableFilter       `json:"table_filter"`
	ZeroDateHandling  string            `json:"zero_date_handling"`
}

// SSLMode is how the connection is encrypted, with the certificates it
// needs, all PEM.
type SSLMode struct {
	Mode              string `json:"mode"`
	CACertificate     string `json:"ca_certificate"`
	ClientCertificate string `json:"client_certificate"`
	ClientKey         string `json:"client_key"`
}

// TunnelMethod is how the connector reaches the database: directly or
// through an SSH bastion.
type TunnelMethod struct {
	TunnelMethod       string `json:"tunnel_method"`
	TunnelHost         string `json:"tunnel_host"`
	TunnelPort         int    `json:"tunnel_port"`
	TunnelUser         string `json:"tunnel_user"`
	SSHKey             string `json:"ssh_key"`
	TunnelUserPassword string `json:"tunnel_user_password"`
	TunnelHostKey      string `json:"tunnel_host_key"`
}

// ReplicationMethod is how rows are read: by SELECT queries or from the
// binlog.
type ReplicationMethod struct {
	Method                string `json:"method"`
	ServerID              uint32 `json:"server_id"`
	InitialWaitingSeconds int    `json:"initial_waiting_seconds"`
	CheckpointSeconds     int    `json:"checkpoint_seconds"`
}

// TableFilter constrains which tables are discovered.
type TableFilter struct {
	SchemaPatterns []string `json:"schema_patterns"`
	TablePatterns  []string `json:"table_patterns"`
}

const (
	sslPreferred      = "preferred"
	sslRequired       = "required"
	sslVerifyCA       = "verify_ca"
	sslVerifyIdentity = "verify_identity"

	tunnelNone     = "NO_TUNNEL"
	tunnelKey      = "SSH_KEY_AUTH"
	tunnelPassword = "SSH_PASSWORD_AUTH"

	replicationStandard = "STANDARD"
	replicationCDC      = "CDC"

	zeroDateNull = "null"
	zeroDateFail = "fail"

	defaultCheckpointSeconds = 300
)

var (
	sslModes          = []string{sslPreferred, sslRequired, sslVerifyCA, sslVerifyIdentity}
	zeroDateHandlings = []string{zeroDateNull, zeroDateFail}
	legacyTunnels     = map[string]string{"no_tunnel": tunnelNone, "ssh_key": tunnelKey, "ssh_password": tunnelPassword}
)

// UnmarshalJSON reads the configuration, taking the keys of earlier versions
// where the Airbyte key is absent: ssl for ssl_mode, tunnel (method, host,
// port, user, ssh_key, password) for tunnel_method and replication (method,
// cdc.server_id, cdc.initial_wait_seconds, cdc.checkpoint_seconds) for
// replication_method.
func (c *Config) UnmarshalJSON(data []byte) error {
	type plain Config
	var raw struct {
		plain
		SSL         *SSLMode           `json:"ssl"`
		Tunnel      *legacyTunnel      `json:"tunnel"`
		Replication *legacyReplication `json:"replication"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	*c = Config(raw.plain)
	if _, ok := keys["ssl_mode"]; !ok && raw.SSL != nil {
		c.SSLMode = *raw.SSL
	}
	if _, ok := keys["tunnel_method"]; !ok && raw.Tunnel != nil {
		c.TunnelMethod = raw.Tunnel.tunnelMethod()
	}
	if _, ok := keys["replication_method"]; !ok && raw.Replication != nil {
		c.ReplicationMethod = raw.Replication.replicationMethod()
	}
	return nil
}

// UnmarshalJSON also reads method for tunnel_method, with the values
// no_tunnel, ssh_key and ssh_password, and tunnel_password for
// tunnel_user_password, as spec.json of earlier versions named them.
func (t *TunnelMethod) UnmarshalJSON(data []byte) error {
	type plain TunnelMethod
	var raw struct {
		plain
		Method         string `json:"method"`
		TunnelPassword string `json:"tunnel_password"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*t = TunnelMethod(raw.plain)
	if t.TunnelMethod == "" {
		t.TunnelMethod = raw.Method
	}
	if m, ok := legacyTunnels[t.TunnelMethod]; ok {
		t.TunnelMethod = m
	}
	if t.TunnelUserPassword == "" {
		t.TunnelUserPassword = raw.TunnelPassword
	}
	return nil
}

// UnmarshalJSON also reads initial_wait_seconds for initial_waiting_seconds.
func (r *ReplicationMethod) UnmarshalJSON(data []byte) error {
	type plain ReplicationMethod
	var raw struct {
		plain
		InitialWaitSeconds int `json:"initial_wait_seconds"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*r = ReplicationMethod(raw.plain)
	if r.InitialWaitingSeconds == 0 {
		r.InitialWaitingSeconds = raw.InitialWaitSeconds
	}
	return nil
}

type legacyTunnel struct {
	Method   string `json:"method"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	SSHKey   string `json:"ssh_key"`
	Password string `json:"password"`
}

func (l legacyTunnel) tunnelMethod() TunnelMethod {
	method := l.Method
	if m, ok := legacyTunnels[method]; ok {
		method = m
	}
	return TunnelMethod{
		TunnelMethod:       method,
		TunnelHost:         l.Host,
		TunnelPort:         l.Port,
		TunnelUser:         l.User,
		SSHKey:             l.SSHKey,
		TunnelUserPassword: l.Password,
	}
}

type legacyReplication struct {
	Method string `json:"method"`
	CDC    struct {
		ServerID           uint32 `json:"server_id"`
		InitialWaitSeconds int    `json:"initial_wait_seconds"`
		CheckpointSeconds  int    `json:"checkpoint_seconds"`
	} `json:"cdc"`
}

func (l legacyReplication) replicationMethod() ReplicationMethod {
	return ReplicationMethod{
		Method:                l.Method,
		ServerID:              l.CDC.ServerID,
		InitialWaitingSeconds: l.CDC.InitialWaitSeconds,
		CheckpointSeconds:     l.CDC.CheckpointSeconds,
	}
}

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.SSLMode.Mode == "" {
		c.SSLMode.Mode = sslPreferred
	}
	if c.TunnelMethod.TunnelMethod == "" {
		c.TunnelMethod.TunnelMethod = tunnelNone
	}
	if c.TunnelMethod.TunnelPort == 0 {
		c.TunnelMethod.TunnelPort = 22
	}
	if c.ReplicationMethod.Method == "" {
		c.ReplicationMethod.Method = replicationStandard
	}
	if c.ReplicationMethod.CheckpointSeconds == 0 {
		c.ReplicationMethod.CheckpointSeconds = defaultCheckpointSeconds
	}
	if c.ZeroDateHandling == "" {
		c.ZeroDateHandling = zeroDateNull
	}
}

func (c *Config) cdc() bool { return c.ReplicationMethod.Method == replicationCDC }

func (c *Config) checkpointInterval() time.Duration {
	return time.Duration(c.ReplicationMethod.CheckpointSeconds) * time.Second
}

func (c *Config) validate() error {
	var errs []error
	if c.Host == "" {
		errs = append(errs, errors.New("host is required"))
	}
	if c.Database == "" {
		errs = append(errs, errors.New("database is required"))
	}
	if c.Username == "" {
		errs = append(errs, errors.New("username is required"))
	}
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("port %d is out of range 1-65535", c.Port))
	}
	switch {
	case !slices.Contains(sslModes, c.SSLMode.Mode):
		errs = append(errs, fmt.Errorf("ssl_mode %q is not one of %v", c.SSLMode.Mode, sslModes))
	case (c.SSLMode.Mode == sslVerifyCA || c.SSLMode.Mode == sslVerifyIdentity) && c.SSLMode.CACertificate == "":
		errs = append(errs, fmt.Errorf("ssl_mode %s needs ca_certificate", c.SSLMode.Mode))
	case c.SSLMode.Mode == sslPreferred && c.SSLMode.CACertificate != "":
		errs = append(errs, errors.New("ssl_mode preferred does not use ca_certificate, since it may connect without encryption; use required, verify_ca or verify_identity"))
	}
	if (c.SSLMode.ClientCertificate == "") != (c.SSLMode.ClientKey == "") {
		errs = append(errs, errors.New("client_certificate and client_key go together"))
	}
	t := c.TunnelMethod
	switch t.TunnelMethod {
	case tunnelNone:
	case tunnelKey, tunnelPassword:
		if t.TunnelHost == "" {
			errs = append(errs, errors.New("tunnel_host is required"))
		}
		if t.TunnelUser == "" {
			errs = append(errs, errors.New("tunnel_user is required"))
		}
		if t.TunnelMethod == tunnelKey && t.SSHKey == "" {
			errs = append(errs, errors.New("ssh_key is required for SSH_KEY_AUTH"))
		}
		if t.TunnelMethod == tunnelPassword && t.TunnelUserPassword == "" {
			errs = append(errs, errors.New("tunnel_user_password is required for SSH_PASSWORD_AUTH"))
		}
		if t.TunnelHostKey != "" {
			if _, err := parseHostKeys(t.TunnelHostKey); err != nil {
				errs = append(errs, err)
			}
		}
	default:
		errs = append(errs, fmt.Errorf("tunnel_method %q is not one of NO_TUNNEL, SSH_KEY_AUTH, SSH_PASSWORD_AUTH", t.TunnelMethod))
	}
	switch c.ReplicationMethod.Method {
	case replicationStandard, replicationCDC:
	default:
		errs = append(errs, fmt.Errorf("replication_method %q is not one of STANDARD, CDC", c.ReplicationMethod.Method))
	}
	if c.ReplicationMethod.CheckpointSeconds < 0 {
		errs = append(errs, fmt.Errorf("checkpoint_seconds %d is negative", c.ReplicationMethod.CheckpointSeconds))
	}
	if !slices.Contains(zeroDateHandlings, c.ZeroDateHandling) {
		errs = append(errs, fmt.Errorf("zero_date_handling %q is not one of %v", c.ZeroDateHandling, zeroDateHandlings))
	}
	return errors.Join(errs...)
}
