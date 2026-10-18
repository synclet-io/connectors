package main

import (
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Config is the connector configuration. Its keys follow Airbyte's
// source-mongodb-v2 so existing configurations carry over.
type Config struct {
	DatabaseConfig     DatabaseConfig `json:"database_config"`
	DiscoverSampleSize int            `json:"discover_sample_size"`
	TLSCACertificate   string         `json:"tls_ca_certificate"`
	TunnelMethod       TunnelMethod   `json:"tunnel_method"`
}

// DatabaseConfig names the cluster, the database and the login.
// ConnectionString is a mongodb:// URI (several hosts separated by commas)
// or a mongodb+srv:// URI.
type DatabaseConfig struct {
	ClusterType      string `json:"cluster_type"`
	ConnectionString string `json:"connection_string"`
	Database         string `json:"database"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	AuthSource       string `json:"auth_source"`
}

// TunnelMethod is how the connector reaches the servers: directly or
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

const (
	clusterAtlas       = "ATLAS_REPLICA_SET"
	clusterSelfManaged = "SELF_MANAGED_REPLICA_SET"

	defaultSampleSize = 10_000
	maxSampleSize     = 100_000

	tunnelNone     = "NO_TUNNEL"
	tunnelKey      = "SSH_KEY_AUTH"
	tunnelPassword = "SSH_PASSWORD_AUTH"
)

var clusterTypes = []string{clusterAtlas, clusterSelfManaged}

func (c *Config) applyDefaults() {
	if c.DatabaseConfig.ClusterType == "" {
		c.DatabaseConfig.ClusterType = clusterSelfManaged
	}
	if c.DiscoverSampleSize == 0 {
		c.DiscoverSampleSize = defaultSampleSize
	}
	if c.TunnelMethod.TunnelMethod == "" {
		c.TunnelMethod.TunnelMethod = tunnelNone
	}
	if c.TunnelMethod.TunnelPort == 0 {
		c.TunnelMethod.TunnelPort = 22
	}
}

func (c *Config) validate() error {
	var errs []error
	d := c.DatabaseConfig
	if !slices.Contains(clusterTypes, d.ClusterType) {
		errs = append(errs, fmt.Errorf("database_config.cluster_type %q is not one of %v", d.ClusterType, clusterTypes))
	}
	switch {
	case d.ConnectionString == "":
		errs = append(errs, errors.New("database_config.connection_string is required"))
	case !strings.HasPrefix(d.ConnectionString, schemeStandard) && !strings.HasPrefix(d.ConnectionString, schemeSRV):
		errs = append(errs, errors.New("database_config.connection_string must start with mongodb:// or mongodb+srv://"))
	}
	if d.Database == "" {
		errs = append(errs, errors.New("database_config.database is required"))
	}
	if d.Password != "" && d.Username == "" {
		errs = append(errs, errors.New("database_config.password is set without database_config.username"))
	}
	if c.DiscoverSampleSize < 1 || c.DiscoverSampleSize > maxSampleSize {
		errs = append(errs, fmt.Errorf("discover_sample_size %d is out of range 1-%d", c.DiscoverSampleSize, maxSampleSize))
	}
	if c.TLSCACertificate != "" && !x509.NewCertPool().AppendCertsFromPEM([]byte(c.TLSCACertificate)) {
		errs = append(errs, errors.New("tls_ca_certificate holds no PEM certificate"))
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
		if t.TunnelPort < 1 || t.TunnelPort > 65535 {
			errs = append(errs, fmt.Errorf("tunnel_port %d is out of range 1-65535", t.TunnelPort))
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
	return errors.Join(errs...)
}
