package main

import (
	"errors"
	"fmt"
	"slices"
)

// Config is the connector configuration. Its keys follow Airbyte's
// source-postgres so existing configurations carry over.
type Config struct {
	Host         string       `json:"host"`
	Port         int          `json:"port"`
	Database     string       `json:"database"`
	Schemas      []string     `json:"schemas"`
	Username     string       `json:"username"`
	Password     string       `json:"password"`
	SSLMode      SSLMode      `json:"ssl_mode"`
	TunnelMethod TunnelMethod `json:"tunnel_method"`
}

// SSLMode is the libpq sslmode with the certificates it needs, all PEM.
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

const (
	sslDisable    = "disable"
	sslAllow      = "allow"
	sslPrefer     = "prefer"
	sslRequire    = "require"
	sslVerifyCA   = "verify-ca"
	sslVerifyFull = "verify-full"

	tunnelNone     = "NO_TUNNEL"
	tunnelKey      = "SSH_KEY_AUTH"
	tunnelPassword = "SSH_PASSWORD_AUTH"
)

var sslModes = []string{sslDisable, sslAllow, sslPrefer, sslRequire, sslVerifyCA, sslVerifyFull}

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 5432
	}
	if len(c.Schemas) == 0 {
		c.Schemas = []string{"public"}
	}
	if c.SSLMode.Mode == "" {
		c.SSLMode.Mode = sslPrefer
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
	case (c.SSLMode.Mode == sslVerifyCA || c.SSLMode.Mode == sslVerifyFull) && c.SSLMode.CACertificate == "":
		errs = append(errs, fmt.Errorf("ssl_mode %s needs ca_certificate", c.SSLMode.Mode))
	}
	if (c.SSLMode.Mode == sslPrefer || c.SSLMode.Mode == sslAllow) && c.SSLMode.CACertificate != "" {
		errs = append(errs, fmt.Errorf("ssl_mode %s does not use ca_certificate, since it may connect without encryption; use require, verify-ca or verify-full", c.SSLMode.Mode))
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
	return errors.Join(errs...)
}
