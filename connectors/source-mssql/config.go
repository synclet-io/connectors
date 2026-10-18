package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Config is the connector configuration. Its keys follow Airbyte's
// source-mssql so existing configurations carry over.
type Config struct {
	Host           string       `json:"host"`
	Port           int          `json:"port"`
	Database       string       `json:"database"`
	Schemas        []string     `json:"schemas"`
	Username       string       `json:"username"`
	Password       string       `json:"password"`
	Authentication string       `json:"authentication"`
	SSLMethod      SSLMethod    `json:"ssl_method"`
	TunnelMethod   TunnelMethod `json:"tunnel_method"`
}

// SSLMethod is how the connection is encrypted and how the server
// certificate is checked. Certificate is a CA certificate in PEM.
type SSLMethod struct {
	SSLMethod             string `json:"ssl_method"`
	HostNameInCertificate string `json:"hostNameInCertificate"`
	Certificate           string `json:"certificate"`
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
	authSQL  = "SQL"
	authNTLM = "NTLM"

	sslUnencrypted = "unencrypted"
	sslTrust       = "encrypted_trust_server_certificate"
	sslVerify      = "encrypted_verify_certificate"

	tunnelNone     = "NO_TUNNEL"
	tunnelKey      = "SSH_KEY_AUTH"
	tunnelPassword = "SSH_PASSWORD_AUTH"
)

var (
	authMethods = []string{authSQL, authNTLM}
	sslMethods  = []string{sslUnencrypted, sslTrust, sslVerify}
)

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 1433
	}
	if len(c.Schemas) == 0 {
		c.Schemas = []string{"dbo"}
	}
	if c.Authentication == "" {
		c.Authentication = authSQL
	}
	if c.SSLMethod.SSLMethod == "" {
		c.SSLMethod.SSLMethod = sslTrust
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
	switch c.Authentication {
	case authSQL:
		if strings.Contains(c.Username, `\`) {
			errs = append(errs, fmt.Errorf(`username %q is a Windows account (DOMAIN\user); set authentication to NTLM`, c.Username))
		}
	case authNTLM:
		domain, user, ok := strings.Cut(c.Username, `\`)
		if c.Username != "" && (!ok || domain == "" || user == "") {
			errs = append(errs, fmt.Errorf(`authentication NTLM needs username as DOMAIN\user, got %q`, c.Username))
		}
	default:
		errs = append(errs, fmt.Errorf("authentication %q is not one of %v", c.Authentication, authMethods))
	}
	s := c.SSLMethod
	switch {
	case !slices.Contains(sslMethods, s.SSLMethod):
		errs = append(errs, fmt.Errorf("ssl_method %q is not one of %v", s.SSLMethod, sslMethods))
	case s.SSLMethod != sslVerify && (s.Certificate != "" || s.HostNameInCertificate != ""):
		errs = append(errs, fmt.Errorf("ssl_method %s does not check the server certificate, so it takes no certificate or hostNameInCertificate; use %s", s.SSLMethod, sslVerify))
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
