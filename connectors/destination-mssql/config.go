package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Config is the connector configuration. Its keys follow Airbyte's
// destination-mssql so existing configurations carry over.
type Config struct {
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	Database  string    `json:"database"`
	Schema    string    `json:"schema"`
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	SSLMethod SSLMethod `json:"ssl_method"`
}

// SSLMethod is how the connection is encrypted and how the server
// certificate is checked. Certificate is a CA certificate in PEM.
type SSLMethod struct {
	SSLMethod             string `json:"ssl_method"`
	HostNameInCertificate string `json:"hostNameInCertificate"`
	Certificate           string `json:"certificate"`
}

const (
	sslUnencrypted = "unencrypted"
	sslTrust       = "encrypted_trust_server_certificate"
	sslVerify      = "encrypted_verify_certificate"
)

var sslMethods = []string{sslUnencrypted, sslTrust, sslVerify}

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 1433
	}
	if c.Schema == "" {
		c.Schema = "dbo"
	}
	if c.SSLMethod.SSLMethod == "" {
		c.SSLMethod.SSLMethod = sslTrust
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
	if strings.Contains(c.Username, `\`) {
		errs = append(errs, fmt.Errorf(`username %q is a Windows account (DOMAIN\user); only SQL Server logins are supported`, c.Username))
	}
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("port %d is out of range 1-65535", c.Port))
	}
	if err := checkName("schema", c.Schema); err != nil {
		errs = append(errs, err)
	} else if identLen(c.Schema) > maxIdentLen {
		errs = append(errs, fmt.Errorf("schema %q is longer than %d characters", c.Schema, maxIdentLen))
	}
	s := c.SSLMethod
	switch {
	case !slices.Contains(sslMethods, s.SSLMethod):
		errs = append(errs, fmt.Errorf("ssl_method %q is not one of %v", s.SSLMethod, sslMethods))
	case s.SSLMethod != sslVerify && (s.Certificate != "" || s.HostNameInCertificate != ""):
		errs = append(errs, fmt.Errorf("ssl_method %s does not check the server certificate, so it takes no certificate or hostNameInCertificate; use %s", s.SSLMethod, sslVerify))
	}
	return errors.Join(errs...)
}
