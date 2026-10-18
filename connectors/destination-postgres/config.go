package main

import (
	"errors"
	"fmt"
	"slices"
)

// Config is the connector configuration. Its keys follow Airbyte's
// destination-postgres so existing configurations carry over.
type Config struct {
	Host     string  `json:"host"`
	Port     int     `json:"port"`
	Database string  `json:"database"`
	Schema   string  `json:"schema"`
	Username string  `json:"username"`
	Password string  `json:"password"`
	SSLMode  SSLMode `json:"ssl_mode"`
}

// SSLMode is the libpq sslmode with the certificates it needs, all PEM.
type SSLMode struct {
	Mode              string `json:"mode"`
	CACertificate     string `json:"ca_certificate"`
	ClientCertificate string `json:"client_certificate"`
	ClientKey         string `json:"client_key"`
}

const (
	sslDisable    = "disable"
	sslAllow      = "allow"
	sslPrefer     = "prefer"
	sslRequire    = "require"
	sslVerifyCA   = "verify-ca"
	sslVerifyFull = "verify-full"
)

var sslModes = []string{sslDisable, sslAllow, sslPrefer, sslRequire, sslVerifyCA, sslVerifyFull}

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.Schema == "" {
		c.Schema = "public"
	}
	if c.SSLMode.Mode == "" {
		c.SSLMode.Mode = sslPrefer
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
	if len(c.Schema) > maxIdentLen {
		errs = append(errs, fmt.Errorf("schema %q is longer than %d bytes", c.Schema, maxIdentLen))
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
	return errors.Join(errs...)
}
