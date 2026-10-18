package main

import (
	"errors"
	"fmt"
	"slices"
	"unicode/utf8"
)

// Config is the connector configuration.
type Config struct {
	Host     string  `json:"host"`
	Port     int     `json:"port"`
	Database string  `json:"database"`
	Username string  `json:"username"`
	Password string  `json:"password"`
	SSLMode  SSLMode `json:"ssl_mode"`
}

// SSLMode is how the connection is encrypted, with the certificates it
// needs, all PEM.
type SSLMode struct {
	Mode              string `json:"mode"`
	CACertificate     string `json:"ca_certificate"`
	ClientCertificate string `json:"client_certificate"`
	ClientKey         string `json:"client_key"`
}

const (
	sslDisabled       = "disabled"
	sslPreferred      = "preferred"
	sslRequired       = "required"
	sslVerifyCA       = "verify_ca"
	sslVerifyIdentity = "verify_identity"
)

var sslModes = []string{sslDisabled, sslPreferred, sslRequired, sslVerifyCA, sslVerifyIdentity}

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.SSLMode.Mode == "" {
		c.SSLMode.Mode = sslPreferred
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
	if utf8.RuneCountInString(c.Database) > maxIdentLen {
		errs = append(errs, fmt.Errorf("database %q is longer than %d characters", c.Database, maxIdentLen))
	}
	switch {
	case !slices.Contains(sslModes, c.SSLMode.Mode):
		errs = append(errs, fmt.Errorf("ssl_mode %q is not one of %v", c.SSLMode.Mode, sslModes))
	case (c.SSLMode.Mode == sslVerifyCA || c.SSLMode.Mode == sslVerifyIdentity) && c.SSLMode.CACertificate == "":
		errs = append(errs, fmt.Errorf("ssl_mode %s needs ca_certificate", c.SSLMode.Mode))
	}
	if c.SSLMode.Mode == sslPreferred && c.SSLMode.CACertificate != "" {
		errs = append(errs, fmt.Errorf("ssl_mode %s does not use ca_certificate, since it may connect without encryption; use required, verify_ca or verify_identity", c.SSLMode.Mode))
	}
	if (c.SSLMode.ClientCertificate == "") != (c.SSLMode.ClientKey == "") {
		errs = append(errs, errors.New("client_certificate and client_key go together"))
	}
	return errors.Join(errs...)
}
