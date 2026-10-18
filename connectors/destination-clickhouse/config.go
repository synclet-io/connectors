package main

import (
	"errors"
	"fmt"
	"slices"
)

// Config is the connector configuration.
type Config struct {
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Protocol      string `json:"protocol"`
	Database      string `json:"database"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	TLS           TLS    `json:"tls"`
	OptimizeFinal bool   `json:"optimize_final"`
}

// TLS encrypts the connection; the certificates are PEM.
type TLS struct {
	Enabled           bool   `json:"enabled"`
	CACertificate     string `json:"ca_certificate"`
	ClientCertificate string `json:"client_certificate"`
	ClientKey         string `json:"client_key"`
}

const (
	protocolNative = "native"
	protocolHTTP   = "http"
)

var protocols = []string{protocolNative, protocolHTTP}

// defaultPort is ClickHouse's port for a protocol, with or without TLS.
func defaultPort(protocol string, tls bool) int {
	switch {
	case protocol == protocolHTTP && tls:
		return 8443
	case protocol == protocolHTTP:
		return 8123
	case tls:
		return 9440
	default:
		return 9000
	}
}

func (c *Config) applyDefaults() {
	if c.Protocol == "" {
		c.Protocol = protocolNative
	}
	if c.Port == 0 {
		c.Port = defaultPort(c.Protocol, c.TLS.Enabled)
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
	if !slices.Contains(protocols, c.Protocol) {
		errs = append(errs, fmt.Errorf("protocol %q is not one of %v", c.Protocol, protocols))
	}
	if !c.TLS.Enabled && (c.TLS.CACertificate != "" || c.TLS.ClientCertificate != "" || c.TLS.ClientKey != "") {
		errs = append(errs, errors.New("tls certificates are set but tls.enabled is false"))
	}
	if (c.TLS.ClientCertificate == "") != (c.TLS.ClientKey == "") {
		errs = append(errs, errors.New("client_certificate and client_key go together"))
	}
	return errors.Join(errs...)
}
