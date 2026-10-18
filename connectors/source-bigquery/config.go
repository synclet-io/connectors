package main

import (
	"errors"
	"fmt"
)

// Config is the connector configuration.
type Config struct {
	ProjectID   string            `json:"project_id"`
	Datasets    []string          `json:"datasets"`
	Location    string            `json:"location"`
	Credentials CredentialsConfig `json:"credentials"`
}

// CredentialsConfig is how the connector authenticates to Google Cloud.
type CredentialsConfig struct {
	AuthType           string `json:"auth_type"`
	ServiceAccountInfo string `json:"service_account_info"`
}

const (
	authService            = "Service"
	authApplicationDefault = "ApplicationDefault"
)

func (c *Config) applyDefaults() {
	if c.Credentials.AuthType == "" {
		c.Credentials.AuthType = authApplicationDefault
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.ProjectID == "" {
		errs = append(errs, errors.New("project_id is required"))
	}
	for _, d := range c.Datasets {
		if d == "" {
			errs = append(errs, errors.New("datasets may not contain an empty name"))
			break
		}
	}
	switch c.Credentials.AuthType {
	case authService:
		if c.Credentials.ServiceAccountInfo == "" {
			errs = append(errs, errors.New("service_account_info is required for auth_type Service"))
		}
	case authApplicationDefault:
	default:
		errs = append(errs, fmt.Errorf("auth_type %q is not one of %s, %s", c.Credentials.AuthType, authService, authApplicationDefault))
	}
	return errors.Join(errs...)
}
