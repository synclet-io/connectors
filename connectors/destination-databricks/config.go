package main

import (
	"errors"
	"fmt"
	"strings"
)

// Config is the connector configuration. Its keys follow Airbyte's
// destination-databricks where it has one, so existing configurations carry
// over; volume names the Unity Catalog volume batches are staged in.
type Config struct {
	Hostname       string `json:"hostname"`
	HTTPPath       string `json:"http_path"`
	Port           int    `json:"port"`
	Database       string `json:"database"`
	Schema         string `json:"schema"`
	Volume         string `json:"volume"`
	Authentication Auth   `json:"authentication"`
}

// Auth is a personal access token (BASIC) or an OAuth machine-to-machine
// service principal (OAUTH).
type Auth struct {
	AuthType            string `json:"auth_type"`
	PersonalAccessToken string `json:"personal_access_token"`
	ClientID            string `json:"client_id"`
	Secret              string `json:"secret"`
}

const (
	authPAT   = "BASIC"
	authOAuth = "OAUTH"
)

func (c *Config) applyDefaults() {
	c.Hostname = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(c.Hostname), "https://"), "/")
	if c.Port == 0 {
		c.Port = 443
	}
	if c.Schema == "" {
		c.Schema = "default"
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.Hostname == "" {
		errs = append(errs, errors.New("hostname is required"))
	} else if strings.ContainsAny(c.Hostname, "/?#@ ") {
		errs = append(errs, fmt.Errorf("hostname %q must be a bare host name, like dbc-1234.cloud.databricks.com", c.Hostname))
	}
	if c.HTTPPath == "" {
		errs = append(errs, errors.New("http_path is required"))
	}
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("port %d is out of range 1-65535", c.Port))
	}
	for _, f := range []struct{ key, val string }{{"database", c.Database}, {"schema", c.Schema}, {"volume", c.Volume}} {
		switch {
		case f.val == "":
			errs = append(errs, fmt.Errorf("%s is required", f.key))
		case strings.ContainsAny(f.val, "./ \x00") || strings.ContainsFunc(f.val, isControl):
			errs = append(errs, fmt.Errorf("%s %q contains a period, slash, space or control character, which Unity Catalog names cannot hold", f.key, f.val))
		}
	}
	switch a := c.Authentication; a.AuthType {
	case authPAT:
		if a.PersonalAccessToken == "" {
			errs = append(errs, errors.New("authentication BASIC needs personal_access_token"))
		}
	case authOAuth:
		if a.ClientID == "" || a.Secret == "" {
			errs = append(errs, errors.New("authentication OAUTH needs client_id and secret"))
		}
	default:
		errs = append(errs, fmt.Errorf("authentication auth_type %q is not one of %s, %s", a.AuthType, authPAT, authOAuth))
	}
	return errors.Join(errs...)
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }

// baseURL is the workspace's HTTPS address.
func (c *Config) baseURL() string {
	if c.Port == 443 {
		return "https://" + c.Hostname
	}
	return fmt.Sprintf("https://%s:%d", c.Hostname, c.Port)
}

// volumePath is the Files API path of the staging volume.
func (c *Config) volumePath() string {
	return "/Volumes/" + c.Database + "/" + c.Schema + "/" + c.Volume
}
