package main

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"
)

const (
	dateLayout = "2006-01-02"
	// defaultThreshold is the share of the daily API request allocation, in
	// percent, at which a read stops.
	defaultThreshold = 90
)

// Config is the connector configuration.
type Config struct {
	ClientID       string   `json:"client_id"`
	ClientSecret   string   `json:"client_secret"`
	RefreshToken   string   `json:"refresh_token"`
	InstanceURL    string   `json:"instance_url"`
	IsSandbox      bool     `json:"is_sandbox"`
	StartDate      string   `json:"start_date,omitempty"`
	StreamsInclude []string `json:"streams_include,omitempty"`
	StreamsExclude []string `json:"streams_exclude,omitempty"`
	// APIUsageThresholdPercent is zero when unset, which means
	// defaultThreshold.
	APIUsageThresholdPercent int `json:"api_usage_threshold_percent,omitempty"`
}

func (c Config) validate() error {
	var errs []error
	if c.ClientID == "" {
		errs = append(errs, errors.New("client_id is required"))
	}
	if c.ClientSecret == "" {
		errs = append(errs, errors.New("client_secret is required"))
	}
	if c.RefreshToken == "" {
		errs = append(errs, errors.New("refresh_token is required"))
	}
	if _, err := c.instance(); err != nil {
		errs = append(errs, err)
	}
	if _, err := c.start(); err != nil {
		errs = append(errs, err)
	}
	if t := c.APIUsageThresholdPercent; t < 0 || t > 100 {
		errs = append(errs, fmt.Errorf("api_usage_threshold_percent %d must be between 1 and 100", t))
	}
	for _, p := range append(append([]string(nil), c.StreamsInclude...), c.StreamsExclude...) {
		if _, err := path.Match(p, ""); err != nil {
			errs = append(errs, fmt.Errorf("stream pattern %q is malformed: %w", p, err))
		}
	}
	return errors.Join(errs...)
}

// instance returns instance_url as scheme://host, without a trailing slash.
func (c Config) instance() (string, error) {
	if c.InstanceURL == "" {
		return "", errors.New("instance_url is required")
	}
	u, err := url.Parse(c.InstanceURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || strings.Trim(u.Path, "/") != "" || u.RawQuery != "" {
		return "", fmt.Errorf("instance_url %q must be the https URL of the org, such as https://acme.my.salesforce.com", c.InstanceURL)
	}
	return "https://" + strings.ToLower(u.Host), nil
}

// start returns start_date in UTC, or the zero time when it is not set; it
// is an RFC 3339 time or a YYYY-MM-DD day.
func (c Config) start() (time.Time, error) {
	if c.StartDate == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, c.StartDate); err == nil {
		return t.UTC(), nil
	}
	d, err := time.Parse(dateLayout, c.StartDate)
	if err != nil {
		return time.Time{}, fmt.Errorf("start_date %q is neither an RFC 3339 time nor a YYYY-MM-DD date", c.StartDate)
	}
	return d, nil
}

func (c Config) threshold() int {
	if c.APIUsageThresholdPercent == 0 {
		return defaultThreshold
	}
	return c.APIUsageThresholdPercent
}

// selected reports whether streams_include and streams_exclude keep the
// object name: it matches an include pattern, or there is none, and no
// exclude pattern.
func (c Config) selected(name string) bool {
	included := len(c.StreamsInclude) == 0
	for _, p := range c.StreamsInclude {
		if ok, _ := path.Match(p, name); ok {
			included = true
			break
		}
	}
	if !included {
		return false
	}
	for _, p := range c.StreamsExclude {
		if ok, _ := path.Match(p, name); ok {
			return false
		}
	}
	return true
}
