package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	dateLayout          = "2006-01-02"
	defaultLookbackDays = 3
	authServiceAccount  = "service_account"
	authOAuth           = "oauth"
)

// Credentials is one of two ways to authenticate: a service account key, or
// an OAuth client with a refresh token of a Google user.
type Credentials struct {
	AuthType          string `json:"auth_type"`
	ServiceAccountKey string `json:"service_account_key"`
	ClientID          string `json:"client_id"`
	ClientSecret      string `json:"client_secret"`
	RefreshToken      string `json:"refresh_token"`
}

// CustomReport is a report read as its own stream.
type CustomReport struct {
	Name       string   `json:"name"`
	Dimensions []string `json:"dimensions"`
	Metrics    []string `json:"metrics"`
}

// Config is the connector configuration.
type Config struct {
	Credentials       Credentials    `json:"credentials"`
	PropertyIDs       []string       `json:"property_ids"`
	StartDate         string         `json:"start_date"`
	EndDate           string         `json:"end_date"`
	LookbackDays      *int           `json:"lookback_days"`
	CustomReports     []CustomReport `json:"custom_reports"`
	DataQualityFields *bool          `json:"data_quality_fields"`
}

func (c Config) validate() error {
	var errs []error
	if err := c.Credentials.validate(); err != nil {
		errs = append(errs, err)
	}
	if len(c.PropertyIDs) == 0 {
		errs = append(errs, errors.New("property_ids needs at least one property id"))
	}
	seen := map[string]bool{}
	for _, id := range c.PropertyIDs {
		p := propertyID(id)
		if !isDigits(p) {
			errs = append(errs, fmt.Errorf("property id %q is not a number, with or without the properties/ prefix", id))
			continue
		}
		if seen[p] {
			errs = append(errs, fmt.Errorf("property id %s is listed twice", p))
		}
		seen[p] = true
	}
	start, err := c.start()
	if err != nil {
		errs = append(errs, err)
	}
	end, err := c.end()
	switch {
	case err != nil:
		errs = append(errs, err)
	case !end.IsZero() && !start.IsZero() && end.Before(start):
		errs = append(errs, fmt.Errorf("end_date %s is before start_date %s", c.EndDate, c.StartDate))
	}
	if c.LookbackDays != nil && *c.LookbackDays < 0 {
		errs = append(errs, fmt.Errorf("lookback_days %d is negative", *c.LookbackDays))
	}
	if _, err := reportDefs(c); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (c Credentials) validate() error {
	switch c.AuthType {
	case authServiceAccount:
		if strings.TrimSpace(c.ServiceAccountKey) == "" {
			return errors.New("credentials.service_account_key is required")
		}
		_, err := parseServiceAccountKey(c.ServiceAccountKey)
		return err
	case authOAuth:
		var errs []error
		for _, kv := range [][2]string{
			{"client_id", c.ClientID},
			{"client_secret", c.ClientSecret},
			{"refresh_token", c.RefreshToken},
		} {
			if strings.TrimSpace(kv[1]) == "" {
				errs = append(errs, fmt.Errorf("credentials.%s is required", kv[0]))
			}
		}
		return errors.Join(errs...)
	case "":
		return errors.New("credentials.auth_type is required")
	}
	return fmt.Errorf("credentials.auth_type %q is neither %s nor %s", c.AuthType, authServiceAccount, authOAuth)
}

// properties returns the configured property ids without the properties/
// prefix.
func (c Config) properties() []string {
	out := make([]string, len(c.PropertyIDs))
	for i, id := range c.PropertyIDs {
		out[i] = propertyID(id)
	}
	return out
}

func (c Config) lookback() int {
	if c.LookbackDays == nil {
		return defaultLookbackDays
	}
	return *c.LookbackDays
}

func (c Config) dataQuality() bool { return c.DataQualityFields == nil || *c.DataQualityFields }

// start returns start_date as a UTC day.
func (c Config) start() (time.Time, error) {
	if c.StartDate == "" {
		return time.Time{}, errors.New("start_date is required")
	}
	return parseDate("start_date", c.StartDate)
}

// end returns end_date as a UTC day, or the zero time when it is not set.
func (c Config) end() (time.Time, error) {
	if c.EndDate == "" {
		return time.Time{}, nil
	}
	return parseDate("end_date", c.EndDate)
}

func parseDate(key, v string) (time.Time, error) {
	d, err := time.Parse(dateLayout, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q is not a YYYY-MM-DD date", key, v)
	}
	return d, nil
}

func propertyID(id string) string {
	return strings.TrimPrefix(strings.TrimSpace(id), "properties/")
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
