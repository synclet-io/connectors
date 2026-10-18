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
	// defaultHistoryMonths is how far back a read starts without start_date:
	// the 16 months of data Search Console keeps.
	defaultHistoryMonths = 16
	authServiceAccount   = "service_account"
	authOAuth            = "oauth"
	dataStateFinal       = "final"
	dataStateAll         = "all"
	sitePrefixDomain     = "sc-domain:"
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

// CustomReport is a search analytics report read as its own stream.
type CustomReport struct {
	Name       string   `json:"name"`
	Dimensions []string `json:"dimensions"`
	SearchType string   `json:"search_type"`
}

// Config is the connector configuration.
type Config struct {
	Credentials   Credentials    `json:"credentials"`
	SiteURLs      []string       `json:"site_urls"`
	StartDate     string         `json:"start_date,omitempty"`
	EndDate       string         `json:"end_date,omitempty"`
	LookbackDays  *int           `json:"lookback_days,omitempty"`
	DataState     string         `json:"data_state,omitempty"`
	CustomReports []CustomReport `json:"custom_reports,omitempty"`
}

func (c Config) validate() error {
	var errs []error
	if err := c.Credentials.validate(); err != nil {
		errs = append(errs, err)
	}
	if len(c.SiteURLs) == 0 {
		errs = append(errs, errors.New("site_urls needs at least one site"))
	}
	seen := map[string]bool{}
	for _, u := range c.SiteURLs {
		if err := validSiteURL(u); err != nil {
			errs = append(errs, err)
			continue
		}
		if seen[u] {
			errs = append(errs, fmt.Errorf("site %s is listed twice", u))
		}
		seen[u] = true
	}
	start, err := c.startDate()
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
	if c.DataState != "" && c.DataState != dataStateFinal && c.DataState != dataStateAll {
		errs = append(errs, fmt.Errorf("data_state %q is neither %s nor %s", c.DataState, dataStateFinal, dataStateAll))
	}
	if _, err := reportDefs(c); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// validSiteURL accepts a URL-prefix property (https://www.example.com/ or
// http://www.example.com/) or a domain property (sc-domain:example.com), as
// Search Console names them.
func validSiteURL(u string) error {
	switch {
	case strings.HasPrefix(u, sitePrefixDomain):
		d := strings.TrimPrefix(u, sitePrefixDomain)
		if d == "" || strings.ContainsAny(d, "/: ") {
			return fmt.Errorf("site %q is not a domain property sc-domain:<domain>", u)
		}
	case strings.HasPrefix(u, "https://"), strings.HasPrefix(u, "http://"):
		_, host, _ := strings.Cut(u, "://")
		if !strings.HasSuffix(u, "/") || strings.HasPrefix(host, "/") || strings.Contains(u, " ") {
			return fmt.Errorf("site %q is not a URL-prefix property http(s)://<host>/...: it must end with /", u)
		}
	default:
		return fmt.Errorf("site %q is neither a URL-prefix property http(s)://<host>/ nor a domain property sc-domain:<domain>", u)
	}
	return nil
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

func (c Config) lookback() int {
	if c.LookbackDays == nil {
		return defaultLookbackDays
	}
	return *c.LookbackDays
}

func (c Config) dataState() string {
	if c.DataState == "" {
		return dataStateFinal
	}
	return c.DataState
}

// startDate returns start_date as a UTC day, or the zero time when it is not
// set.
func (c Config) startDate() (time.Time, error) {
	if c.StartDate == "" {
		return time.Time{}, nil
	}
	return parseDate("start_date", c.StartDate)
}

// start returns start_date, or the day defaultHistoryMonths before today.
func (c Config) start(today time.Time) (time.Time, error) {
	d, err := c.startDate()
	if err != nil || !d.IsZero() {
		return d, err
	}
	return today.AddDate(0, -defaultHistoryMonths, 0), nil
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
