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
	maxLookbackDays     = 30

	envProduction = "production"
	envSandbox    = "sandbox"
)

// baseURLs are the API roots of each environment.
var baseURLs = map[string]string{
	envProduction: "https://business-api.tiktok.com/open_api/v1.3/",
	envSandbox:    "https://sandbox-ads.tiktok.com/open_api/v1.3/",
}

// Config is the connector configuration.
type Config struct {
	AccessToken   string   `json:"access_token"`
	AdvertiserIDs []string `json:"advertiser_ids"`
	StartDate     string   `json:"start_date"`
	EndDate       string   `json:"end_date,omitempty"`
	LookbackDays  *int     `json:"lookback_days,omitempty"`
	Environment   string   `json:"environment,omitempty"`
}

func (c Config) validate() error {
	var errs []error
	if c.AccessToken == "" {
		errs = append(errs, errors.New("access_token is required"))
	}
	if len(c.AdvertiserIDs) == 0 {
		errs = append(errs, errors.New("advertiser_ids needs at least one advertiser id"))
	}
	for _, id := range c.AdvertiserIDs {
		if !isDigits(strings.TrimSpace(id)) {
			errs = append(errs, fmt.Errorf("advertiser id %q is not a number", id))
		}
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
	if n := c.lookback(); n < 0 || n > maxLookbackDays {
		errs = append(errs, fmt.Errorf("lookback_days %d is not between 0 and %d", n, maxLookbackDays))
	}
	if _, ok := baseURLs[c.environment()]; !ok {
		errs = append(errs, fmt.Errorf("environment %q is neither %s nor %s", c.Environment, envProduction, envSandbox))
	}
	return errors.Join(errs...)
}

// advertisers returns the configured advertiser ids without surrounding spaces.
func (c Config) advertisers() []string {
	ids := make([]string, len(c.AdvertiserIDs))
	for i, id := range c.AdvertiserIDs {
		ids[i] = strings.TrimSpace(id)
	}
	return ids
}

func (c Config) lookback() int {
	if c.LookbackDays == nil {
		return defaultLookbackDays
	}
	return *c.LookbackDays
}

func (c Config) environment() string {
	if c.Environment == "" {
		return envProduction
	}
	return c.Environment
}

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
