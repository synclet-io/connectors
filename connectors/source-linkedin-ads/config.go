package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	dateLayout          = "2006-01-02"
	defaultLookbackDays = 7
	maxLookbackDays     = 30
)

// Config is the connector configuration.
type Config struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	RefreshToken string   `json:"refresh_token"`
	AccountIDs   []string `json:"account_ids,omitempty"`
	StartDate    string   `json:"start_date"`
	EndDate      string   `json:"end_date,omitempty"`
	LookbackDays *int     `json:"lookback_days,omitempty"`
}

func (c Config) validate() error {
	var errs []error
	for key, v := range map[string]string{"client_id": c.ClientID, "client_secret": c.ClientSecret, "refresh_token": c.RefreshToken} {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", key))
		}
	}
	for _, id := range c.AccountIDs {
		if !isDigits(strings.TrimSpace(id)) {
			errs = append(errs, fmt.Errorf("account id %q is not a number", id))
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
	return errors.Join(errs...)
}

// accounts returns the configured account ids without surrounding spaces.
func (c Config) accounts() []string {
	ids := make([]string, len(c.AccountIDs))
	for i, id := range c.AccountIDs {
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
