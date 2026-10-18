package main

import (
	"errors"
	"fmt"
	"time"
)

const (
	dateLayout          = "2006-01-02"
	defaultLookbackDays = 3
)

// Config is the connector configuration.
type Config struct {
	APIKey       string `json:"api_key"`
	StartDate    string `json:"start_date"`
	LookbackDays *int   `json:"lookback_days"`
}

func (c Config) validate() error {
	var errs []error
	if c.APIKey == "" {
		errs = append(errs, errors.New("api_key is required"))
	}
	if _, err := c.start(); err != nil {
		errs = append(errs, err)
	}
	if c.LookbackDays != nil && *c.LookbackDays < 0 {
		errs = append(errs, errors.New("lookback_days must not be negative"))
	}
	return errors.Join(errs...)
}

// start returns start_date as a UTC day.
func (c Config) start() (time.Time, error) {
	if c.StartDate == "" {
		return time.Time{}, errors.New("start_date is required")
	}
	d, err := time.Parse(dateLayout, c.StartDate)
	if err != nil {
		return time.Time{}, fmt.Errorf("start_date %q is not a YYYY-MM-DD date", c.StartDate)
	}
	return d, nil
}

func (c Config) lookback() int {
	if c.LookbackDays == nil {
		return defaultLookbackDays
	}
	return *c.LookbackDays
}

// day truncates t to the start of its UTC day.
func day(t time.Time) time.Time {
	return t.UTC().Truncate(24 * time.Hour)
}
