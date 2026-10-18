package main

import (
	"errors"
	"fmt"
	"time"
)

const dateLayout = "2006-01-02"

// Config is the connector configuration.
type Config struct {
	AccessToken string `json:"access_token"`
	StartDate   string `json:"start_date"`
}

func (c Config) validate() error {
	var errs []error
	if c.AccessToken == "" {
		errs = append(errs, errors.New("access_token is required"))
	}
	if _, err := c.start(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// start returns start_date in UTC; it is an RFC 3339 time or a YYYY-MM-DD
// day, and the Unix epoch when empty.
func (c Config) start() (time.Time, error) {
	if c.StartDate == "" {
		return time.Unix(0, 0).UTC(), nil
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
