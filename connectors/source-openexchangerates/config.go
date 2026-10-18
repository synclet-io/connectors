package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const dateLayout = "2006-01-02"

// Config is the connector configuration.
type Config struct {
	AppID     string   `json:"app_id"`
	StartDate string   `json:"start_date"`
	Base      string   `json:"base"`
	Symbols   []string `json:"symbols"`
}

func (c Config) validate() error {
	var errs []error
	if c.AppID == "" {
		errs = append(errs, errors.New("app_id is required"))
	}
	if _, err := c.start(); err != nil {
		errs = append(errs, err)
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

// query returns the base and symbols parameters every rates request carries;
// each is left out when not configured.
func (c Config) query() url.Values {
	q := url.Values{}
	if c.Base != "" {
		q.Set("base", c.Base)
	}
	if len(c.Symbols) > 0 {
		q.Set("symbols", strings.Join(c.Symbols, ","))
	}
	return q
}
