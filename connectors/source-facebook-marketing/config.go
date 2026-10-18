package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	dateLayout          = "2006-01-02"
	defaultLookbackDays = 28
)

// Header is an extra HTTP header sent with every Graph API request.
type Header struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Config is the connector configuration.
type Config struct {
	AccessToken     string   `json:"access_token"`
	AccountIDs      []string `json:"account_ids"`
	BusinessIDs     []string `json:"business_ids"`
	StartDate       string   `json:"start_date"`
	EndDate         string   `json:"end_date"`
	LookbackDays    *int     `json:"lookback_days"`
	GraphAPIBaseURL string   `json:"graph_api_base_url"`
	GraphAPIHeaders []Header `json:"graph_api_headers"`
}

func (c Config) validate() error {
	var errs []error
	if c.AccessToken == "" {
		errs = append(errs, errors.New("access_token is required"))
	}
	if len(c.AccountIDs) == 0 {
		errs = append(errs, errors.New("account_ids needs at least one ad account id"))
	}
	for _, id := range c.AccountIDs {
		if !isDigits(stripAct(id)) {
			errs = append(errs, fmt.Errorf("account id %q is not a number, with or without the act_ prefix", id))
		}
	}
	for _, id := range c.BusinessIDs {
		if !isDigits(id) {
			errs = append(errs, fmt.Errorf("business id %q is not a number", id))
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
	if c.LookbackDays != nil && *c.LookbackDays < 0 {
		errs = append(errs, fmt.Errorf("lookback_days %d is negative", *c.LookbackDays))
	}
	if c.GraphAPIBaseURL != "" {
		u, err := url.Parse(c.GraphAPIBaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("graph_api_base_url %q is not an http(s) URL", c.GraphAPIBaseURL))
		}
	}
	for i, h := range c.GraphAPIHeaders {
		if strings.TrimSpace(h.Name) == "" {
			errs = append(errs, fmt.Errorf("graph_api_headers[%d] has no name", i))
		}
	}
	return errors.Join(errs...)
}

// accounts returns the configured ad account ids without the act_ prefix.
func (c Config) accounts() []string {
	ids := make([]string, len(c.AccountIDs))
	for i, id := range c.AccountIDs {
		ids[i] = stripAct(id)
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

func stripAct(id string) string { return strings.TrimPrefix(strings.TrimSpace(id), "act_") }

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
