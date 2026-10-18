package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	dateLayout          = "2006-01-02"
	defaultLookbackDays = 14
	// historyMonths is how far back Google Ads keeps performance data.
	historyMonths = 37
)

// CustomQuery is a GAQL query read as its own stream.
type CustomQuery struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// Config is the connector configuration.
type Config struct {
	DeveloperToken   string        `json:"developer_token"`
	ClientID         string        `json:"client_id"`
	ClientSecret     string        `json:"client_secret"`
	RefreshToken     string        `json:"refresh_token"`
	CustomerIDs      []string      `json:"customer_ids"`
	LoginCustomerID  string        `json:"login_customer_id"`
	ChildCustomerIDs []string      `json:"child_customer_ids"`
	StartDate        string        `json:"start_date"`
	EndDate          string        `json:"end_date"`
	LookbackDays     *int          `json:"lookback_days"`
	CustomQueries    []CustomQuery `json:"custom_queries"`
}

func (c Config) validate() error {
	var errs []error
	for _, kv := range [][2]string{
		{"developer_token", c.DeveloperToken},
		{"client_id", c.ClientID},
		{"client_secret", c.ClientSecret},
		{"refresh_token", c.RefreshToken},
	} {
		if strings.TrimSpace(kv[1]) == "" {
			errs = append(errs, fmt.Errorf("%s is required", kv[0]))
		}
	}
	if len(c.CustomerIDs) == 0 {
		errs = append(errs, errors.New("customer_ids needs at least one customer id"))
	}
	for _, id := range c.CustomerIDs {
		if !isDigits(customerID(id)) {
			errs = append(errs, fmt.Errorf("customer id %q is not a number, with or without dashes", id))
		}
	}
	for _, id := range c.ChildCustomerIDs {
		if !isDigits(customerID(id)) {
			errs = append(errs, fmt.Errorf("child customer id %q is not a number, with or without dashes", id))
		}
	}
	if c.LoginCustomerID != "" && !isDigits(customerID(c.LoginCustomerID)) {
		errs = append(errs, fmt.Errorf("login_customer_id %q is not a number, with or without dashes", c.LoginCustomerID))
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
	if _, err := customDefs(c.CustomQueries); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// customers returns the configured customer ids without dashes.
func (c Config) customers() []string { return customerIDs(c.CustomerIDs) }

// children returns the child_customer_ids filter without dashes.
func (c Config) children() []string { return customerIDs(c.ChildCustomerIDs) }

func (c Config) login() string { return customerID(c.LoginCustomerID) }

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

func customerID(id string) string {
	return strings.ReplaceAll(strings.TrimSpace(id), "-", "")
}

func customerIDs(ids []string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = customerID(id)
	}
	return out
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
