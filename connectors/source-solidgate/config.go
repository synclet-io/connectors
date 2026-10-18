package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	dateLayout          = "2006-01-02"
	defaultLookbackDays = 2
	// defaultHistoryYears is how far back a read without start_date goes.
	defaultHistoryYears = 2
)

// Account is one Solidgate merchant account; its name fills the solid_account
// field of every record read with its keys.
type Account struct {
	Name      string `json:"name"`
	PublicKey string `json:"public_key"`
	SecretKey string `json:"secret_key"`
}

// Config is the connector configuration.
type Config struct {
	Accounts     []Account `json:"accounts"`
	StartDate    string    `json:"start_date"`
	LookbackDays *int      `json:"lookback_days"`
}

func (c Config) validate() error {
	var errs []error
	if len(c.Accounts) == 0 {
		errs = append(errs, errors.New("accounts needs at least one account"))
	}
	seen := map[string]bool{}
	for i, a := range c.Accounts {
		if a.Name == "" {
			errs = append(errs, fmt.Errorf("accounts[%d]: name is required", i))
		} else if seen[a.Name] {
			errs = append(errs, fmt.Errorf("accounts[%d]: name %q is used by another account", i, a.Name))
		}
		seen[a.Name] = true
		if !strings.HasPrefix(a.PublicKey, "api_pk_") {
			errs = append(errs, fmt.Errorf("accounts[%d]: public_key must start with api_pk_", i))
		}
		if !strings.HasPrefix(a.SecretKey, "api_sk_") {
			errs = append(errs, fmt.Errorf("accounts[%d]: secret_key must start with api_sk_", i))
		}
	}
	if c.StartDate != "" {
		if _, err := time.Parse(dateLayout, c.StartDate); err != nil {
			errs = append(errs, fmt.Errorf("start_date %q is not a YYYY-MM-DD date", c.StartDate))
		}
	}
	if c.LookbackDays != nil && *c.LookbackDays < 0 {
		errs = append(errs, errors.New("lookback_days must not be negative"))
	}
	return errors.Join(errs...)
}

// start returns start_date as a UTC day, or the day defaultHistoryYears
// before now when it is not set.
func (c Config) start(now time.Time) time.Time {
	if d, err := time.Parse(dateLayout, c.StartDate); err == nil {
		return d
	}
	return day(now).AddDate(-defaultHistoryYears, 0, 0)
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
