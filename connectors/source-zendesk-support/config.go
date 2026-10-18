package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const dateLayout = "2006-01-02"

// startTimeLag is how far before now a start_time must be: Zendesk rejects
// an incremental export whose start_time is less than a minute old.
const startTimeLag = time.Minute

var subdomainPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Config is the connector configuration.
type Config struct {
	Subdomain string `json:"subdomain"`
	Email     string `json:"email"`
	APIToken  string `json:"api_token"`
	StartDate string `json:"start_date"`
}

func (c Config) validate() error {
	var errs []error
	if err := validateSubdomain(c.Subdomain); err != nil {
		errs = append(errs, err)
	}
	if c.Email == "" {
		errs = append(errs, errors.New("email is required"))
	}
	if c.APIToken == "" {
		errs = append(errs, errors.New("api_token is required"))
	}
	if _, err := c.start(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func validateSubdomain(s string) error {
	if s == "" {
		return errors.New("subdomain is required")
	}
	if strings.Contains(s, "://") || strings.HasSuffix(strings.ToLower(s), ".zendesk.com") {
		name := s
		if i := strings.Index(name, "://"); i >= 0 {
			name = name[i+3:]
		}
		name = strings.TrimSuffix(strings.ToLower(strings.TrimRight(name, "/")), ".zendesk.com")
		return fmt.Errorf("subdomain %q must be the account name only, such as %q for https://%s.zendesk.com", s, name, name)
	}
	if !subdomainPattern.MatchString(s) {
		return fmt.Errorf("subdomain %q may contain only lowercase letters, digits and hyphens", s)
	}
	return nil
}

// start returns start_date in UTC; it is an RFC 3339 time or a YYYY-MM-DD
// day.
func (c Config) start() (time.Time, error) {
	if c.StartDate == "" {
		return time.Time{}, errors.New("start_date is required")
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

// startTime returns t as the Unix start_time of an incremental export, no
// later than one minute before now.
func startTime(t, now time.Time) int64 {
	if latest := now.Add(-startTimeLag); t.After(latest) {
		t = latest
	}
	return t.Unix()
}
