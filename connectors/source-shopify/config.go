package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	dateLayout        = "2006-01-02"
	defaultWindowDays = 30
	maxWindowDays     = 365
	shopDomainSuffix  = ".myshopify.com"
)

// shopName is the store name Shopify puts before .myshopify.com.
var shopName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Config is the connector configuration. Exactly one of AccessToken or
// ClientID with ClientSecret is set.
type Config struct {
	Shop         string `json:"shop"`
	AccessToken  string `json:"access_token"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	StartDate    string `json:"start_date"`
	WindowDays   int    `json:"window_days"`
}

func (c Config) validate() error {
	var errs []error
	if _, err := c.shop(); err != nil {
		errs = append(errs, err)
	}
	grant := c.ClientID != "" || c.ClientSecret != ""
	switch {
	case c.AccessToken != "" && grant:
		errs = append(errs, errors.New("set either access_token or client_id and client_secret, not both"))
	case c.AccessToken == "" && !grant:
		errs = append(errs, errors.New("access_token, or client_id and client_secret, is required"))
	case grant && (c.ClientID == "" || c.ClientSecret == ""):
		errs = append(errs, errors.New("client_id and client_secret are both required"))
	}
	if _, _, err := c.start(); err != nil {
		errs = append(errs, err)
	}
	if c.WindowDays < 0 || c.WindowDays > maxWindowDays {
		errs = append(errs, fmt.Errorf("window_days %d is outside 1-%d", c.WindowDays, maxWindowDays))
	}
	return errors.Join(errs...)
}

// shop returns the store name of the shop setting, which is the name or
// its myshopify.com domain.
func (c Config) shop() (string, error) {
	s := strings.ToLower(strings.TrimSpace(c.Shop))
	if s == "" {
		return "", errors.New("shop is required")
	}
	name := strings.TrimSuffix(s, shopDomainSuffix)
	if !shopName.MatchString(name) {
		return "", fmt.Errorf("shop %q must be the store name or its myshopify.com domain, such as example or example.myshopify.com; custom domains and URLs are not accepted", c.Shop)
	}
	return name, nil
}

// start returns start_date in UTC, an RFC 3339 time or a YYYY-MM-DD day,
// and false when it is empty.
func (c Config) start() (time.Time, bool, error) {
	if c.StartDate == "" {
		return time.Time{}, false, nil
	}
	if t, err := time.Parse(time.RFC3339, c.StartDate); err == nil {
		return t.UTC(), true, nil
	}
	d, err := time.Parse(dateLayout, c.StartDate)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("start_date %q is neither an RFC 3339 time nor a YYYY-MM-DD date", c.StartDate)
	}
	return d, true, nil
}

func (c Config) window() time.Duration {
	days := c.WindowDays
	if days == 0 {
		days = defaultWindowDays
	}
	return time.Duration(days) * 24 * time.Hour
}
