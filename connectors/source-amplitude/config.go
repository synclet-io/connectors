package main

import (
	"errors"
	"fmt"
	"time"
)

const (
	regionStandard = "Standard Server"
	regionEU       = "EU Residency Server"

	defaultRequestTimeRange = 24
	maxRequestTimeRange     = 8760
)

var regionBaseURLs = map[string]string{
	regionStandard: "https://amplitude.com/api/",
	regionEU:       "https://analytics.eu.amplitude.com/api/",
}

// Config is the connector configuration.
type Config struct {
	APIKey                    string `json:"api_key"`
	SecretKey                 string `json:"secret_key"`
	DataRegion                string `json:"data_region"`
	StartDate                 string `json:"start_date"`
	RequestTimeRange          *int   `json:"request_time_range"`
	ActiveUsersGroupByCountry *bool  `json:"active_users_group_by_country"`
}

func (c Config) validate() error {
	var errs []error
	if c.APIKey == "" {
		errs = append(errs, errors.New("api_key is required"))
	}
	if c.SecretKey == "" {
		errs = append(errs, errors.New("secret_key is required"))
	}
	if _, ok := regionBaseURLs[c.region()]; !ok {
		errs = append(errs, fmt.Errorf("data_region %q is not %q or %q", c.DataRegion, regionStandard, regionEU))
	}
	if _, err := c.start(); err != nil {
		errs = append(errs, err)
	}
	if r := c.timeRange(); r < 1 || r > maxRequestTimeRange {
		errs = append(errs, fmt.Errorf("request_time_range must be between 1 and %d hours, got %d", maxRequestTimeRange, r))
	}
	return errors.Join(errs...)
}

func (c Config) region() string {
	if c.DataRegion == "" {
		return regionStandard
	}
	return c.DataRegion
}

// start returns start_date in UTC. A date without a time starts at midnight.
func (c Config) start() (time.Time, error) {
	if c.StartDate == "" {
		return time.Time{}, errors.New("start_date is required")
	}
	if t, err := time.Parse(time.RFC3339, c.StartDate); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(dateLayout, c.StartDate); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("start_date %q is not a YYYY-MM-DDTHH:mm:ssZ time", c.StartDate)
}

// timeRange returns request_time_range in hours.
func (c Config) timeRange() int {
	if c.RequestTimeRange == nil {
		return defaultRequestTimeRange
	}
	return *c.RequestTimeRange
}

func (c Config) groupByCountry() bool {
	return c.ActiveUsersGroupByCountry == nil || *c.ActiveUsersGroupByCountry
}
