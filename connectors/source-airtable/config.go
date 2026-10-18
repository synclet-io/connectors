package main

import (
	"errors"
	"fmt"
	"regexp"
)

var baseIDPattern = regexp.MustCompile(`^app[A-Za-z0-9]{14}$`)

// Config is the connector configuration.
type Config struct {
	PersonalAccessToken string   `json:"personal_access_token"`
	BaseIDs             []string `json:"base_ids,omitempty"`
}

func (c Config) validate() error {
	var errs []error
	if c.PersonalAccessToken == "" {
		errs = append(errs, errors.New("personal_access_token is required"))
	}
	seen := map[string]bool{}
	for _, id := range c.BaseIDs {
		if !baseIDPattern.MatchString(id) {
			errs = append(errs, fmt.Errorf("base id %q must be \"app\" followed by 14 letters or digits, as in the base URL https://airtable.com/appXXXXXXXXXXXXXX", id))
			continue
		}
		if seen[id] {
			errs = append(errs, fmt.Errorf("base id %q is listed twice", id))
		}
		seen[id] = true
	}
	return errors.Join(errs...)
}
