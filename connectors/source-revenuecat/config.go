package main

import (
	"errors"
	"fmt"
)

// Config is the connector configuration.
type Config struct {
	APIKey      string `json:"api_key"`
	ProjectID   string `json:"project_id"`
	Environment string `json:"environment"`
}

func (c Config) validate() error {
	var errs []error
	if c.APIKey == "" {
		errs = append(errs, errors.New("api_key is required"))
	}
	switch c.Environment {
	case "", "sandbox", "production":
	default:
		errs = append(errs, fmt.Errorf("environment %q must be sandbox, production or empty for both", c.Environment))
	}
	return errors.Join(errs...)
}
