package main

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultLookbackMinutes  = 5
	defaultMaxBlockDepth    = 16
	maxMaxBlockDepth        = 100
	defaultMaxBlocksPerPage = 10000
)

// Config is the connector configuration.
type Config struct {
	AccessToken      string `json:"access_token"`
	LookbackMinutes  *int   `json:"lookback_minutes,omitempty"`
	MaxBlockDepth    *int   `json:"max_block_depth,omitempty"`
	MaxBlocksPerPage *int   `json:"max_blocks_per_page,omitempty"`
}

func (c Config) validate() error {
	var errs []error
	if c.AccessToken == "" {
		errs = append(errs, errors.New("access_token is required"))
	}
	if c.LookbackMinutes != nil && *c.LookbackMinutes < 0 {
		errs = append(errs, fmt.Errorf("lookback_minutes must be 0 or more, got %d", *c.LookbackMinutes))
	}
	if c.MaxBlockDepth != nil && (*c.MaxBlockDepth < 1 || *c.MaxBlockDepth > maxMaxBlockDepth) {
		errs = append(errs, fmt.Errorf("max_block_depth must be between 1 and %d, got %d", maxMaxBlockDepth, *c.MaxBlockDepth))
	}
	if c.MaxBlocksPerPage != nil && *c.MaxBlocksPerPage < 1 {
		errs = append(errs, fmt.Errorf("max_blocks_per_page must be 1 or more, got %d", *c.MaxBlocksPerPage))
	}
	return errors.Join(errs...)
}

func (c Config) lookback() time.Duration {
	if c.LookbackMinutes == nil {
		return defaultLookbackMinutes * time.Minute
	}
	return time.Duration(*c.LookbackMinutes) * time.Minute
}

func (c Config) maxBlockDepth() int {
	if c.MaxBlockDepth == nil {
		return defaultMaxBlockDepth
	}
	return *c.MaxBlockDepth
}

func (c Config) maxBlocksPerPage() int {
	if c.MaxBlocksPerPage == nil {
		return defaultMaxBlocksPerPage
	}
	return *c.MaxBlocksPerPage
}
