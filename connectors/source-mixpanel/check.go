package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check lists the cohorts of the project, one Query API request that needs
// valid credentials and access to the project. A 400 names a project the
// service account is not a member of, or a project secret used with a
// project_id.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	if _, err := cfg.days(s.clock()); err != nil {
		return synclet.ConfigError(err)
	}
	r := &reader{c: s.apiClient(cfg), cfg: cfg}
	_, err := r.listCohorts(ctx)
	var ae *apiError
	if errors.As(err, &ae) && ae.status == http.StatusBadRequest {
		return synclet.ConfigError(fmt.Errorf("Mixpanel rejected the request: check the project_id and region (%w)", ae)) //nolint:staticcheck // the message starts with the product name
	}
	return err
}
