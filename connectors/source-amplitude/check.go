package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check lists the project's annotations, the cheapest Dashboard REST
// request. A 401 fails with the keys or the data region named.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	var resp map[string]any
	return s.apiClient(cfg).getJSON(ctx, listDefs[streamAnnotations].path, nil, &resp)
}
