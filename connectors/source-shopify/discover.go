package main

import (
	"context"
	"errors"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const probeCompaniesDoc = `query ProbeCompanies { companies(first: 1) { nodes { id } } }`

// Discover returns every stream. The B2B streams are left out when the
// companies query fails with a GraphQL error, as it does on stores
// without B2B.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	b2b := true
	var res struct{}
	if err := c.query(ctx, "ProbeCompanies", probeCompaniesDoc, nil, &res); err != nil {
		var qe *queryError
		var ae *accessError
		if !errors.As(err, &qe) && !errors.As(err, &ae) {
			return nil, err
		}
		synclet.Logger(ctx).Info("companies and company_locations left out: the companies query failed", "error", err.Error())
		b2b = false
	}
	catalog := &synclet.Catalog{}
	for _, st := range streams {
		if st.b2b() && !b2b {
			continue
		}
		catalog.Streams = append(catalog.Streams, st.catalogStream())
	}
	return catalog, nil
}

func (s stream) catalogStream() synclet.Stream {
	out := synclet.Stream{
		Name:                    s.name,
		JSONSchema:              synclet.ObjectSchema(s.schema()),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{"id"}},
	}
	if cur := s.cursor(); cur != "" {
		out.SupportedSyncModes = append(out.SupportedSyncModes, synclet.SyncModeIncremental)
		out.SourceDefinedCursor = true
		out.DefaultCursorField = []string{cur}
	}
	return out
}
