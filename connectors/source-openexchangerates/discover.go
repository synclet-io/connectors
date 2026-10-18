package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	streamCurrencies      = "currencies"
	streamLatestRates     = "latest_rates"
	streamHistoricalRates = "historical_rates"
)

func streamSchema(withDate bool) map[synclet.PropertyName]synclet.PropertySpec {
	props := map[synclet.PropertyName]synclet.PropertySpec{
		"base":      {PropertyType: typed(synclet.String)},
		"currency":  {PropertyType: typed(synclet.String)},
		"rate":      {PropertyType: typed(synclet.Number)},
		"timestamp": {PropertyType: typed(synclet.Integer)},
	}
	if withDate {
		props["date"] = synclet.PropertySpec{PropertyType: typed(synclet.String), Format: synclet.Date}
	}
	return props
}

// Discover returns the three streams; the API has the same routes for every
// account, so nothing is requested.
func (Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	return &synclet.Catalog{Streams: []synclet.Stream{
		{
			Name: streamCurrencies,
			JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
				"code": {PropertyType: typed(synclet.String)},
				"name": {PropertyType: typed(synclet.String)},
			}),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
			SourceDefinedPrimaryKey: [][]string{{"code"}},
		},
		{
			Name:                    streamLatestRates,
			JSONSchema:              synclet.ObjectSchema(streamSchema(false)),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
			SourceDefinedPrimaryKey: [][]string{{"base"}, {"currency"}},
		},
		{
			Name:                    streamHistoricalRates,
			JSONSchema:              synclet.ObjectSchema(streamSchema(true)),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
			SourceDefinedCursor:     true,
			DefaultCursorField:      []string{"date"},
			SourceDefinedPrimaryKey: [][]string{{"date"}, {"base"}, {"currency"}},
		},
	}}, nil
}
