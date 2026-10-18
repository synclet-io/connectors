package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// checkColumns are the columns of the one-day probe check sends per report
// type.
var checkColumns = []string{"day", "impressions"}

// Check requests one row of yesterday from every report type. A 401 fails
// with an invalid report key; a report type the key may not read is logged
// as a warning, and check fails only when no report type is readable.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	yesterday := day(s.clock()).AddDate(0, 0, -1).Format(dateLayout)
	var unavailable []string
	for _, d := range streamDefs {
		_, err := c.page(ctx, d, yesterday, checkColumns, 1, 0)
		var ue unavailableError
		if errors.As(err, &ue) {
			unavailable = append(unavailable, d.typeName)
			synclet.Logger(ctx).Warn(fmt.Sprintf("report type %s is not available to the report key; reading %s will fail", d.typeName, d.name))
			continue
		}
		if err != nil {
			return err
		}
	}
	if len(unavailable) == len(streamDefs) {
		return synclet.ConfigError(fmt.Errorf("report types %s are not available to the report key (HTTP 403)", strings.Join(unavailable, " and ")))
	}
	return nil
}
