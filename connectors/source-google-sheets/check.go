package main

import (
	"context"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check gets an access token and reads the sheet list of every configured
// spreadsheet, naming the first cause that fails: the credentials, a
// missing scope, a disabled Sheets API, or a spreadsheet that does not
// exist or is not shared with the account. With incremental, a spreadsheet
// whose modifiedTime Drive does not return passes with a warning, and its
// streams offer full refresh only.
func (s Source) Check(ctx context.Context, cfg Config) error {
	cfg.normalize()
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c, err := s.apiClient(ctx, cfg)
	if err != nil {
		return err
	}
	if _, err := c.auth.get(); err != nil {
		return err
	}
	for _, ref := range cfg.spreadsheets() {
		if _, err := c.sheets(ctx, ref.id); err != nil {
			return err
		}
	}
	if !cfg.Incremental {
		return nil
	}
	for _, ref := range cfg.spreadsheets() {
		if _, err := c.modifiedTime(ctx, ref.id); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			synclet.Logger(ctx).Warn(fmt.Sprintf("spreadsheet %s: %s; its streams offer full refresh only", ref.id, driveUnreadable(err)))
		}
	}
	return nil
}

// driveUnreadable describes a failed Drive modifiedTime request.
func driveUnreadable(err error) string {
	return fmt.Sprintf("Google Drive does not return its modifiedTime, which incremental sync needs (grant the %s scope and access to the file): %v", driveScope, err)
}
