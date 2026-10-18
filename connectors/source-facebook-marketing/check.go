package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// inaccessibleCodes are the Graph API error codes of an ad account the token
// cannot read: unknown object, missing permission, or no access to it.
var inaccessibleCodes = map[int]bool{10: true, 100: true, 200: true, 803: true}

// Check verifies the token, its ads_read permission and access to every
// configured ad account, naming the first cause that fails.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var me map[string]any
	if err := c.get(ctx, "me", fieldsQuery([]string{"id"}), &me); err != nil {
		return err
	}
	var perms struct {
		Data []struct {
			Permission string `json:"permission"`
			Status     string `json:"status"`
		} `json:"data"`
	}
	if err := c.get(ctx, "me/permissions", nil, &perms); err != nil {
		return err
	}
	granted := false
	for _, p := range perms.Data {
		if p.Permission == "ads_read" && p.Status == "granted" {
			granted = true
		}
	}
	if !granted {
		return synclet.ConfigError(errors.New("missing ads_read permission: grant it to the access token"))
	}
	for _, acct := range cfg.accounts() {
		var obj map[string]any
		err := c.get(ctx, "act_"+acct, fieldsQuery([]string{"account_id", "name"}), &obj)
		var ae *apiError
		if errors.As(err, &ae) && (inaccessibleCodes[ae.graph.Code] || ae.status == http.StatusForbidden) {
			return synclet.ConfigError(fmt.Errorf("ad account act_%s not accessible (%w)", acct, err))
		}
		if err != nil {
			return err
		}
	}
	return nil
}
