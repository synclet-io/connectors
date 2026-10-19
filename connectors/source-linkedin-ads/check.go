package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check verifies the configuration, the OAuth credentials, that the app may
// call the Advertising API and that every configured ad account is
// accessible, naming the first cause that fails.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var probe page
	err := c.get(ctx, "adAccounts", query{{key: "q", value: "search"}, {key: "pageSize", value: "1"}}, &probe)
	var ae *apiError
	if errors.As(err, &ae) {
		switch ae.status {
		case http.StatusForbidden:
			return synclet.ConfigError(fmt.Errorf("the LinkedIn app lacks the Advertising API product or the token lacks the r_ads scope (%w)", err))
		case http.StatusUnauthorized:
			return synclet.ConfigError(fmt.Errorf("LinkedIn refused a newly issued access token (%w)", err))
		}
	}
	if err != nil {
		return err
	}
	if len(cfg.AccountIDs) == 0 && len(probe.Elements) == 0 {
		return synclet.ConfigError(errors.New("the user who authorized the app has no ad account; set account_ids or grant the user a role on an account"))
	}
	for _, id := range cfg.accounts() {
		err := c.get(ctx, "adAccounts/"+id, nil, &map[string]any{})
		if errors.As(err, &ae) && (ae.status == http.StatusForbidden || ae.status == http.StatusNotFound) {
			return synclet.ConfigError(fmt.Errorf("ad account %s is not accessible to the user who authorized the app (%w)", id, err))
		}
		if err != nil {
			return err
		}
	}
	return nil
}
