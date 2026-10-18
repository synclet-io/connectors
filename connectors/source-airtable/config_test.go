package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		err  string
	}{
		{name: "token only", cfg: Config{PersonalAccessToken: testToken}},
		{name: "with bases", cfg: Config{PersonalAccessToken: testToken, BaseIDs: []string{base1, base2}}},
		{name: "no token", cfg: Config{}, err: "personal_access_token is required"},
		{name: "base URL instead of id", cfg: Config{PersonalAccessToken: testToken, BaseIDs: []string{"https://airtable.com/" + base1}}, err: `base id "https://airtable.com/appBase0000000001" must be "app" followed by 14 letters or digits`},
		{name: "short id", cfg: Config{PersonalAccessToken: testToken, BaseIDs: []string{"app123"}}, err: `base id "app123" must be`},
		{name: "duplicate id", cfg: Config{PersonalAccessToken: testToken, BaseIDs: []string{base1, base1}}, err: `base id "appBase0000000001" is listed twice`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.err)
		})
	}
}
