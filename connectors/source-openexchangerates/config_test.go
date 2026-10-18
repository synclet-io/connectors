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
		{name: "valid", cfg: Config{AppID: "x", StartDate: "2026-01-31"}},
		{name: "no app_id", cfg: Config{StartDate: "2026-01-31"}, err: "app_id is required"},
		{name: "no start_date", cfg: Config{AppID: "x"}, err: "start_date is required"},
		{name: "bad start_date", cfg: Config{AppID: "x", StartDate: "2026-1-31"}, err: `start_date "2026-1-31" is not a YYYY-MM-DD date`},
		{name: "impossible day", cfg: Config{AppID: "x", StartDate: "2026-02-30"}, err: "is not a YYYY-MM-DD date"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestConfigQuery(t *testing.T) {
	assert.Empty(t, Config{}.query().Encode())
	assert.Equal(t, "base=EUR&symbols=GBP%2CUSD", Config{Base: "EUR", Symbols: []string{"GBP", "USD"}}.query().Encode())
}
