package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	cfg := sharedPostgres(t, "check_ok")
	execSQL(t, cfg, `DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN
    CREATE ROLE reader LOGIN PASSWORD 'reader-pw';
  END IF;
END $$`)
	execSQL(t, cfg, `CREATE SCHEMA IF NOT EXISTS locked`)
	execSQL(t, cfg, `GRANT USAGE ON SCHEMA locked TO reader`)
	reader := cfg
	reader.Username, reader.Password = "reader", "reader-pw"

	t.Run("succeeds and creates nothing", func(t *testing.T) {
		require.NoError(t, Destination{}.Check(t.Context(), cfg))
	})

	cases := []struct {
		name string
		edit func(c *Config)
		want string
	}{
		{"wrong password", func(c *Config) { c.Password = "wrong" }, `cannot log in as "synclet": wrong user name or password`},
		{"unreachable host", func(c *Config) { c.Host, c.Port = "127.0.0.1", closedPort(t) }, "cannot reach 127.0.0.1:"},
		{"missing database", func(c *Config) { c.Database = "nope" }, `database "nope" does not exist`},
		{"no CREATE on schema", func(c *Config) { *c = reader; c.Schema = "locked" }, `user "reader" has no CREATE privilege on schema "locked"`},
		{"missing schema without CREATE on database", func(c *Config) { *c = reader; c.Schema = "absent" }, `schema "absent" does not exist and user "reader" cannot create it`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bad := cfg
			c.edit(&bad)
			err := Destination{}.Check(t.Context(), bad)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
		})
	}
}
