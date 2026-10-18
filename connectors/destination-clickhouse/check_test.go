package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	cfg := sharedClickHouse(t, "check_ok")
	execSQL(t, cfg, `CREATE USER IF NOT EXISTS reader IDENTIFIED BY 'reader-pw'`)
	execSQL(t, cfg, `GRANT SELECT, INSERT ON check_ok.* TO reader`)
	execSQL(t, cfg, `CREATE DATABASE IF NOT EXISTS ordinary_db ENGINE = Memory`)
	reader := cfg
	reader.Username, reader.Password = "reader", "reader-pw"

	t.Run("succeeds and creates nothing", func(t *testing.T) {
		require.NoError(t, Destination{}.Check(t.Context(), cfg))
		assert.Empty(t, queryStrings(t, cfg, `SELECT name FROM system.tables WHERE database = 'check_ok'`))
	})
	t.Run("succeeds over http", func(t *testing.T) {
		require.NoError(t, Destination{}.Check(t.Context(), overHTTP(cfg)))
	})

	cases := []struct {
		name string
		edit func(c *Config)
		want string
	}{
		{"wrong password", func(c *Config) { c.Password = "wrong" }, `cannot log in as "synclet": wrong user name or password`},
		{"wrong password over http", func(c *Config) { *c = overHTTP(*c); c.Password = "wrong" }, `cannot log in as "synclet": wrong user name or password`},
		{"unreachable host", func(c *Config) { c.Host, c.Port = "127.0.0.1", closedPort(t) }, "cannot reach 127.0.0.1:"},
		{"missing database", func(c *Config) { c.Database = "nope" }, `database "nope" does not exist`},
		{"missing database over http", func(c *Config) { *c = overHTTP(*c); c.Database = "nope" }, `database "nope" does not exist`},
		{"no CREATE TABLE privilege", func(c *Config) { *c = reader }, `user "reader" has no CREATE TABLE privilege on database "check_ok"`},
		{"database engine without EXCHANGE", func(c *Config) { c.Database = "ordinary_db" }, `database "ordinary_db" has engine Memory`},
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
