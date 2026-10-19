package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Snowflake error codes the connector explains.
const (
	codeIncorrectLogin   = 390100
	codeInvalidJWT       = 390144
	codeRoleNotGranted   = 390189
	codeObjectNotAllowed = 390201
	codeNoWarehouse      = 606
	codeNotAuthorized    = 2003
)

// conn is a logged-in session with the warehouse, database and role it
// resolved to.
type conn struct {
	db        *sql.DB
	cfg       Config
	warehouse string
	database  string
	role      string
}

func (c *conn) close() { _ = c.db.Close() }

// driverConfig is the gosnowflake configuration for cfg.
func (s Source) driverConfig(cfg Config) (sf.Config, error) {
	dc := sf.Config{
		Account:                   cfg.Account,
		User:                      cfg.Credentials.Username,
		Warehouse:                 cfg.Warehouse,
		Database:                  cfg.Database,
		Role:                      cfg.Role,
		Application:               "synclet",
		ValidateDefaultParameters: sf.ConfigBoolFalse,
		LoginTimeout:              time.Minute,
	}
	if s.endpoint != nil {
		dc.Protocol = s.endpoint.Scheme
		dc.Host = s.endpoint.Hostname()
		port, err := strconv.Atoi(s.endpoint.Port())
		if err != nil {
			return sf.Config{}, fmt.Errorf("endpoint port: %w", err)
		}
		dc.Port = port
	}
	switch cfg.Credentials.AuthType {
	case authKeyPair:
		key, err := parsePrivateKey(cfg.Credentials.PrivateKey, cfg.Credentials.PrivateKeyPassword)
		if err != nil {
			return sf.Config{}, synclet.ConfigError(err)
		}
		dc.Authenticator = sf.AuthTypeJwt
		dc.PrivateKey = key
	case authPassword:
		dc.Authenticator = sf.AuthTypeSnowflake
		dc.Password = cfg.Credentials.Password
	}
	return dc, nil
}

// connect validates cfg, logs in and resolves the session's warehouse,
// database and role. A login or session that cannot serve the connector is
// a synclet.ConfigError naming the cause.
func (s Source) connect(ctx context.Context, cfg Config) (*conn, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	dc, err := s.driverConfig(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(sf.NewConnector(sf.SnowflakeDriver{}, dc))
	c := &conn{db: db, cfg: cfg}
	var wh, database, role sql.NullString
	err = db.QueryRowContext(ctx, `SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()`).Scan(&wh, &database, &role)
	if err != nil {
		c.close()
		return nil, loginError(cfg, err)
	}
	c.role = role.String
	switch {
	case !database.Valid:
		c.close()
		return nil, synclet.ConfigError(fmt.Errorf("database %q does not exist, or role %q has no USAGE privilege on it", cfg.Database, c.role))
	case !wh.Valid:
		c.close()
		return nil, synclet.ConfigError(fmt.Errorf("warehouse %q does not exist, or role %q has no USAGE privilege on it", cfg.Warehouse, c.role))
	}
	c.warehouse, c.database = wh.String, database.String
	return c, nil
}

// loginError names the cause of a failed login.
func loginError(cfg Config, err error) error {
	user := cfg.Credentials.Username
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return synclet.ConfigError(fmt.Errorf("account %q not found: use the account identifier orgname-accountname or the account locator: %w", cfg.Account, err))
	}
	var se *sf.SnowflakeError
	if !errors.As(err, &se) {
		return fmt.Errorf("log in to account %q: %w", cfg.Account, err)
	}
	switch se.Number {
	case codeIncorrectLogin, codeInvalidJWT:
		if cfg.Credentials.AuthType == authKeyPair {
			return synclet.ConfigError(fmt.Errorf("login rejected for user %q: private_key does not match the user's RSA_PUBLIC_KEY, or the user does not exist: %w", user, err))
		}
		return synclet.ConfigError(fmt.Errorf("login rejected for user %q: the username or password is wrong: %w", user, err))
	case sf.ErrCodeFailedToConnect:
		return synclet.ConfigError(fmt.Errorf("account %q not found: use the account identifier orgname-accountname or the account locator: %w", cfg.Account, err))
	case codeRoleNotGranted:
		return synclet.ConfigError(fmt.Errorf("role %q is not granted to user %q: %w", cfg.Role, user, err))
	case codeObjectNotAllowed:
		return synclet.ConfigError(fmt.Errorf("log in as %q: %w", user, err))
	}
	if se.Number >= 390000 && se.Number < 400000 {
		return synclet.ConfigError(fmt.Errorf("login rejected for user %q: %w", user, err))
	}
	return fmt.Errorf("log in to account %q: %w", cfg.Account, err)
}

// queryError explains err from a query of the session, or returns it.
func (c *conn) queryError(err error) error {
	var se *sf.SnowflakeError
	if errors.As(err, &se) && se.Number == codeNoWarehouse {
		return synclet.ConfigError(fmt.Errorf("warehouse %q cannot run queries for role %q: it is missing, or the role has no USAGE privilege on it: %w", c.cfg.Warehouse, c.role, err))
	}
	return err
}

func notAuthorized(err error) bool {
	var se *sf.SnowflakeError
	return errors.As(err, &se) && se.Number == codeNotAuthorized
}

// ident quotes a Snowflake identifier.
func ident(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// tableRef is the quoted name of table name in schema of the session's
// database.
func (c *conn) tableRef(schema, name string) string {
	return ident(c.database) + "." + ident(schema) + "." + ident(name)
}

// rows runs query and returns its rows, each column as a string; NULL is
// the empty string.
func (c *conn) rows(ctx context.Context, query string, args ...any) ([][]string, error) {
	_, out, err := c.query(ctx, query, args...)
	return out, err
}

// records runs query and returns its rows by column name.
func (c *conn) records(ctx context.Context, query string, args ...any) ([]map[string]string, error) {
	cols, rows, err := c.query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]string, len(rows))
	for i, r := range rows {
		out[i] = make(map[string]string, len(cols))
		for j, name := range cols {
			out[i][name] = r[j]
		}
	}
	return out, nil
}

func (c *conn) query(ctx context.Context, query string, args ...any) (cols []string, out [][]string, rerr error) {
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, c.queryError(err)
	}
	defer func() { rerr = errors.Join(rerr, rows.Close()) }()
	cols, err = rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		cells := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		r := make([]string, len(cols))
		for i, cell := range cells {
			r[i] = cell.String
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, c.queryError(err)
	}
	return cols, out, nil
}
