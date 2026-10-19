package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"

	sf "github.com/snowflakedb/gosnowflake/v2"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// Snowflake error codes the connector explains.
const (
	codeIncorrectLogin   = 390100
	codeInvalidJWT       = 390144
	codeRoleNotGranted   = 390189
	codeObjectNotAllowed = 390201
	codeNoWarehouse      = 606
	codeNotAuthorized    = 2003
	codeNoPrivilege      = 3001
)

// conn is one logged-in Snowflake session. Temporary tables and the
// transaction of a batch live in the session, so every statement of a sync
// runs on c.
type conn struct {
	db  *sql.DB
	c   *sql.Conn
	cfg Config
	// database and role are the session's, as Snowflake resolved them;
	// schema is the configured schema's identifier.
	database string
	role     string
	schema   string
}

func (c *conn) close() error {
	return multierr.Combine(c.c.Close(), c.db.Close())
}

// driverConfig is the gosnowflake configuration for cfg.
func (d Destination) driverConfig(cfg Config) (sf.Config, error) {
	key, err := parsePrivateKey(cfg.Credentials.PrivateKey, cfg.Credentials.PrivateKeyPassword)
	if err != nil {
		return sf.Config{}, synclet.ConfigError(fmt.Errorf("credentials.private_key: %w", err))
	}
	dc := sf.Config{
		Account:                   cfg.Account,
		User:                      cfg.Credentials.Username,
		Warehouse:                 cfg.Warehouse,
		Database:                  cfg.Database,
		Role:                      cfg.Role,
		Authenticator:             sf.AuthTypeJwt,
		PrivateKey:                key,
		Application:               "synclet",
		ValidateDefaultParameters: sf.ConfigBoolFalse,
		Transporter:               d.transport,
		LoginTimeout:              d.loginTimeout,
	}
	if d.endpoint != nil {
		dc.Protocol = d.endpoint.Scheme
		dc.Host = d.endpoint.Hostname()
		port, err := strconv.Atoi(d.endpoint.Port())
		if err != nil {
			return sf.Config{}, fmt.Errorf("endpoint port: %w", err)
		}
		dc.Port = port
	}
	return dc, nil
}

// connect validates cfg, logs in and resolves the session's warehouse,
// database and role. A login or session that cannot serve the connector is
// a synclet.ConfigError naming the cause.
func (d Destination) connect(ctx context.Context, cfg Config) (_ *conn, rerr error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	schema, err := sfName("schema", cfg.Schema)
	if err != nil {
		return nil, synclet.ConfigError(err)
	}
	dc, err := d.driverConfig(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(sf.NewConnector(sf.SnowflakeDriver{}, dc))
	c := &conn{db: db, cfg: cfg, schema: schema}
	if c.c, err = db.Conn(ctx); err != nil {
		_ = db.Close()
		return nil, loginError(cfg, err)
	}
	defer func() {
		if rerr != nil {
			_ = c.close()
		}
	}()
	var wh, database, role sql.NullString
	if err := c.c.QueryRowContext(ctx, `SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()`).Scan(&wh, &database, &role); err != nil {
		return nil, loginError(cfg, err)
	}
	c.role = role.String
	switch {
	case !database.Valid:
		return nil, synclet.ConfigError(fmt.Errorf("database %q does not exist, or role %q has no USAGE privilege on it", cfg.Database, c.role))
	case !wh.Valid:
		return nil, synclet.ConfigError(fmt.Errorf("warehouse %q does not exist, or role %q has no USAGE privilege on it", cfg.Warehouse, c.role))
	}
	c.database = database.String
	return c, nil
}

// loginError names the cause of a failed login.
func loginError(cfg Config, err error) error {
	user := cfg.Credentials.Username
	notFound := synclet.ConfigError(fmt.Errorf("account %q not found: use the account identifier orgname-accountname or the account locator: %w", cfg.Account, err))
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return notFound
	}
	var se *sf.SnowflakeError
	if !errors.As(err, &se) {
		return fmt.Errorf("log in to account %q: %w", cfg.Account, err)
	}
	switch se.Number {
	case codeIncorrectLogin, codeInvalidJWT:
		return synclet.ConfigError(fmt.Errorf("login rejected for user %q: private_key does not match the user's RSA_PUBLIC_KEY, or the user does not exist: %w", user, err))
	case sf.ErrCodeFailedToConnect:
		return notFound
	case codeRoleNotGranted:
		role := cfg.Role
		if role == "" {
			role = roleInMessage(se.Message)
		}
		return synclet.ConfigError(fmt.Errorf("role %q is not granted to user %q: %w", role, user, err))
	case codeObjectNotAllowed:
		return synclet.ConfigError(fmt.Errorf("database %q or warehouse %q does not exist or is not authorized for user %q: %w", cfg.Database, cfg.Warehouse, user, err))
	}
	if se.Number >= 390000 && se.Number < 400000 {
		return synclet.ConfigError(fmt.Errorf("login rejected for user %q: %w", user, err))
	}
	return fmt.Errorf("log in to account %q: %w", cfg.Account, err)
}

// roleInMessage returns the quoted role name of a 390189 message.
func roleInMessage(msg string) string {
	var role string
	if _, err := fmt.Sscanf(msg, "Role '%s", &role); err == nil {
		if i := len(role) - 1; i >= 0 && role[i] == '\'' {
			return role[:i]
		}
	}
	return ""
}

// errCode is the Snowflake error number of err, or 0.
func errCode(err error) int {
	var se *sf.SnowflakeError
	if errors.As(err, &se) {
		return se.Number
	}
	return 0
}

// sessionError explains err from a statement of the session, or returns it.
func (c *conn) sessionError(err error) error {
	if errCode(err) == codeNoWarehouse {
		return synclet.ConfigError(fmt.Errorf("warehouse %q cannot run queries for role %q: it is missing, suspended without AUTO_RESUME, or the role has no USAGE privilege on it: %w", c.cfg.Warehouse, c.role, err))
	}
	return err
}

func (c *conn) exec(ctx context.Context, query string) error {
	_, err := c.c.ExecContext(ctx, query)
	return c.sessionError(err)
}

// schemaRef is the quoted name of the configured schema.
func (c *conn) schemaRef() string { return ident(c.database, c.schema) }

// tableRef is the quoted name of table in the configured schema.
func (c *conn) tableRef(table string) string { return ident(c.database, c.schema, table) }

// ensureSchema creates the configured schema when it does not exist.
func (c *conn) ensureSchema(ctx context.Context) error {
	var n int
	q := `SELECT COUNT(*) FROM ` + ident(c.database) + `.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ` + sqlString(c.schema)
	if err := c.c.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return fmt.Errorf("look up schema %s: %w", c.schemaRef(), c.sessionError(err))
	}
	if n > 0 {
		return nil
	}
	if err := c.exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+c.schemaRef()); err != nil {
		if errCode(err) == codeNoPrivilege {
			return synclet.ConfigError(fmt.Errorf("schema %q does not exist and role %q has no CREATE SCHEMA privilege on database %q: %w", c.schema, c.role, c.database, err))
		}
		return fmt.Errorf("create schema %s: %w", c.schemaRef(), err)
	}
	return nil
}

// createError explains err from a CREATE TABLE in the configured schema.
func (c *conn) createError(table string, err error) error {
	switch errCode(err) {
	case codeNoPrivilege:
		return synclet.ConfigError(fmt.Errorf("role %q has no CREATE TABLE privilege on schema %s: %w", c.role, c.schemaRef(), err))
	case codeNotAuthorized:
		return synclet.ConfigError(fmt.Errorf("role %q has no USAGE privilege on schema %s: %w", c.role, c.schemaRef(), err))
	}
	return fmt.Errorf("create table %s: %w", c.tableRef(table), err)
}

// sqlString is s as a SQL string literal.
func sqlString(s string) string {
	out := []byte{'\''}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			out = append(out, '\'', '\'')
		case '\\':
			out = append(out, '\\', '\\')
		default:
			out = append(out, s[i])
		}
	}
	return string(append(out, '\''))
}
