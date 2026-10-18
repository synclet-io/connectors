package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/crypto/ssh"
)

const (
	connectTimeout      = 30 * time.Second
	serverSelectTimeout = 30 * time.Second

	schemeStandard = "mongodb://"
	schemeSRV      = "mongodb+srv://"
)

// MongoDB error codes the connector tells apart.
const (
	codeUnauthorized         = 13
	codeAuthenticationFailed = 18
)

// client is a connected driver client, with the SSH client it runs over
// when the configuration asks for a tunnel.
type client struct {
	*mongo.Client
	ssh  *ssh.Client
	cfg  Config
	user string
}

func (c *client) close() {
	if c.Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Disconnect(ctx)
	}
	if c.ssh != nil {
		_ = c.ssh.Close()
	}
}

func (c *client) db() *mongo.Database { return c.Database(c.cfg.DatabaseConfig.Database) }

// who names the login in a message.
func (c *client) who() string {
	if c.user == "" {
		return "the unauthenticated connection"
	}
	return fmt.Sprintf("user %q", c.user)
}

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func (f dialFunc) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return f(ctx, network, addr)
}

// uriHosts returns the host part of a connection string: the comma
// separated host list of a mongodb:// URI or the host name of a
// mongodb+srv:// URI.
func uriHosts(uri string) string {
	rest := strings.TrimPrefix(strings.TrimPrefix(uri, schemeSRV), schemeStandard)
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		rest = rest[:i]
	}
	if i := strings.LastIndex(rest, "@"); i >= 0 {
		rest = rest[i+1:]
	}
	return rest
}

// uriQuery returns the options of a connection string.
func uriQuery(uri string) url.Values {
	_, query, ok := strings.Cut(uri, "?")
	if !ok {
		return url.Values{}
	}
	v, err := url.ParseQuery(query)
	if err != nil {
		return url.Values{}
	}
	return v
}

// uriPassword returns the password in the user info of a connection string.
func uriPassword(uri string) string {
	rest := strings.TrimPrefix(strings.TrimPrefix(uri, schemeSRV), schemeStandard)
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		rest = rest[:i]
	}
	i := strings.LastIndex(rest, "@")
	if i < 0 {
		return ""
	}
	_, pass, _ := strings.Cut(rest[:i], ":")
	return pass
}

// redact removes the passwords of cfg from msg.
func redact(cfg Config, msg string) string {
	for _, secret := range []string{cfg.DatabaseConfig.Password, uriPassword(cfg.DatabaseConfig.ConnectionString)} {
		if secret != "" {
			msg = strings.ReplaceAll(msg, secret, "****")
		}
	}
	return msg
}

// clientOptions builds the driver options from cfg alone. User name and
// password of the configuration override those of the connection string;
// auth_source applies when set, else the connection string's authSource,
// else admin.
func (s Source) clientOptions(cfg Config) (*options.ClientOptions, error) {
	d := cfg.DatabaseConfig
	opts := options.Client().ApplyURI(d.ConnectionString)
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	opts.SetAppName("synclet-source-mongodb")
	opts.SetConnectTimeout(connectTimeout)
	timeout := s.selectTimeout
	if timeout == 0 {
		timeout = serverSelectTimeout
	}
	opts.SetServerSelectionTimeout(timeout)
	if d.Username != "" {
		cred := options.Credential{AuthSource: uriQuery(d.ConnectionString).Get("authSource")}
		if opts.Auth != nil {
			cred = *opts.Auth
		}
		cred.Username = d.Username
		cred.Password = d.Password
		cred.PasswordSet = d.Password != ""
		opts.SetAuth(cred)
	}
	if opts.Auth != nil {
		switch {
		case d.AuthSource != "":
			opts.Auth.AuthSource = d.AuthSource
		case opts.Auth.AuthSource == "":
			opts.Auth.AuthSource = "admin"
		}
	}
	if pemCA := cfg.TLSCACertificate; pemCA != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(pemCA)) {
			return nil, errors.New("tls_ca_certificate holds no PEM certificate")
		}
		tc := opts.TLSConfig
		if tc == nil {
			tc = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tc.RootCAs = roots
		opts.SetTLSConfig(tc)
	}
	if s.dial != nil {
		opts.SetDialer(dialFunc(s.dial))
	}
	return opts, nil
}

// connect validates cfg, connects and pings the servers.
func (s Source) connect(ctx context.Context, cfg Config) (*client, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c := &client{cfg: cfg}
	opts, err := s.clientOptions(cfg)
	if err != nil {
		return nil, c.connectError(err)
	}
	if opts.Auth != nil {
		c.user = opts.Auth.Username
	}
	if cfg.TunnelMethod.TunnelMethod != tunnelNone {
		sc, err := dialSSH(ctx, cfg.TunnelMethod)
		if err != nil {
			return nil, err
		}
		c.ssh = sc
		opts.SetDialer(tunnelDialer(sc))
	}
	mc, err := mongo.Connect(opts)
	if err != nil {
		c.close()
		return nil, c.connectError(err)
	}
	c.Client = mc
	if err := mc.Ping(ctx, nil); err != nil {
		err = c.connectError(err)
		c.close()
		return nil, err
	}
	return c, nil
}

// connectError names the cause of a failed connection. Its message never
// holds the connection string or a password.
func (c *client) connectError(err error) error {
	cfg := c.cfg
	uri := cfg.DatabaseConfig.ConnectionString
	hosts := uriHosts(uri)
	msg := redact(cfg, err.Error())
	fail := func(config bool, format string, args ...any) error {
		e := fmt.Errorf(format, args...)
		if config {
			return synclet.ConfigError(e)
		}
		return e
	}
	var se mongo.ServerError
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &se) && se.HasErrorCode(codeAuthenticationFailed),
		strings.Contains(msg, "AuthenticationFailed"), strings.Contains(msg, "auth error"):
		return fail(true, "cannot log in as %q: wrong user name or password", c.user)
	case strings.HasPrefix(uri, schemeSRV) && (errors.As(err, &dnsErr) || strings.Contains(msg, "lookup ")) && !strings.Contains(msg, "server selection"):
		return fail(false, "cannot resolve SRV record _mongodb._tcp.%s: %s", hosts, msg)
	case isCertError(err):
		return fail(true, "server certificate of %s is not trusted: %s", hosts, msg)
	case strings.Contains(msg, "error parsing uri"), strings.Contains(msg, "error validating uri"):
		return fail(true, "database_config.connection_string: %s", msg)
	case mongo.IsTimeout(err), mongo.IsNetworkError(err), errors.Is(err, context.DeadlineExceeded),
		strings.Contains(msg, "server selection"), errors.As(err, &dnsErr):
		return fail(false, "cannot reach %s: %s", hosts, msg)
	}
	return fail(false, "connect to %s: %s", hosts, msg)
}

// isCertError reports whether err is a failed check of a server
// certificate. A server selection error carries it as text.
func isCertError(err error) bool {
	var certErr *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if errors.As(err, &certErr) || errors.As(err, &unknownCA) || errors.As(err, &hostErr) {
		return true
	}
	return strings.Contains(err.Error(), "x509: ")
}
