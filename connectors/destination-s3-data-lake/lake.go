package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"golang.org/x/oauth2"
)

// lake is an open REST catalog and the S3 settings every table's files are
// read and written with.
type lake struct {
	cat       *rest.Catalog
	cfg       Config
	aws       aws.Config
	namespace table.Identifier
	ioProps   map[string]string
}

// openLake connects to the catalog. The namespace is split but not checked.
func openLake(ctx context.Context, cfg Config) (*lake, error) {
	ns, err := cfg.namespace()
	if err != nil {
		return nil, err
	}
	awsCfg, err := awsConfig(ctx, cfg.S3)
	if err != nil {
		return nil, err
	}
	cat, err := openCatalog(ctx, cfg.Catalog)
	if err != nil {
		return nil, err
	}
	return &lake{cat: cat, cfg: cfg, aws: awsCfg, namespace: ns, ioProps: ioProperties(cfg.S3)}, nil
}

func openCatalog(ctx context.Context, cc CatalogConfig) (*rest.Catalog, error) {
	var opts []rest.Option
	if cc.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cc.Warehouse))
	}
	if cc.Credential != "" {
		opts = append(opts, rest.WithCredential(cc.Credential))
	}
	if cc.Token != "" {
		opts = append(opts, rest.WithOAuthToken(cc.Token))
	}
	if cc.Scope != "" {
		opts = append(opts, rest.WithScope(cc.Scope))
	}
	cat, err := rest.NewCatalog(ctx, "synclet", cc.URI, opts...)
	if err != nil {
		return nil, catalogError(cc.URI, err)
	}
	return cat, nil
}

// catalogError names the cause of a failed catalog request: the catalog is
// unreachable, or it rejected the credentials.
func catalogError(uri string, err error) error {
	var netErr net.Error
	var retrieve *oauth2.RetrieveError
	switch {
	case errors.Is(err, rest.ErrUnauthorized), errors.Is(err, rest.ErrForbidden),
		errors.Is(err, rest.ErrAuthorizationExpired), errors.Is(err, rest.ErrOAuthError),
		errors.As(err, &retrieve):
		return fmt.Errorf("catalog rejected the credentials: %w", err)
	case errors.As(err, &netErr), isConnError(err):
		return fmt.Errorf("cannot reach catalog at %s: %w", uri, err)
	}
	return fmt.Errorf("catalog at %s: %w", uri, err)
}

func isConnError(err error) bool {
	var opErr *net.OpError
	var dnsErr *net.DNSError
	return errors.As(err, &opErr) || errors.As(err, &dnsErr) || strings.Contains(err.Error(), "connection refused")
}

func (l *lake) close() {
	_ = l.cat.Close()
}

func (l *lake) displayNamespace() string {
	return strings.Join(l.namespace, ".")
}

func (l *lake) ident(name string) table.Identifier {
	return append(append(table.Identifier{}, l.namespace...), name)
}

// checkNamespace fails when the configured namespace does not exist.
func (l *lake) checkNamespace(ctx context.Context) error {
	ok, err := l.cat.CheckNamespaceExists(ctx, l.namespace)
	if err != nil {
		return catalogError(l.cfg.Catalog.URI, err)
	}
	if !ok {
		return fmt.Errorf("namespace %q does not exist; create it in the catalog", l.displayNamespace())
	}
	return nil
}

// wrap gives a table the connector's own file IO, so its files go to the
// configured bucket with the configured credentials whatever file IO
// properties the catalog returns.
func (l *lake) wrap(t *table.Table) *table.Table {
	loc := t.MetadataLocation()
	props := l.ioProps
	awsCfg := l.aws
	fsF := func(ctx context.Context) (icebergio.IO, error) {
		return icebergio.LoadFS(utils.WithAwsConfig(ctx, &awsCfg), props, loc)
	}
	return table.New(t.Identifier(), t.Metadata(), t.MetadataLocation(), fsF, l.cat)
}

// loadTable loads a table; found is false when the catalog has none of that
// name.
func (l *lake) loadTable(ctx context.Context, name string) (t *table.Table, found bool, err error) {
	t, err = l.cat.LoadTable(ctx, l.ident(name))
	if errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("load table %s.%s: %w", l.displayNamespace(), name, err)
	}
	return l.wrap(t), true, nil
}
