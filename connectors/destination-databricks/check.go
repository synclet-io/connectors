package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// Check connects to the SQL warehouse, finds the catalog, schema and volume,
// then writes and removes a probe file in the volume and a probe table in
// the schema.
func (d Destination) Check(ctx context.Context, cfg Config) (rerr error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	client := d.client()
	b := newBearer(ctx, cfg, client)
	if _, err := b.token(); err != nil {
		return err
	}
	run, err := d.open(ctx, cfg, client, b)
	if err != nil {
		return err
	}
	defer func() { rerr = errors.Join(rerr, run.close()) }()

	if err := findObjects(ctx, run, cfg); err != nil {
		return err
	}
	files := &filesClient{base: cfg.baseURL(), http: client, bearer: b}
	if err := probeVolume(ctx, files, cfg); err != nil {
		return err
	}
	return probeSchema(ctx, run, cfg)
}

// findObjects fails when the catalog, the schema or the volume does not
// exist or the user cannot see it. Unity Catalog stores names in lower case.
func findObjects(ctx context.Context, run sqlRunner, cfg Config) error {
	cat, schema, vol := strings.ToLower(cfg.Database), strings.ToLower(cfg.Schema), strings.ToLower(cfg.Volume)
	lookups := []struct {
		query   string
		missing string
	}{
		{
			"SELECT 1 FROM system.information_schema.catalogs WHERE catalog_name = " + sqlString(cat),
			fmt.Sprintf("catalog %q does not exist or the user cannot see it (needs USE CATALOG)", cfg.Database),
		},
		{
			"SELECT 1 FROM " + ident(cat) + ".information_schema.schemata WHERE schema_name = " + sqlString(schema),
			fmt.Sprintf("schema %q does not exist or the user cannot see it (needs USE SCHEMA)", cfg.Database+"."+cfg.Schema),
		},
		{
			"SELECT 1 FROM " + ident(cat) + ".information_schema.volumes WHERE volume_schema = " + sqlString(schema) + " AND volume_name = " + sqlString(vol),
			fmt.Sprintf("volume %q does not exist or the user cannot see it (needs READ VOLUME and WRITE VOLUME)", cfg.Database+"."+cfg.Schema+"."+cfg.Volume),
		},
	}
	for _, l := range lookups {
		rows, err := run.query(ctx, l.query)
		if err != nil {
			return describeSQLError(err, "look up "+strings.SplitN(l.missing, " ", 2)[0])
		}
		if len(rows) == 0 {
			return errors.New(l.missing)
		}
	}
	return nil
}

func probeVolume(ctx context.Context, files *filesClient, cfg Config) error {
	probe := cfg.volumePath() + "/_synclet_check_" + uuid.NewString()
	if err := files.put(ctx, probe, []byte("synclet")); err != nil {
		return describeFilesError(err, cfg, "no write privilege on volume "+cfg.volumePath()+" (needs WRITE VOLUME)")
	}
	if err := files.delete(ctx, probe); err != nil {
		return describeFilesError(err, cfg, "no privilege to delete files in volume "+cfg.volumePath()+" (needs WRITE VOLUME)")
	}
	return nil
}

func probeSchema(ctx context.Context, run sqlRunner, cfg Config) error {
	loc := location{catalog: cfg.Database, schema: cfg.Schema}
	probe := loc.table("_synclet_check_" + strings.ReplaceAll(uuid.NewString(), "-", ""))
	if err := run.exec(ctx, "CREATE TABLE "+probe+" (id INT) USING DELTA"); err != nil {
		if permissionDenied(err) {
			return fmt.Errorf("no privilege to create tables in schema %s.%s (needs USE CATALOG, USE SCHEMA, CREATE TABLE and MODIFY): %w", cfg.Database, cfg.Schema, err)
		}
		return fmt.Errorf("create probe table %s: %w", probe, err)
	}
	if err := run.exec(ctx, "DROP TABLE IF EXISTS "+probe); err != nil {
		return fmt.Errorf("drop probe table %s: %w", probe, err)
	}
	return nil
}

// describeFilesError names a rejected token, a missing volume, or denied,
// which is reported as denied.
func describeFilesError(err error, cfg Config, denied string) error {
	var ae *apiError
	if errors.As(err, &ae) {
		switch ae.status {
		case http.StatusUnauthorized:
			return fmt.Errorf("invalid token: the Files API rejected the credentials: %w", err)
		case http.StatusForbidden:
			return fmt.Errorf("%s: %w", denied, err)
		case http.StatusNotFound:
			return fmt.Errorf("volume %s not found: %w", cfg.volumePath(), err)
		}
	}
	return err
}

// describeSQLError names a statement refused for lack of a privilege.
func describeSQLError(err error, what string) error {
	if permissionDenied(err) {
		return fmt.Errorf("%s: permission denied: %w", what, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

func (d Destination) client() *http.Client {
	if d.httpClient != nil {
		return d.httpClient
	}
	return &http.Client{Timeout: httpTimeout}
}

func (d Destination) open(ctx context.Context, cfg Config, client *http.Client, b *bearer) (sqlRunner, error) {
	if d.openSQL != nil {
		return d.openSQL(ctx, cfg, client, b, d.retries)
	}
	return openWarehouse(ctx, cfg, d.httpClient, b, d.retries)
}
