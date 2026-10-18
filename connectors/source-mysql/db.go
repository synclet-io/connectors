package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/go-sql-driver/mysql"
)

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// openDB creates a MySQL database connection with optional SSH tunnel and TLS.
// Returns the *sql.DB, an io.Closer for cleanup (tunnel if any), and any error.
func openDB(ctx context.Context, cfg Config) (*sql.DB, io.Closer, error) {
	var closer io.Closer = nopCloser{}
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	// Set up SSH tunnel if configured.
	if cfg.Tunnel.Method != "" && cfg.Tunnel.Method != "no_tunnel" {
		tunnel, err := newSSHTunnel(cfg.Tunnel, cfg.Host, cfg.Port)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create SSH tunnel: %w", err)
		}
		closer = tunnel
		addr = tunnel.LocalAddr()
	}

	// Build TLS config if needed.
	tlsCfg, err := buildTLSConfig(cfg.SSL)
	if err != nil {
		closer.Close()

		return nil, nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	if tlsCfg != nil {
		if err := mysql.RegisterTLSConfig("custom", tlsCfg); err != nil {
			closer.Close()
			return nil, nil, fmt.Errorf("failed to register TLS config: %w", err)
		}
	}

	// Build DSN.
	dsnCfg := mysql.Config{
		User:      cfg.Username,
		Passwd:    cfg.Password,
		Net:       "tcp",
		Addr:      addr,
		DBName:    cfg.Database,
		ParseTime: true,
		Params:    map[string]string{"charset": "utf8mb4"},
	}

	if tlsCfg != nil {
		dsnCfg.TLSConfig = "custom"
	} else if cfg.SSL.Mode == "preferred" || cfg.SSL.Mode == "" {
		dsnCfg.TLSConfig = "preferred"
	}

	db, err := sql.Open("mysql", dsnCfg.FormatDSN())
	if err != nil {
		closer.Close()
		return nil, nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		closer.Close()
		return nil, nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	return db, closer, nil
}
