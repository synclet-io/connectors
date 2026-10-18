package main

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/multierr"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// MySQLSource implements synclet.Source for MySQL databases.
type MySQLSource struct{}

// NewMySQLSource creates a new MySQL source connector.
func NewMySQLSource() *MySQLSource {
	return &MySQLSource{}
}

// Spec returns the connector specification defining config fields.
func (s *MySQLSource) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://docs.synclet.dev/connectors/source-mysql",
		SupportsIncremental: true,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "MySQL Source Spec",
			Description: "Reads data from a MySQL database",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "database", "username", "password"},
			Properties: synclet.Properties{
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"host": {
						Title:       "Host",
						Description: "Hostname of the MySQL server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"port": {
						Title:       "Port",
						Description: "Port of the MySQL server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.Integer},
						},
						Default: 3306,
					},
					"database": {
						Title:       "Database",
						Description: "Name of the MySQL database to sync",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"username": {
						Title:       "Username",
						Description: "MySQL username",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"password": {
						Title:       "Password",
						Description: "MySQL password",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
					"ssl_mode":           sslModeSpec(),
					"tunnel_method":      tunnelMethodSpec(),
					"replication_method": replicationMethodSpec(),
					"table_filter":       tableFilterSpec(),
				},
			},
		},
		ProtocolVersion: "0.5.2",
	}, nil
}

// Check validates the configuration by attempting to connect to the MySQL server.
func (s *MySQLSource) Check(ctx context.Context, cfg Config) (rerr error) {
	cfg.applyDefaults()
	log := synclet.Logger(ctx)

	db, tunnelCloser, err := openDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(tunnelCloser))
	defer multierr.AppendInvoke(&rerr, multierr.Close(db))

	log.Info("Successfully connected to MySQL")

	if cfg.Replication.Method == "CDC" {
		if err := checkCDCRequirements(ctx, db); err != nil {
			return fmt.Errorf("CDC requirements not met: %w", err)
		}
		log.Info("CDC requirements verified")
	}

	return nil
}

// checkCDCRequirements verifies MySQL is configured for CDC replication.
func checkCDCRequirements(ctx context.Context, db *sql.DB) (rerr error) {
	val, err := showVariable(ctx, db, "log_bin")
	if err != nil {
		return fmt.Errorf("checking log_bin: %w", err)
	}
	if val != "ON" {
		return fmt.Errorf("binary logging is not enabled (log_bin=%s, expected ON)", val)
	}

	val, err = showVariable(ctx, db, "binlog_format")
	if err != nil {
		return fmt.Errorf("checking binlog_format: %w", err)
	}
	if val != "ROW" {
		return fmt.Errorf("binlog_format is %s, expected ROW", val)
	}

	val, err = showVariable(ctx, db, "binlog_row_image")
	if err != nil {
		return fmt.Errorf("checking binlog_row_image: %w", err)
	}
	if val != "FULL" {
		return fmt.Errorf("binlog_row_image is %s, expected FULL", val)
	}

	rows, err := db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return fmt.Errorf("executing SHOW MASTER STATUS: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))

	if !rows.Next() {
		return fmt.Errorf("SHOW MASTER STATUS returned no rows; binary logging may not be active")
	}

	return nil
}

// showVariable queries a MySQL system variable and returns its value.
func showVariable(ctx context.Context, db *sql.DB, name string) (string, error) {
	var varName, value string
	// SHOW does not take placeholders in a prepared statement; name is always a constant.
	row := db.QueryRowContext(ctx, fmt.Sprintf("SHOW VARIABLES LIKE '%s'", name))
	if err := row.Scan(&varName, &value); err != nil {
		return "", err
	}
	return value, nil
}

// Discover returns a catalog describing all available streams (tables) in the database.
func (s *MySQLSource) Discover(ctx context.Context, cfg Config) (_ *synclet.Catalog, rerr error) {
	cfg.applyDefaults()

	db, tunnelCloser, err := openDB(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(tunnelCloser))
	defer multierr.AppendInvoke(&rerr, multierr.Close(db))

	tables, err := discoverTables(ctx, db, cfg)
	if err != nil {
		return nil, fmt.Errorf("discovering tables: %w", err)
	}

	var streams []synclet.Stream
	for _, t := range tables {

		syncModes := []synclet.SyncMode{synclet.SyncModeFullRefresh}
		if cfg.Replication.Method == "CDC" || len(t.PrimaryKey) > 0 {
			syncModes = append(syncModes, synclet.SyncModeIncremental)
		}

		stream := synclet.Stream{
			Name:               t.Name,
			Namespace:          t.Schema,
			JSONSchema:         synclet.ObjectSchema(buildStreamSchema(t)),
			SupportedSyncModes: syncModes,
		}

		if len(t.PrimaryKey) > 0 {
			var pk [][]string
			for _, col := range t.PrimaryKey {
				pk = append(pk, []string{col})
			}
			stream.SourceDefinedPrimaryKey = pk
		}

		if cfg.Replication.Method == "CDC" {
			stream.SourceDefinedCursor = true
		}

		streams = append(streams, stream)
	}

	return &synclet.Catalog{Streams: streams}, nil
}

// Read extracts data from the MySQL database based on the configured catalog.
func (s *MySQLSource) Read(
	ctx context.Context,
	cfg Config,
	configuredCat *synclet.ConfiguredCatalog,
	prevState *synclet.InputState,
	em *synclet.Emitter,
) (rerr error) {
	cfg.applyDefaults()
	log := synclet.Logger(ctx)

	db, tunnelCloser, err := openDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(tunnelCloser))
	defer multierr.AppendInvoke(&rerr, multierr.Close(db))

	// Discover table metadata for configured streams
	allTables, err := discoverTables(ctx, db, cfg)
	if err != nil {
		return fmt.Errorf("discovering tables for read: %w", err)
	}
	tableMap := make(map[string]TableInfo, len(allTables))
	for _, t := range allTables {
		tableMap[t.Name] = t
	}

	// CDC mode reads all streams together via binlog
	if cfg.Replication.Method == "CDC" {
		var tables []TableInfo
		for _, cs := range configuredCat.Streams {
			if t, ok := tableMap[cs.Stream.Name]; ok {
				tables = append(tables, t)
			}
		}

		prevCDCState, err := loadCDCState(prevState)
		if err != nil {
			log.Warn(fmt.Sprintf("Failed to load CDC state, starting fresh: %v", err))
		}

		reader := NewCDCReader(db, cfg, em)
		return reader.Read(ctx, tables, prevCDCState)
	}

	// Standard mode: iterate streams individually
	for _, cs := range configuredCat.Streams {
		streamName := cs.Stream.Name

		table, ok := tableMap[streamName]
		if !ok {
			log.Warn(fmt.Sprintf("Table %s not found, skipping", streamName))
			continue
		}

		log.Info(fmt.Sprintf("Reading stream: %s (mode: %s)", streamName, cs.SyncMode))

		switch cs.SyncMode {
		case synclet.SyncModeFullRefresh:
			// Try partitioned read if applicable
			if len(table.PrimaryKey) == 1 && MaxPartitionWorkers > 1 && table.DataLength > 100*1024*1024 {
				reader := NewPartitionedReader(db, em)
				if err := reader.ReadTable(ctx, table, MaxPartitionWorkers); err != nil {
					return fmt.Errorf("partitioned read of %s failed: %w", streamName, err)
				}
			} else {
				var prevFRState *FullRefreshState
				if raw := prevState.Stream(streamDescriptor(streamName)); raw != nil {
					prevFRState = &FullRefreshState{}
					parseStreamState(ctx, streamName, raw, prevFRState)
				}
				reader := NewFullRefreshReader(db, em)
				if err := reader.ReadTable(ctx, table, prevFRState); err != nil {
					return fmt.Errorf("full refresh read of %s failed: %w", streamName, err)
				}
			}

		case synclet.SyncModeIncremental:
			cursorField := ""
			if len(cs.CursorField) > 0 {
				cursorField = cs.CursorField[0]
			}
			if cursorField == "" {
				return fmt.Errorf("incremental sync requires cursor_field for stream %s", streamName)
			}

			var prevIncState *IncrementalState
			if raw := prevState.Stream(streamDescriptor(streamName)); raw != nil {
				prevIncState = &IncrementalState{}
				parseStreamState(ctx, streamName, raw, prevIncState)
			}

			reader := NewIncrementalReader(db, em)
			if err := reader.ReadTable(ctx, table, cursorField, prevIncState); err != nil {
				return fmt.Errorf("incremental read of %s failed: %w", streamName, err)
			}

		default:
			return fmt.Errorf("unsupported sync mode: %s", cs.SyncMode)
		}

		log.Info(fmt.Sprintf("Completed stream: %s", streamName))
	}

	return nil
}
