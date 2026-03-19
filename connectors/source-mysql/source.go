package main

import (
	"context"
	"database/sql"
	"fmt"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// MySQLSource implements airbyte.Source for MySQL databases.
type MySQLSource struct{}

// NewMySQLSource creates a new MySQL source connector.
func NewMySQLSource() *MySQLSource {
	return &MySQLSource{}
}

// Spec returns the connector specification defining config fields.
func (s *MySQLSource) Spec(logTracker airbyte.LogTracker) (*airbyte.ConnectorSpecification, error) {
	return &airbyte.ConnectorSpecification{
		DocumentationURL:    "https://docs.synclet.dev/connectors/source-mysql",
		SupportsIncremental: true,
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
			airbyte.DestinationSyncModeAppend,
			airbyte.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: airbyte.ConnectionSpecification{
			Title:       "MySQL Source Spec",
			Description: "Reads data from a MySQL database",
			Type:        "object",
			Required:    []airbyte.PropertyName{"host", "database", "username", "password"},
			Properties: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"host": {
						Title:       "Host",
						Description: "Hostname of the MySQL server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"port": {
						Title:       "Port",
						Description: "Port of the MySQL server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Default: 3306,
					},
					"database": {
						Title:       "Database",
						Description: "Name of the MySQL database to sync",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"username": {
						Title:       "Username",
						Description: "MySQL username",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"password": {
						Title:       "Password",
						Description: "MySQL password",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
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
func (s *MySQLSource) Check(srcCfgPath string, logTracker airbyte.LogTracker) error {
	var cfg Config
	if err := airbyte.UnmarshalFromPath(srcCfgPath, &cfg); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	cfg.applyDefaults()

	ctx := context.Background()
	db, tunnelCloser, err := openDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()
	defer tunnelCloser.Close()

	logTracker.Log(airbyte.LogLevelInfo, "Successfully connected to MySQL")

	if cfg.Replication.Method == "CDC" {
		if err := checkCDCRequirements(db); err != nil {
			return fmt.Errorf("CDC requirements not met: %w", err)
		}
		logTracker.Log(airbyte.LogLevelInfo, "CDC requirements verified")
	}

	return nil
}

// checkCDCRequirements verifies MySQL is configured for CDC replication.
func checkCDCRequirements(db *sql.DB) error {
	val, err := showVariable(db, "log_bin")
	if err != nil {
		return fmt.Errorf("checking log_bin: %w", err)
	}
	if val != "ON" {
		return fmt.Errorf("binary logging is not enabled (log_bin=%s, expected ON)", val)
	}

	val, err = showVariable(db, "binlog_format")
	if err != nil {
		return fmt.Errorf("checking binlog_format: %w", err)
	}
	if val != "ROW" {
		return fmt.Errorf("binlog_format is %s, expected ROW", val)
	}

	val, err = showVariable(db, "binlog_row_image")
	if err != nil {
		return fmt.Errorf("checking binlog_row_image: %w", err)
	}
	if val != "FULL" {
		return fmt.Errorf("binlog_row_image is %s, expected FULL", val)
	}

	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		return fmt.Errorf("executing SHOW MASTER STATUS: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return fmt.Errorf("SHOW MASTER STATUS returned no rows; binary logging may not be active")
	}

	return nil
}

// showVariable queries a MySQL system variable and returns its value.
func showVariable(db *sql.DB, name string) (string, error) {
	var varName, value string
	row := db.QueryRow("SHOW VARIABLES LIKE ?", name)
	if err := row.Scan(&varName, &value); err != nil {
		return "", err
	}
	return value, nil
}

// Discover returns a catalog describing all available streams (tables) in the database.
func (s *MySQLSource) Discover(srcCfgPath string, logTracker airbyte.LogTracker) (*airbyte.Catalog, error) {
	var cfg Config
	if err := airbyte.UnmarshalFromPath(srcCfgPath, &cfg); err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}
	cfg.applyDefaults()

	ctx := context.Background()
	db, tunnelCloser, err := openDB(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()
	defer tunnelCloser.Close()

	tables, err := discoverTables(ctx, db, cfg)
	if err != nil {
		return nil, fmt.Errorf("discovering tables: %w", err)
	}

	var streams []airbyte.Stream
	for _, t := range tables {
		schema := buildStreamSchema(t)

		syncModes := []airbyte.SyncMode{airbyte.SyncModeFullRefresh}
		if cfg.Replication.Method == "CDC" || len(t.PrimaryKey) > 0 {
			syncModes = append(syncModes, airbyte.SyncModeIncremental)
		}

		stream := airbyte.Stream{
			Name:               t.Name,
			Namespace:          t.Schema,
			JSONSchema:         schema,
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

	return &airbyte.Catalog{Streams: streams}, nil
}

// Read extracts data from the MySQL database based on the configured catalog.
func (s *MySQLSource) Read(
	srcCfgPath string,
	prevStatePath string,
	configuredCat *airbyte.ConfiguredCatalog,
	tracker airbyte.MessageTracker,
) error {
	var cfg Config
	if err := airbyte.UnmarshalFromPath(srcCfgPath, &cfg); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	cfg.applyDefaults()

	ctx := context.Background()
	db, tunnelCloser, err := openDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()
	defer tunnelCloser.Close()

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

		prevCDCState, err := loadCDCState(prevStatePath)
		if err != nil {
			tracker.Log(airbyte.LogLevelWarn, fmt.Sprintf("Failed to load CDC state, starting fresh: %v", err))
		}

		reader := NewCDCReader(db, cfg, tracker)
		return reader.Read(ctx, tables, prevCDCState)
	}

	// Standard mode: iterate streams individually
	for _, cs := range configuredCat.Streams {
		streamName := cs.Stream.Name

		table, ok := tableMap[streamName]
		if !ok {
			tracker.Log(airbyte.LogLevelWarn, fmt.Sprintf("Table %s not found, skipping", streamName))
			continue
		}

		tracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("Reading stream: %s (mode: %s)", streamName, cs.SyncMode))

		switch cs.SyncMode {
		case airbyte.SyncModeFullRefresh:
			// Try partitioned read if applicable
			if len(table.PrimaryKey) == 1 && MaxPartitionWorkers > 1 && table.DataLength > 100*1024*1024 {
				reader := NewPartitionedReader(db, tracker)
				if err := reader.ReadTable(ctx, table, MaxPartitionWorkers); err != nil {
					return fmt.Errorf("partitioned read of %s failed: %w", streamName, err)
				}
			} else {
				var prevFRState *FullRefreshState
				raw, _ := loadStreamState(prevStatePath, streamName)
				if raw != nil {
					prevFRState = &FullRefreshState{}
					_ = parseStreamState(raw, prevFRState)
				}
				reader := NewFullRefreshReader(db, tracker)
				if err := reader.ReadTable(ctx, table, prevFRState); err != nil {
					return fmt.Errorf("full refresh read of %s failed: %w", streamName, err)
				}
			}

		case airbyte.SyncModeIncremental:
			cursorField := ""
			if len(cs.CursorField) > 0 {
				cursorField = cs.CursorField[0]
			}
			if cursorField == "" {
				return fmt.Errorf("incremental sync requires cursor_field for stream %s", streamName)
			}

			var prevIncState *IncrementalState
			raw, _ := loadStreamState(prevStatePath, streamName)
			if raw != nil {
				prevIncState = &IncrementalState{}
				_ = parseStreamState(raw, prevIncState)
			}

			reader := NewIncrementalReader(db, tracker)
			if err := reader.ReadTable(ctx, table, cursorField, prevIncState); err != nil {
				return fmt.Errorf("incremental read of %s failed: %w", streamName, err)
			}

		default:
			return fmt.Errorf("unsupported sync mode: %s", cs.SyncMode)
		}

		tracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("Completed stream: %s", streamName))
	}

	return nil
}
