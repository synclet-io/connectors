package main

import (
	"context"
	"encoding/json"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// propBoolean is the JSON schema boolean type, which the SDK has no constant for.
const propBoolean synclet.PropType = "boolean"

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

type props = map[synclet.PropertyName]synclet.PropertySpec

func constProp(v string) synclet.PropertySpec {
	return synclet.PropertySpec{PropertyType: typed(synclet.String), Const: v, Order: order(0)}
}

func option(title, description string, required []synclet.PropertyName, p props) synclet.PropertySpec {
	return synclet.PropertySpec{
		Title: title, Description: description, PropertyType: typed(synclet.Object),
		Properties: p, Required: required,
	}
}

func credentialOptions() []synclet.PropertySpec {
	keyID := func(o int) synclet.PropertySpec {
		return synclet.PropertySpec{
			Title: "Access Key ID", Description: "Access key ID of the IAM user or the S3-compatible service.",
			PropertyType: typed(synclet.String), IsSecret: true, Order: order(o),
		}
	}
	secret := func(o int) synclet.PropertySpec {
		return synclet.PropertySpec{
			Title: "Secret Access Key", Description: "Secret of the access key.",
			PropertyType: typed(synclet.String), IsSecret: true, Order: order(o),
		}
	}
	return []synclet.PropertySpec{
		option("Access Key", "A static access key.",
			[]synclet.PropertyName{"auth_type", "access_key_id", "secret_access_key"},
			props{"auth_type": constProp(authAccessKey), "access_key_id": keyID(1), "secret_access_key": secret(2)}),
		option("Default Credential Chain", "The AWS SDK default chain: environment variables, shared config files, web identity (EKS IRSA), ECS task role or EC2 instance profile.",
			[]synclet.PropertyName{"auth_type"},
			props{"auth_type": constProp(authDefaultChain)}),
		option("Assume Role", "Assumes an IAM role through STS. The call is signed with the access key when one is given, with the default credential chain otherwise.",
			[]synclet.PropertyName{"auth_type", "role_arn"},
			props{
				"auth_type": constProp(authAssumeRole),
				"role_arn": {
					Title: "Role ARN", Description: "ARN of the role to assume.",
					PropertyType: typed(synclet.String), Examples: []string{"arn:aws:iam::123456789012:role/synclet-reader"}, Order: order(1),
				},
				"external_id": {
					Title: "External ID", Description: "External ID the role's trust policy requires, if any.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
				},
				"role_session_name": {
					Title: "Role Session Name", Description: "Session name recorded in CloudTrail.",
					PropertyType: typed(synclet.String), Default: defaultSessionName, Order: order(3),
				},
				"access_key_id":     keyID(4),
				"secret_access_key": secret(5),
			}),
	}
}

func delimitedOption(title, filetype, delimiter, quote, description string) synclet.PropertySpec {
	return option(title, description, []synclet.PropertyName{"filetype"}, props{
		"filetype": constProp(filetype),
		"delimiter": {
			Title: "Delimiter", Description: "Character between fields.",
			PropertyType: typed(synclet.String), Default: delimiter, Order: order(1),
		},
		"quote_char": {
			Title: "Quote Character", Description: "Character that quotes a field holding the delimiter, line breaks or quotes; a doubled quote inside stands for one. Empty: fields are never quoted.",
			PropertyType: typed(synclet.String), Default: quote, Order: order(2),
		},
		"escape_char": {
			Title: "Escape Character", Description: "Character that makes the next character literal, inside or outside quotes. Empty: none.",
			PropertyType: typed(synclet.String), Default: "", Order: order(3),
		},
		"header": {
			Title: "Header Row", Description: "Whether the first row (after skip_rows) names the columns.",
			PropertyType: typed(propBoolean), Default: true, Order: order(4),
		},
		"column_names": {
			Title: "Column Names", Description: "Column names in file order. Required without a header row; with one they replace its names.",
			PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"}, Order: order(5),
		},
		"skip_rows": {
			Title: "Skip Rows", Description: "Lines to skip at the start of each file, before the header.",
			PropertyType: typed(synclet.Integer), Default: 0, Order: order(6),
		},
		"null_values": {
			Title: "Null Values", Description: "Unquoted values read as null. A quoted value is never null.",
			PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"}, Default: []string{""}, Order: order(7),
		},
	})
}

func formatOptions() []synclet.PropertySpec {
	return []synclet.PropertySpec{
		delimitedOption("CSV", formatCSV, ",", `"`, "Delimited text with a header row; values with the delimiter, quotes or line breaks are quoted."),
		delimitedOption("TSV", formatTSV, "\t", "", "Tab-separated text; by default no quoting, so a field holds no tab or line break."),
		option("JSONL", "One JSON object per line.", []synclet.PropertyName{"filetype"}, props{"filetype": constProp(formatJSONL)}),
		option("Parquet", "Apache Parquet files.", []synclet.PropertyName{"filetype"}, props{"filetype": constProp(formatParquet)}),
	}
}

// streamItems is the JSON schema of one entry of streams.
func streamItems() map[string]any {
	item := option("Stream", "", []synclet.PropertyName{"name", "glob", "format"}, props{
		"name": {
			Title: "Name", Description: "Stream name.",
			PropertyType: typed(synclet.String), Order: order(0),
		},
		"glob": {
			Title: "Glob", Description: "Object keys the stream reads: * matches within one path segment, ** across segments, ? one character, {a,b} alternatives. The part before the first wildcard is the listing prefix.",
			PropertyType: typed(synclet.String), Examples: []string{"exports/**/*.csv", "events/2026-*/part-*.parquet"}, Order: order(1),
		},
		"format": {
			Title: "Format", Description: "File format of the stream's objects.",
			PropertyType: typed(synclet.Object), OneOf: formatOptions(), Order: order(2),
		},
		"compression": {
			Title: "Compression", Description: "auto: by the key's extension (.gz, .zst), then by the first bytes; none, gzip or zstd: always that.",
			PropertyType: typed(synclet.String), Default: compressionAuto, Order: order(3),
		},
		"schema": {
			Title: "Schema", Description: "JSON schema of the records, an object schema with properties. Empty: inferred from the newest files.",
			PropertyType: typed(synclet.String), Examples: []string{`{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":["null","string"]}}}`}, Order: order(4),
		},
		"schema_sample_files": {
			Title: "Schema Sample Files", Description: "How many of the newest matching files the schema is inferred from.",
			PropertyType: typed(synclet.Integer), Default: defaultSampleFiles, Order: order(5),
		},
		"skip_bad_rows": {
			Title: "Skip Bad Rows", Description: "Skip rows that cannot be parsed or do not fit the schema, and log how many per file, instead of failing the read.",
			PropertyType: typed(propBoolean), Default: false, Order: order(6),
		},
	})
	b, err := json.Marshal(item)
	if err != nil {
		panic(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		panic(err)
	}
	m["properties"].(map[string]any)["compression"].(map[string]any)["enum"] = compressions
	return m
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-s3",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "S3 Source Spec",
			Description: "Reads CSV, TSV, JSONL and Parquet files from S3-compatible storage, one stream per glob: full refresh, or incremental by object modification time.",
			Type:        "object",
			Required:    []synclet.PropertyName{"bucket", "credentials", "streams"},
			Properties: synclet.Properties{Properties: props{
				"bucket": {
					Title: "Bucket", Description: "Name of the bucket.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"region": {
					Title: "Region", Description: "Region of the bucket.",
					PropertyType: typed(synclet.String), Default: defaultRegion, Order: order(1),
				},
				"endpoint": {
					Title: "Endpoint", Description: "Endpoint of an S3-compatible service (MinIO, Cloudflare R2, ...). Empty: AWS S3.",
					PropertyType: typed(synclet.String), Examples: []string{"https://<account>.r2.cloudflarestorage.com", "http://minio:9000"}, Order: order(2),
				},
				"force_path_style": {
					Title: "Path-Style Addressing", Description: "Address the bucket as endpoint/bucket instead of bucket.endpoint, as MinIO usually needs.",
					PropertyType: typed(propBoolean), Default: false, Order: order(3),
				},
				"credentials": {
					Title: "Credentials", Description: "How requests are signed.",
					PropertyType: typed(synclet.Object), OneOf: credentialOptions(), Order: order(4),
				},
				"streams": {
					Title: "Streams", Description: "One stream per glob.",
					PropertyType: typed(synclet.Array), Items: streamItems(), Order: order(5),
				},
			}},
		},
	}, nil
}
