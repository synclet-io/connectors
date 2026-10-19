package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	uploadRole = "arn:aws:iam::123456789012:role/loader"
	copyRole   = "arn:aws:iam::123456789012:role/copy"
)

// checkDestination runs Check's SQL on the Postgres container, with COPY
// answered by rec, and stages in MinIO.
func checkDestination(m *minioServer, rec *recorder) Destination {
	d := NewDestination()
	d.s3Endpoint = m.endpoint
	d.open = func(ctx context.Context, cfg Config) (warehouse, error) {
		w, err := connect(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return copyInterceptor{warehouse: w, rec: rec}, nil
	}
	return d
}

func TestCheck(t *testing.T) {
	pg := runPostgres(t)
	m := startMinio(t)
	bucket := m.newBucket(t)
	execSQL(t, pg, `DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN
    CREATE ROLE reader LOGIN PASSWORD 'reader-pw';
  END IF;
END $$`)
	execSQL(t, pg, `CREATE SCHEMA IF NOT EXISTS locked`)
	execSQL(t, pg, `GRANT USAGE ON SCHEMA locked TO reader`)

	base := m.destConfig(bucket)
	base.Host, base.Port, base.Database, base.Username, base.Password, base.SSLMode = pg.Host, pg.Port, pg.Database, pg.Username, pg.Password, pg.SSLMode
	base.Schema = "check_ok"

	t.Run("succeeds, loads the probe with COPY and deletes it", func(t *testing.T) {
		rec := &recorder{}
		require.NoError(t, checkDestination(m, rec).Check(t.Context(), base))
		stmts := rec.log()
		require.Len(t, stmts, 1)
		assert.Regexp(t, `^COPY "_synclet_check" \("v"\) FROM 's3://`+bucket+`/stage/_synclet_check_[0-9a-f-]{36}\.csv' `+
			`ACCESS_KEY_ID '`+m.user+`' SECRET_ACCESS_KEY '`+m.password+`' REGION 'eu-west-1' FORMAT AS CSV$`, stmts[0])
		assert.Empty(t, m.keys(t, bucket))
	})

	readOnlyKey, readOnlySecret := m.restrictedUser(t, bucket, "s3:GetObject", "s3:ListBucket")
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Type>Sender</Type><Code>AccessDenied</Code>` +
			`<Message>User: arn:aws:iam::123456789012:user/stager is not authorized to perform: sts:AssumeRole on resource: ` + uploadRole + `</Message></Error><RequestId>r1</RequestId></ErrorResponse>`))
	}))
	defer sts.Close()

	cases := []struct {
		name    string
		edit    func(c *Config)
		copyErr error
		want    string
	}{
		{name: "wrong credentials", edit: func(c *Config) { c.Password = "wrong" },
			want: `cannot log in as "synclet": wrong user name or password`},
		{name: "unreachable cluster", edit: func(c *Config) { c.Host, c.Port = "127.0.0.1", closedPort(t) },
			want: "cannot reach 127.0.0.1:"},
		{name: "no CREATE privilege", edit: func(c *Config) { c.Username, c.Password, c.Schema = "reader", "reader-pw", "locked" },
			want: `user "reader" has no CREATE privilege on schema "locked"`},
		{name: "missing schema without CREATE on the database", edit: func(c *Config) { c.Username, c.Password, c.Schema = "reader", "reader-pw", "absent" },
			want: `schema "absent" does not exist and user "reader" cannot create it`},
		{name: "staging bucket not writable", edit: func(c *Config) {
			c.UploadingMethod.AccessKeyID, c.UploadingMethod.SecretAccessKey = readOnlyKey, readOnlySecret
		}, want: "S3 staging bucket is not writable: no write permission on s3://" + bucket + "/stage/_synclet_check_"},
		{name: "upload role not assumable", edit: func(c *Config) { c.UploadingMethod.RoleARN = uploadRole },
			want: "cannot assume IAM role " + uploadRole + ":"},
		{name: "COPY role not assumable", edit: func(c *Config) { c.UploadingMethod.IAMRoleARN = copyRole },
			copyErr: &pgconn.PgError{Severity: "ERROR", Code: "XX000",
				Message: "User arn:aws:redshift:eu-west-1:123456789012:dbuser:cluster/loader is not authorized to assume IAM Role " + copyRole},
			want: "the Redshift cluster cannot assume IAM role " + copyRole + ": User arn:aws:redshift:"},
		{name: "COPY role cannot read the bucket", edit: func(c *Config) { c.UploadingMethod.IAMRoleARN = copyRole },
			copyErr: &pgconn.PgError{Severity: "ERROR", Code: "XX000", Message: "S3ServiceException:Access Denied,Status 403,Error AccessDenied"},
			want:    "COPY cannot read s3://" + bucket + "/stage/_synclet_check_"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := base
			c.edit(&cfg)
			rec := &recorder{fail: func(sql string) error {
				if c.copyErr != nil && strings.HasPrefix(sql, "COPY ") {
					return c.copyErr
				}
				return nil
			}}
			d := checkDestination(m, rec)
			d.stsEndpoint = sts.URL
			err := d.Check(t.Context(), cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.Empty(t, m.keys(t, bucket), "no probe is left behind")
		})
	}
}

func TestCheckInvalidConfig(t *testing.T) {
	err := NewDestination().Check(t.Context(), Config{Host: "h", Database: "d", Username: "u",
		UploadingMethod: UploadingMethod{BucketName: "Bad_Bucket", IAMRoleARN: "role/x"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `s3_bucket_name "Bad_Bucket" is not a valid S3 bucket name`)
	assert.Contains(t, err.Error(), `iam_role_arn "role/x" is not an IAM role ARN`)
}
