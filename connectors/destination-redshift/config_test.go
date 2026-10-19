package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"host":"h","database":"dev","username":"u","password":"p",
		"uploading_method":{"method":"S3 Staging","s3_bucket_name":"staging","s3_bucket_path":"/a/b/"}}`), &cfg))
	cfg.applyDefaults()
	require.NoError(t, cfg.validate())
	assert.Equal(t, 5439, cfg.Port)
	assert.Equal(t, "public", cfg.Schema)
	assert.Equal(t, sslRequire, cfg.SSLMode.Mode)
	assert.Equal(t, defaultRegion, cfg.UploadingMethod.BucketRegion)
	assert.Equal(t, "a/b", cfg.UploadingMethod.BucketPath)
	assert.True(t, cfg.UploadingMethod.purge())
}

func TestConfigValidate(t *testing.T) {
	cfg := Config{Port: 70000, Schema: "s", SSLMode: SSLMode{Mode: "prefer"}, UploadingMethod: UploadingMethod{
		Method: "Standard", BucketName: "b", BucketRegion: "moon", BucketPath: "it's",
		AccessKeyID: "AKIA", RoleARN: "role", ExternalID: "x", IAMRoleARN: "arn:aws:iam::123456789012:role/a,nope",
	}}
	err := cfg.validate()
	require.Error(t, err)
	for _, want := range []string{
		"host is required", "database is required", "username is required", "port 70000 is out of range",
		`ssl_mode "prefer" is not one of`, `uploading_method "Standard" is not supported`,
		`s3_bucket_name "b" is not a valid S3 bucket name`, `s3_bucket_region "moon" is not an AWS region`,
		`s3_bucket_path "it's" contains a quote`, "access_key_id and secret_access_key go together",
		`role_arn "role" is not an IAM role ARN`, `iam_role_arn "arn:aws:iam::123456789012:role/a,nope" is not an IAM role ARN`,
	} {
		assert.Contains(t, err.Error(), want)
	}

	ok := Config{Host: "h", Port: 5439, Database: "d", Schema: "s", Username: "u", SSLMode: SSLMode{Mode: sslVerifyFull},
		UploadingMethod: UploadingMethod{Method: methodS3Staging, BucketName: "my.bucket-1", BucketRegion: "us-gov-west-1",
			RoleARN: "arn:aws:iam::123456789012:role/path/loader", ExternalID: "ext",
			IAMRoleARN: "arn:aws:iam::123456789012:role/a,arn:aws:iam::210987654321:role/b"}}
	assert.NoError(t, ok.validate())
}

func TestLiteral(t *testing.T) {
	assert.Equal(t, `'it''s a \\ path'`, literal(`it's a \ path`))
	assert.Equal(t, `"a""b"."c"`, quote(`a"b`, "c"))
}
