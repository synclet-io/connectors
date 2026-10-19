package main

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Config is the connector configuration. Its keys follow Airbyte's
// destination-redshift so existing configurations carry over.
type Config struct {
	Host            string          `json:"host"`
	Port            int             `json:"port"`
	Database        string          `json:"database"`
	Schema          string          `json:"schema"`
	Username        string          `json:"username"`
	Password        string          `json:"password"`
	SSLMode         SSLMode         `json:"ssl_mode"`
	UploadingMethod UploadingMethod `json:"uploading_method"`
}

// SSLMode is how the connection to the cluster is encrypted.
type SSLMode struct {
	Mode string `json:"mode"`
}

// UploadingMethod is the S3 bucket batches are staged in and the
// credentials of the upload and of COPY.
type UploadingMethod struct {
	Method           string `json:"method"`
	BucketName       string `json:"s3_bucket_name"`
	BucketPath       string `json:"s3_bucket_path"`
	BucketRegion     string `json:"s3_bucket_region"`
	AccessKeyID      string `json:"access_key_id"`
	SecretAccessKey  string `json:"secret_access_key"`
	RoleARN          string `json:"role_arn"`
	ExternalID       string `json:"external_id"`
	IAMRoleARN       string `json:"iam_role_arn"`
	PurgeStagingData *bool  `json:"purge_staging_data"`
}

const (
	sslDisable    = "disable"
	sslRequire    = "require"
	sslVerifyFull = "verify-full"

	methodS3Staging = "S3 Staging"
	defaultRegion   = "us-east-1"
)

var sslModes = []string{sslDisable, sslRequire, sslVerifyFull}

var (
	roleARN    = regexp.MustCompile(`^arn:aws[a-z-]*:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$`)
	bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	regionName = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]+$`)
)

func (c *Config) applyDefaults() {
	if c.Port == 0 {
		c.Port = 5439
	}
	if c.Schema == "" {
		c.Schema = "public"
	}
	if c.SSLMode.Mode == "" {
		c.SSLMode.Mode = sslRequire
	}
	u := &c.UploadingMethod
	if u.Method == "" {
		u.Method = methodS3Staging
	}
	if u.BucketRegion == "" {
		u.BucketRegion = defaultRegion
	}
	u.BucketPath = strings.Trim(u.BucketPath, "/")
}

// purge reports whether staged files are deleted once loaded.
func (u UploadingMethod) purge() bool {
	return u.PurgeStagingData == nil || *u.PurgeStagingData
}

func (c *Config) validate() error {
	var errs []error
	if c.Host == "" {
		errs = append(errs, errors.New("host is required"))
	}
	if c.Database == "" {
		errs = append(errs, errors.New("database is required"))
	}
	if c.Username == "" {
		errs = append(errs, errors.New("username is required"))
	}
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("port %d is out of range 1-65535", c.Port))
	}
	if err := checkIdentName("schema", c.Schema); err != nil {
		errs = append(errs, err)
	}
	if !slices.Contains(sslModes, c.SSLMode.Mode) {
		errs = append(errs, fmt.Errorf("ssl_mode %q is not one of %v", c.SSLMode.Mode, sslModes))
	}
	u := c.UploadingMethod
	if u.Method != methodS3Staging {
		errs = append(errs, fmt.Errorf("uploading_method %q is not supported; use %q", u.Method, methodS3Staging))
	}
	if !bucketName.MatchString(u.BucketName) {
		errs = append(errs, fmt.Errorf("s3_bucket_name %q is not a valid S3 bucket name", u.BucketName))
	}
	if !regionName.MatchString(u.BucketRegion) {
		errs = append(errs, fmt.Errorf("s3_bucket_region %q is not an AWS region", u.BucketRegion))
	}
	if strings.ContainsAny(u.BucketPath, "'\\") || strings.ContainsFunc(u.BucketPath, isControl) {
		errs = append(errs, fmt.Errorf("s3_bucket_path %q contains a quote, a backslash or a control character", u.BucketPath))
	}
	if (u.AccessKeyID == "") != (u.SecretAccessKey == "") {
		errs = append(errs, errors.New("access_key_id and secret_access_key go together"))
	}
	if u.RoleARN != "" && !roleARN.MatchString(u.RoleARN) {
		errs = append(errs, fmt.Errorf("role_arn %q is not an IAM role ARN", u.RoleARN))
	}
	if u.ExternalID != "" && u.RoleARN == "" {
		errs = append(errs, errors.New("external_id needs role_arn"))
	}
	if u.IAMRoleARN != "" {
		for _, arn := range strings.Split(u.IAMRoleARN, ",") {
			if !roleARN.MatchString(arn) {
				errs = append(errs, fmt.Errorf("iam_role_arn %q is not an IAM role ARN or a comma-separated chain of them", u.IAMRoleARN))
				break
			}
		}
	}
	return errors.Join(errs...)
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }
