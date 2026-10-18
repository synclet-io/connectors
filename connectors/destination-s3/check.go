package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
)

// Check verifies the bucket exists and that the credentials may write,
// delete and list objects under bucket_path, by writing and deleting a
// probe object.
func (d *S3Destination) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	client, err := newS3Client(ctx, cfg)
	if err != nil {
		return err
	}
	return checkBucket(ctx, client, cfg)
}

func checkBucket(ctx context.Context, client *s3.Client, cfg Config) error {
	bucket := aws.String(cfg.Bucket)
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket}); err != nil {
		switch {
		case isNotFound(err):
			return fmt.Errorf("bucket %q does not exist", cfg.Bucket)
		case !isForbidden(err):
			return fmt.Errorf("reaching bucket %q: %w", cfg.Bucket, err)
		}
		// HEAD answers 403 without an error code, both for wrong
		// credentials and for a user without s3:ListBucket; the probe
		// below tells them apart.
	}

	probe := "_synclet_check_" + uuid.NewString()
	if p := cfg.keyPrefix(); p != "" {
		probe = p + "/" + probe
	}
	_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: aws.String(probe), Body: strings.NewReader("synclet")})
	if err != nil {
		return describeError(err, cfg.Bucket, "no write permission", probe)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: aws.String(probe)}); err != nil {
		return describeError(err, cfg.Bucket, "no delete permission", probe)
	}
	prefix := cfg.keyPrefix()
	if prefix != "" {
		prefix += "/"
	}
	_, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket, Prefix: aws.String(prefix), MaxKeys: aws.Int32(1)})
	if err != nil {
		return describeError(err, cfg.Bucket, "no list permission", prefix)
	}
	return nil
}

// describeError names the cause of a failed request: invalid credentials, a
// missing bucket, or denied, which is reported as denied.
func describeError(err error, bucket, denied, key string) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken", "ExpiredToken", "InvalidClientTokenId", "TokenRefreshRequired":
			return fmt.Errorf("invalid credentials: %s", ae.ErrorMessage())
		case "NoSuchBucket":
			return fmt.Errorf("bucket %q does not exist", bucket)
		case "AccessDenied", "AllAccessDisabled":
			return fmt.Errorf("%s on s3://%s/%s: %s", denied, bucket, key, ae.ErrorMessage())
		}
	}
	if isForbidden(err) {
		return fmt.Errorf("%s on s3://%s/%s: %w", denied, bucket, key, err)
	}
	return fmt.Errorf("s3://%s/%s: %w", bucket, key, err)
}

func isNotFound(err error) bool {
	return httpStatus(err) == http.StatusNotFound
}

func isForbidden(err error) bool {
	return httpStatus(err) == http.StatusForbidden
}

func httpStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}
