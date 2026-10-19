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

// Check writes and deletes a probe object under warehouse_location, connects
// to the catalog and checks that the namespace exists.
func (Destination) Check(ctx context.Context, cfg Config) error {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	awsCfg, err := awsConfig(ctx, cfg.S3)
	if err != nil {
		return err
	}
	if err := checkBucket(ctx, newS3Client(awsCfg, cfg.S3), cfg.S3); err != nil {
		return err
	}
	l, err := openLake(ctx, cfg)
	if err != nil {
		return err
	}
	defer l.close()
	return l.checkNamespace(ctx)
}

func checkBucket(ctx context.Context, client *s3.Client, cfg S3Config) error {
	bucket, prefix, err := cfg.bucketAndPrefix()
	if err != nil {
		return err
	}
	probe := "_synclet_check_" + uuid.NewString()
	if prefix != "" {
		probe = prefix + "/" + probe
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(probe), Body: strings.NewReader("synclet")})
	if err != nil {
		return describeS3Error(err, bucket, "no write permission", probe)
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(probe)}); err != nil {
		return describeS3Error(err, bucket, "no delete permission", probe)
	}
	return nil
}

// describeS3Error names the cause of a failed request: invalid credentials,
// a missing bucket, or denied, which is reported as denied.
func describeS3Error(err error, bucket, denied, key string) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken", "ExpiredToken", "InvalidClientTokenId", "TokenRefreshRequired":
			return fmt.Errorf("invalid S3 credentials: %s", ae.ErrorMessage())
		case "NoSuchBucket":
			return fmt.Errorf("bucket %q does not exist", bucket)
		case "AccessDenied", "AllAccessDisabled":
			return fmt.Errorf("%s on s3://%s/%s: %s", denied, bucket, key, ae.ErrorMessage())
		}
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusForbidden {
		return fmt.Errorf("%s on s3://%s/%s: %w", denied, bucket, key, err)
	}
	return fmt.Errorf("s3://%s/%s: %w", bucket, key, err)
}
