package main

import (
	"context"
	"fmt"
	"strconv"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// awsConfig loads the AWS configuration of the configured region and
// credentials.
func awsConfig(ctx context.Context, cfg S3Config) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.Credentials.AuthType == authAccessKey {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.Credentials.AccessKeyID, cfg.Credentials.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("loading AWS configuration: %w", err)
	}
	if cfg.Credentials.AuthType == authAssumeRole {
		provider := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(awsCfg), cfg.Credentials.RoleARN,
			func(o *stscreds.AssumeRoleOptions) {
				o.RoleSessionName = "synclet-destination-s3-data-lake"
				if cfg.Credentials.ExternalID != "" {
					o.ExternalID = aws.String(cfg.Credentials.ExternalID)
				}
			})
		awsCfg.Credentials = aws.NewCredentialsCache(provider)
	}
	return awsCfg, nil
}

// newS3Client builds an S3 client for the configured endpoint. A custom
// endpoint sends request checksums only where the operation requires them,
// which S3-compatible stores accept.
func newS3Client(awsCfg aws.Config, cfg S3Config) *s3.Client {
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})
}

// ioProperties are the iceberg-go file IO properties of the configured
// bucket. The endpoint key is always set, so neither the catalog's
// properties nor AWS_S3_ENDPOINT can redirect the files.
func ioProperties(cfg S3Config) map[string]string {
	return map[string]string{
		icebergio.S3Region:                 cfg.Region,
		icebergio.S3EndpointURL:            cfg.Endpoint,
		icebergio.S3ForceVirtualAddressing: strconv.FormatBool(!cfg.ForcePathStyle),
	}
}
