package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// newS3Client builds an S3 client for the configured region, endpoint and
// credentials. A custom endpoint sends request checksums only where the
// operation requires them, which S3-compatible stores accept.
func newS3Client(ctx context.Context, cfg Config) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.Credentials.AuthType == authAccessKey {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.Credentials.AccessKeyID, cfg.Credentials.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS configuration: %w", err)
	}
	if cfg.Credentials.AuthType == authAssumeRole {
		provider := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(awsCfg), cfg.Credentials.RoleARN,
			func(o *stscreds.AssumeRoleOptions) {
				o.RoleSessionName = "synclet-destination-s3"
				if cfg.Credentials.ExternalID != "" {
					o.ExternalID = aws.String(cfg.Credentials.ExternalID)
				}
			})
		awsCfg.Credentials = aws.NewCredentialsCache(provider)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	}), nil
}
