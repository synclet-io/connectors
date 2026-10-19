package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

const deleteBatchSize = 1000

// staging is the S3 bucket batches are uploaded to before COPY, with the
// credentials of the upload and the authorization COPY reads them with.
type staging struct {
	client  *s3.Client
	creds   aws.CredentialsProvider
	bucket  string
	region  string
	prefix  string
	iamRole string
	assumed string
}

// newStaging builds the S3 client: static keys when given, the default
// credential chain otherwise, and role_arn assumed on top of either.
// endpoint and stsEndpoint replace the AWS endpoints in tests.
func newStaging(ctx context.Context, cfg Config, endpoint, stsEndpoint string) (*staging, error) {
	u := cfg.UploadingMethod
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(u.BucketRegion)}
	if u.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(u.AccessKeyID, u.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if u.RoleARN != "" {
		stsClient := sts.NewFromConfig(awsCfg, func(o *sts.Options) {
			if stsEndpoint != "" {
				o.BaseEndpoint = aws.String(stsEndpoint)
			}
		})
		provider := stscreds.NewAssumeRoleProvider(stsClient, u.RoleARN, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = "synclet-destination-redshift"
			if u.ExternalID != "" {
				o.ExternalID = aws.String(u.ExternalID)
			}
		})
		awsCfg.Credentials = aws.NewCredentialsCache(provider)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})
	return &staging{
		client:  client,
		creds:   awsCfg.Credentials,
		bucket:  u.BucketName,
		region:  u.BucketRegion,
		prefix:  u.BucketPath,
		iamRole: u.IAMRoleARN,
		assumed: u.RoleARN,
	}, nil
}

// key joins the bucket path and name.
func (s *staging) key(name string) string {
	if s.prefix == "" {
		return name
	}
	return s.prefix + "/" + name
}

func (s *staging) url(key string) string {
	return "s3://" + s.bucket + "/" + key
}

// retrieve fetches the upload credentials, which assumes role_arn when it
// is set; it fails with a named cause when the role cannot be assumed.
func (s *staging) retrieve(ctx context.Context) (aws.Credentials, error) {
	c, err := s.creds.Retrieve(ctx)
	if err != nil {
		if s.assumed != "" {
			return aws.Credentials{}, fmt.Errorf("cannot assume IAM role %s: %w", s.assumed, err)
		}
		return aws.Credentials{}, fmt.Errorf("no AWS credentials for the S3 staging bucket: %w", err)
	}
	return c, nil
}

// copyAuth is the authorization clause of COPY: the cluster's IAM role
// when iam_role_arn is set, otherwise the upload credentials, which are
// temporary ones with a session token when role_arn is assumed.
func (s *staging) copyAuth(ctx context.Context) (string, error) {
	if s.iamRole != "" {
		return "IAM_ROLE " + literal(s.iamRole), nil
	}
	c, err := s.retrieve(ctx)
	if err != nil {
		return "", err
	}
	auth := "ACCESS_KEY_ID " + literal(c.AccessKeyID) + " SECRET_ACCESS_KEY " + literal(c.SecretAccessKey)
	if c.SessionToken != "" {
		auth += " SESSION_TOKEN " + literal(c.SessionToken)
	}
	return auth, nil
}

func (s *staging) put(ctx context.Context, key string, body []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return describeS3Error(err, s.bucket, key, "no write permission")
	}
	return nil
}

func (s *staging) delete(ctx context.Context, keys []string) error {
	for len(keys) > 0 {
		n := min(len(keys), deleteBatchSize)
		batch := keys[:n]
		keys = keys[n:]
		ids := make([]types.ObjectIdentifier, len(batch))
		for i, k := range batch {
			ids[i] = types.ObjectIdentifier{Key: aws.String(k)}
		}
		out, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return describeS3Error(err, s.bucket, batch[0], "no delete permission")
		}
		if len(out.Errors) > 0 {
			msgs := make([]string, 0, len(out.Errors))
			for _, e := range out.Errors {
				msgs = append(msgs, fmt.Sprintf("%s: %s %s", aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message)))
			}
			return errors.New("delete staged files from s3://" + s.bucket + ": " + strings.Join(msgs, "; "))
		}
	}
	return nil
}

// describeS3Error names the cause of a failed request: invalid
// credentials, a missing bucket, or denied, which is reported as denied.
func describeS3Error(err error, bucket, key, denied string) error {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken", "ExpiredToken", "InvalidClientTokenId", "TokenRefreshRequired":
			return fmt.Errorf("invalid S3 credentials: %s", ae.ErrorMessage())
		case "NoSuchBucket":
			return fmt.Errorf("S3 staging bucket %q does not exist", bucket)
		case "AccessDenied", "AllAccessDisabled":
			return fmt.Errorf("S3 staging bucket is not writable: %s on s3://%s/%s: %s", denied, bucket, key, ae.ErrorMessage())
		}
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusForbidden {
		return fmt.Errorf("S3 staging bucket is not writable: %s on s3://%s/%s: %w", denied, bucket, key, err)
	}
	return fmt.Errorf("s3://%s/%s: %w", bucket, key, err)
}
