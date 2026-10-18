package main

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// s3Store is the objectStore of one S3 bucket.
type s3Store struct {
	client *s3.Client
	bucket string
	creds  aws.CredentialsProvider
}

// newS3Store builds a client for cfg's bucket. Credentials are resolved
// lazily, on the first request or on retrieveCredentials.
func newS3Store(ctx context.Context, cfg Config) (*s3Store, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	cr := cfg.Credentials
	if cr.AccessKeyID != "" && (cr.AuthType == authAccessKey || cr.AuthType == authAssumeRole) {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cr.AccessKeyID, cr.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if cr.AuthType == authAssumeRole {
		stsClient := sts.NewFromConfig(awsCfg, func(o *sts.Options) {
			if cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.Endpoint)
			}
		})
		awsCfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, cr.RoleARN,
			func(o *stscreds.AssumeRoleOptions) {
				o.RoleSessionName = cr.RoleSessionName
				if cr.ExternalID != "" {
					o.ExternalID = aws.String(cr.ExternalID)
				}
			}))
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	return &s3Store{client: client, bucket: cfg.Bucket, creds: awsCfg.Credentials}, nil
}

func (s *s3Store) retrieveCredentials(ctx context.Context) error {
	if s.creds == nil {
		return fmt.Errorf("no credentials found in the default chain")
	}
	_, err := s.creds.Retrieve(ctx)
	return err
}

// List returns every object under prefix, page by page.
func (s *s3Store) List(ctx context.Context, prefix string) ([]object, error) {
	var out []object
	err := s.walk(ctx, prefix, func(o object) bool {
		out = append(out, o)
		return true
	})
	return out, err
}

// walk calls fn for each object under prefix in key order until fn returns
// false.
func (s *s3Store) walk(ctx context.Context, prefix string, fn func(object) bool) error {
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, o := range page.Contents {
			obj := object{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), LastModified: aws.ToTime(o.LastModified).UTC()}
			if !fn(obj) {
				return nil
			}
		}
	}
	return nil
}

// Open streams the object's content.
func (s *s3Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// readFirstByte fetches the first byte of an object, which proves read
// permission without downloading it. An empty object has no byte range.
func (s *s3Store) readFirstByte(ctx context.Context, o object) error {
	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(o.Key)}
	if o.Size > 0 {
		in.Range = aws.String("bytes=0-0")
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, out.Body)
	return errorsJoinClose(err, out.Body)
}

func errorsJoinClose(err error, c io.Closer) error {
	if cerr := c.Close(); err == nil {
		return cerr
	}
	return err
}
