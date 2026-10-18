package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const deleteBatchSize = 1000

// objectStore is the bucket the writer uploads to.
type objectStore interface {
	put(ctx context.Context, key string, body io.Reader) error
	list(ctx context.Context, prefix string) ([]string, error)
	delete(ctx context.Context, keys []string) error
}

// s3Store is an objectStore on one S3 bucket; uploads are multipart for
// large files.
type s3Store struct {
	client   *s3.Client
	uploader *transfermanager.Client
	bucket   string
}

func newS3Store(client *s3.Client, bucket string) *s3Store {
	return &s3Store{client: client, uploader: transfermanager.New(client), bucket: bucket}
}

func (s *s3Store) put(ctx context.Context, key string, body io.Reader) error {
	_, err := s.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("uploading s3://%s/%s: %w", s.bucket, key, err)
	}
	return nil
}

func (s *s3Store) list(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing s3://%s/%s: %w", s.bucket, prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}

func (s *s3Store) delete(ctx context.Context, keys []string) error {
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
			return fmt.Errorf("deleting %d objects from s3://%s: %w", len(batch), s.bucket, err)
		}
		if len(out.Errors) > 0 {
			msgs := make([]string, 0, len(out.Errors))
			for _, e := range out.Errors {
				msgs = append(msgs, fmt.Sprintf("%s: %s %s", aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message)))
			}
			return errors.New("deleting objects from s3://" + s.bucket + ": " + strings.Join(msgs, "; "))
		}
	}
	return nil
}
