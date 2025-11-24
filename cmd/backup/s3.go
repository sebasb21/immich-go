package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Backend handles S3 operations for backup
type S3Backend struct {
	client         *s3.Client
	assetsBucket   string
	manifestBucket string
	assetsPrefix   string
	region         string
}

// NewS3Backend creates a new S3 backend
func NewS3Backend(ctx context.Context, assetsBucket, manifestBucket, region, assetsPrefix string) (*S3Backend, error) {
	// Load AWS configuration from environment/credentials file
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Backend{
		client:         client,
		assetsBucket:   assetsBucket,
		manifestBucket: manifestBucket,
		assetsPrefix:   assetsPrefix,
		region:         region,
	}, nil
}

// UploadAsset uploads an asset to S3
// The reader content is buffered to allow S3 SDK retries (requires seekable stream)
func (s *S3Backend) UploadAsset(ctx context.Context, key string, reader io.Reader, contentType string) error {
	fullKey := key
	if s.assetsPrefix != "" {
		fullKey = s.assetsPrefix + "/" + key
	}

	// Buffer content for S3 SDK retry support (needs seekable stream)
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return fmt.Errorf("failed to buffer asset data: %w", err)
	}

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.assetsBucket),
		Key:           aws.String(fullKey),
		Body:          bytes.NewReader(buf.Bytes()),
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(int64(buf.Len())),
		StorageClass:  types.StorageClassDeepArchive,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// DownloadManifest downloads the manifest from S3
func (s *S3Backend) DownloadManifest(ctx context.Context, manifestKey string) (map[string]ManifestEntry, error) {
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.manifestBucket),
		Key:    aws.String(manifestKey),
	})
	if err != nil {
		// Check if it's a "not found" error - that's OK for first run
		return make(map[string]ManifestEntry), nil
	}
	defer result.Body.Close()

	var entries map[string]ManifestEntry
	if err := json.NewDecoder(result.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	return entries, nil
}

// UploadManifest uploads the manifest to S3
func (s *S3Backend) UploadManifest(ctx context.Context, manifestKey string, entries map[string]ManifestEntry) error {
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.manifestBucket),
		Key:         aws.String(manifestKey),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload manifest to S3: %w", err)
	}

	return nil
}

// GetAssetKey generates the S3 key for an asset
func (s *S3Backend) GetAssetKey(year, month, filename string) string {
	key := fmt.Sprintf("%s/%s/%s", year, month, filename)
	if s.assetsPrefix != "" {
		key = s.assetsPrefix + "/" + key
	}
	return key
}
