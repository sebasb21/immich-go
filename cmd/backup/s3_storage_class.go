package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// GetStorageClass checks the current storage class of an S3 object
func (s *S3Backend) GetStorageClass(ctx context.Context, key string) (types.StorageClass, error) {
	fullKey := key
	if s.assetsPrefix != "" {
		fullKey = s.assetsPrefix + "/" + key
	}

	result, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.assetsBucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return "", err
	}
	return result.StorageClass, nil
}

// MigrateStorageClass changes the storage class of an S3 object via CopyObject
// Uses exponential backoff retry similar to UploadAsset
func (s *S3Backend) MigrateStorageClass(ctx context.Context, key string, targetClass types.StorageClass) error {
	fullKey := key
	if s.assetsPrefix != "" {
		fullKey = s.assetsPrefix + "/" + key
	}

	// Retry configuration (matching UploadAsset pattern)
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 5 * time.Minute
		backoffFactor  = 2.0
	)

	backoff := initialBackoff
	attempt := 0

	for {
		attempt++

		// Copy object to itself with new storage class
		_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(s.assetsBucket),
			Key:               aws.String(fullKey),
			CopySource:        aws.String(s.assetsBucket + "/" + fullKey),
			StorageClass:      targetClass,
			MetadataDirective: types.MetadataDirectiveCopy, // Preserve all metadata
		})

		if err == nil {
			if attempt > 1 {
				fmt.Printf("  Successfully migrated %s after %d attempts\n", fullKey, attempt)
			}
			return nil
		}

		// Check if context was cancelled
		if ctx.Err() != nil {
			return fmt.Errorf("migration cancelled: %w", ctx.Err())
		}

		// Log the error and prepare to retry
		fmt.Printf("✗ Migration attempt %d failed for %s: %v. Retrying in %v...\n",
			attempt, fullKey, err, backoff)

		// Wait with exponential backoff
		select {
		case <-ctx.Done():
			return fmt.Errorf("migration cancelled during backoff: %w", ctx.Err())
		case <-time.After(backoff):
			// Continue to next attempt
		}

		// Increase backoff exponentially up to max
		backoff = time.Duration(float64(backoff) * backoffFactor)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// VerifyStorageClass confirms that an S3 object has the expected storage class
func (s *S3Backend) VerifyStorageClass(ctx context.Context, key string, expected types.StorageClass) error {
	actual, err := s.GetStorageClass(ctx, key)
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	// Handle empty storage class (might be returned for some objects)
	if actual == "" {
		actual = types.StorageClassStandard
	}

	if actual != expected {
		return fmt.Errorf("storage class mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

// GetStorageClassName converts a storage class type to a string for logging
func GetStorageClassName(sc types.StorageClass) string {
	if sc == "" {
		return "STANDARD"
	}
	return string(sc)
}

// IsNoSuchKeyError checks if an error is a NoSuchKey or NotFound error
func IsNoSuchKeyError(err error) bool {
	if err == nil {
		return false
	}

	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}

	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}

	// Also check error message for 404/NotFound
	errMsg := err.Error()
	return strings.Contains(errMsg, "NotFound") ||
	       strings.Contains(errMsg, "StatusCode: 404") ||
	       strings.Contains(errMsg, "NoSuchKey")
}
