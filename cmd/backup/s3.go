package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Backend handles S3 operations for backup
type S3Backend struct {
	client            *s3.Client
	assetsBucket      string
	manifestBucket    string
	assetsPrefix      string
	region            string
	maxUploadBandwidth int64 // bytes per second, 0 = unlimited
}

// NewS3Backend creates a new S3 backend
func NewS3Backend(ctx context.Context, assetsBucket, manifestBucket, region, assetsPrefix string, maxUploadBandwidth int64) (*S3Backend, error) {
	// Load AWS configuration from environment/credentials file
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Backend{
		client:            client,
		assetsBucket:      assetsBucket,
		manifestBucket:    manifestBucket,
		assetsPrefix:      assetsPrefix,
		region:            region,
		maxUploadBandwidth: maxUploadBandwidth,
	}, nil
}

// UploadAsset uploads an asset to S3 with exponential backoff retry and optional bandwidth throttling
// The reader content is buffered to allow retries (requires seekable stream)
// Retries indefinitely with exponential backoff up to 5 minutes
// Logs progress every 3 seconds during upload
func (s *S3Backend) UploadAsset(ctx context.Context, key string, reader io.Reader, contentType string) error {
	fullKey := key
	if s.assetsPrefix != "" {
		fullKey = s.assetsPrefix + "/" + key
	}

	// Buffer content for retry support (needs seekable stream)
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return fmt.Errorf("failed to buffer asset data: %w", err)
	}

	fileSize := int64(buf.Len())

	// Retry configuration
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 5 * time.Minute
		backoffFactor  = 2.0
	)

	backoff := initialBackoff
	attempt := 0

	for {
		attempt++

		// Create a reader for this attempt
		bodyReader := bytes.NewReader(buf.Bytes())

		// Wrap in progress tracker
		progressReader := NewProgressReader(bodyReader, fileSize, fullKey)

		// Apply bandwidth throttling on top of progress tracking if configured
		var uploadReader io.Reader = progressReader
		if s.maxUploadBandwidth > 0 {
			uploadReader = NewRateLimitedReader(ctx, progressReader, s.maxUploadBandwidth)
		}

		// Start progress logging goroutine
		progressCtx, cancelProgress := context.WithCancel(ctx)
		progressDone := make(chan struct{})
		go func() {
			defer close(progressDone)
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-progressCtx.Done():
					return
				case <-ticker.C:
					fmt.Println(progressReader.FormatProgress())
					progressReader.ResetLogCheckpoint()
				}
			}
		}()

		// Attempt upload
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.assetsBucket),
			Key:           aws.String(fullKey),
			Body:          uploadReader,
			ContentType:   aws.String(contentType),
			ContentLength: aws.Int64(fileSize),
			StorageClass:  types.StorageClassDeepArchive,
		})

		// Stop progress logging
		cancelProgress()
		<-progressDone

		if err == nil {
			// Success! Log final progress
			fmt.Printf("✓ Completed upload: %s (%s) - Avg speed: %s/s\n",
				fullKey,
				formatBytesS3(fileSize),
				formatBytesS3(int64(progressReader.AverageSpeed())),
			)
			if attempt > 1 {
				fmt.Printf("  Successfully uploaded after %d attempts\n", attempt)
			}
			return nil
		}

		// Check if context was cancelled
		if ctx.Err() != nil {
			return fmt.Errorf("upload cancelled: %w", ctx.Err())
		}

		// Log the error and prepare to retry
		fmt.Printf("✗ Upload attempt %d failed for %s: %v. Retrying in %v...\n", attempt, fullKey, err, backoff)

		// Wait with exponential backoff
		select {
		case <-ctx.Done():
			return fmt.Errorf("upload cancelled during backoff: %w", ctx.Err())
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

// formatBytesS3 formats bytes for display (local helper to avoid import issues)
func formatBytesS3(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// DownloadManifest downloads the manifest from S3
func (s *S3Backend) DownloadManifest(ctx context.Context, manifestKey string) (map[string]ManifestEntry, error) {
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.manifestBucket),
		Key:    aws.String(manifestKey),
	})
	if err != nil {
		// Return error for any issue (including not found)
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("manifest not found in S3 bucket '%s' (key: %s). Cannot continue without existing manifest", s.manifestBucket, manifestKey)
		}
		// For any other error (permissions, network, etc.), return it
		return nil, fmt.Errorf("failed to get manifest from S3 bucket '%s': %w", s.manifestBucket, err)
	}
	defer result.Body.Close()

	var entries map[string]ManifestEntry
	if err := json.NewDecoder(result.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	return entries, nil
}

// UploadManifest uploads the manifest to S3 and saves local backup
func (s *S3Backend) UploadManifest(ctx context.Context, manifestKey string, entries map[string]ManifestEntry) error {
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Save local backup before uploading to S3
	if err := s.saveLocalManifestBackup(manifestKey, data); err != nil {
		// Log warning but don't fail - local backup is not critical
		fmt.Printf("Warning: failed to save local manifest backup: %v\n", err)
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

// saveLocalManifestBackup saves the manifest locally and keeps only the last 2 versions
func (s *S3Backend) saveLocalManifestBackup(manifestKey string, data []byte) error {
	// Get local backup directory
	backupDir, err := getManifestBackupDir()
	if err != nil {
		return err
	}

	// Create backup directory if it doesn't exist
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Generate timestamped filename
	timestamp := time.Now().Format("20060102-150405")
	backupFilename := fmt.Sprintf("%s.%s.backup", manifestKey, timestamp)
	backupPath := filepath.Join(backupDir, backupFilename)

	// Write the backup file
	if err := os.WriteFile(backupPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write backup file: %w", err)
	}

	// Clean up old backups, keeping only the last 2
	if err := cleanupOldManifestBackups(backupDir, manifestKey, 2); err != nil {
		// Log warning but don't fail
		fmt.Printf("Warning: failed to cleanup old backups: %v\n", err)
	}

	fmt.Printf("Saved local manifest backup: %s\n", backupPath)
	return nil
}

// getManifestBackupDir returns the directory for manifest backups
func getManifestBackupDir() (string, error) {
	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		// Fallback to relative path if we can't get cwd
		return "./.immich-go-manifest-backups", nil
	}
	return filepath.Join(cwd, ".immich-go-manifest-backups"), nil
}

// cleanupOldManifestBackups keeps only the last N backup files for a given manifest
func cleanupOldManifestBackups(backupDir, manifestKey string, keepCount int) error {
	// Read all files in backup directory
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return err
	}

	// Filter files that match this manifest key
	var backupFiles []os.DirEntry
	prefix := manifestKey + "."
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".backup") {
			backupFiles = append(backupFiles, entry)
		}
	}

	// If we have fewer backups than keepCount, nothing to delete
	if len(backupFiles) <= keepCount {
		return nil
	}

	// Sort by name (which includes timestamp, so newest first when sorted descending)
	sort.Slice(backupFiles, func(i, j int) bool {
		return backupFiles[i].Name() > backupFiles[j].Name()
	})

	// Delete old backups (keeping only the newest keepCount)
	for i := keepCount; i < len(backupFiles); i++ {
		oldFile := filepath.Join(backupDir, backupFiles[i].Name())
		if err := os.Remove(oldFile); err != nil {
			fmt.Printf("Warning: failed to delete old backup %s: %v\n", oldFile, err)
		} else {
			fmt.Printf("Deleted old manifest backup: %s\n", backupFiles[i].Name())
		}
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
