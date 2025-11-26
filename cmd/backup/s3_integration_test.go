//go:build integration

package backup

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/simulot/immich-go/immich"
)

// Test configuration from environment variables
// Required: S3_ASSETS_BUCKET, S3_MANIFEST_BUCKET, IMMICH_SERVER, IMMICH_API_KEY
// Optional: S3_REGION (defaults to us-east-1)
//
// Example:
//   S3_ASSETS_BUCKET=my-assets S3_MANIFEST_BUCKET=my-manifest S3_REGION=us-east-1 \
//   IMMICH_SERVER=http://localhost:2283 IMMICH_API_KEY=your-key \
//   go test -tags=integration -v ./cmd/backup -run TestS3
func getTestConfig(t *testing.T) (assetsBucket, manifestBucket, region, immichServer, immichKey string) {
	t.Helper()

	assetsBucket = os.Getenv("S3_ASSETS_BUCKET")
	manifestBucket = os.Getenv("S3_MANIFEST_BUCKET")
	region = os.Getenv("S3_REGION")
	immichServer = os.Getenv("IMMICH_SERVER")
	immichKey = os.Getenv("IMMICH_API_KEY")

	if region == "" {
		region = "us-east-1" // Default region
	}

	return
}

// skipIfNoAWSCredentials skips the test if AWS credentials are not configured
func skipIfNoAWSCredentials(t *testing.T) {
	t.Helper()

	// Check for environment variables
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return
	}

	// Check for credentials file
	home, _ := os.UserHomeDir()
	if _, err := os.Stat(home + "/.aws/credentials"); err == nil {
		return
	}

	t.Skip("Skipping S3 integration test: AWS credentials not configured. " +
		"Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables, " +
		"or configure ~/.aws/credentials")
}

func TestS3Backend_Integration(t *testing.T) {
	skipIfNoAWSCredentials(t)
	assetsBucket, manifestBucket, region, _, _ := getTestConfig(t)
	if assetsBucket == "" || manifestBucket == "" {
		t.Skip("Skipping: S3_ASSETS_BUCKET and S3_MANIFEST_BUCKET environment variables required")
	}
	ctx := context.Background()

	// Test S3 backend creation
	t.Run("CreateS3Backend", func(t *testing.T) {
		backend, err := NewS3Backend(ctx, assetsBucket, manifestBucket, region, "test-prefix", 0)
		if err != nil {
			t.Fatalf("Failed to create S3 backend: %v", err)
		}
		if backend == nil {
			t.Fatal("S3 backend is nil")
		}
		t.Log("S3 backend created successfully")
	})

	// Test manifest operations
	t.Run("ManifestOperations", func(t *testing.T) {
		backend, err := NewS3Backend(ctx, assetsBucket, manifestBucket, region, "", 0)
		if err != nil {
			t.Fatalf("Failed to create S3 backend: %v", err)
		}

		manifestKey := "test-manifest-" + time.Now().Format("20060102-150405") + ".json"

		// Create test entries
		testEntries := map[string]ManifestEntry{
			"test-asset-1": {
				AssetID:    "test-asset-1",
				Checksum:   "abc123",
				BackupPath: "2025/11/test1.jpg",
				BackedUpAt: time.Now(),
				FileSize:   1024,
			},
			"test-asset-2": {
				AssetID:    "test-asset-2",
				Checksum:   "def456",
				BackupPath: "2025/11/test2.jpg",
				BackedUpAt: time.Now(),
				FileSize:   2048,
			},
		}

		// Upload manifest
		err = backend.UploadManifest(ctx, manifestKey, testEntries)
		if err != nil {
			t.Fatalf("Failed to upload manifest: %v", err)
		}
		t.Log("Manifest uploaded successfully")

		// Download manifest
		downloaded, err := backend.DownloadManifest(ctx, manifestKey)
		if err != nil {
			t.Fatalf("Failed to download manifest: %v", err)
		}

		if len(downloaded) != len(testEntries) {
			t.Errorf("Expected %d entries, got %d", len(testEntries), len(downloaded))
		}

		for id, entry := range testEntries {
			if d, ok := downloaded[id]; !ok {
				t.Errorf("Missing entry: %s", id)
			} else if d.Checksum != entry.Checksum {
				t.Errorf("Checksum mismatch for %s: expected %s, got %s", id, entry.Checksum, d.Checksum)
			}
		}
		t.Log("Manifest download and verification successful")

		// Cleanup test manifest
		cfg, _ := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		client := s3.NewFromConfig(cfg)
		_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(manifestBucket),
			Key:    aws.String(manifestKey),
		})
		t.Log("Test manifest cleaned up")
	})
}

func TestS3Backup_Integration(t *testing.T) {
	skipIfNoAWSCredentials(t)
	assetsBucket, manifestBucket, region, immichServer, immichKey := getTestConfig(t)
	if assetsBucket == "" || manifestBucket == "" {
		t.Skip("Skipping: S3_ASSETS_BUCKET and S3_MANIFEST_BUCKET environment variables required")
	}
	if immichServer == "" || immichKey == "" {
		t.Skip("Skipping: IMMICH_SERVER and IMMICH_API_KEY environment variables required")
	}
	ctx := context.Background()

	// Create Immich client
	immichClient, err := immich.NewImmichClient(immichServer, immichKey)
	if err != nil {
		t.Fatalf("Failed to create Immich client: %v", err)
	}

	// Validate connection
	user, err := immichClient.ValidateConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Immich: %v", err)
	}
	t.Logf("Connected to Immich as: %s", user.Email)

	// Create S3 backend
	testPrefix := "integration-test-" + time.Now().Format("20060102-150405")
	backend, err := NewS3Backend(ctx, assetsBucket, manifestBucket, region, testPrefix, 0)
	if err != nil {
		t.Fatalf("Failed to create S3 backend: %v", err)
	}

	// Find a small image to backup
	var testAsset *immich.Asset
	err = immichClient.GetAllAssetsWithFilter(ctx, func(asset *immich.Asset) error {
		if asset.Type == "IMAGE" && asset.ExifInfo.FileSizeInByte > 0 && asset.ExifInfo.FileSizeInByte < 5*1024*1024 {
			testAsset = asset
			return context.Canceled // Stop iteration
		}
		return nil
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("Failed to find test asset: %v", err)
	}

	if testAsset == nil {
		t.Skip("No suitable small image found for testing")
	}

	t.Logf("Test asset: ID=%s, Name=%s, Size=%d bytes",
		testAsset.ID, testAsset.OriginalFileName, testAsset.ExifInfo.FileSizeInByte)

	// Download from Immich
	reader, err := immichClient.DownloadAsset(ctx, testAsset.ID)
	if err != nil {
		t.Fatalf("Failed to download from Immich: %v", err)
	}

	// Upload to S3
	s3Key := "2025/11/" + testAsset.OriginalFileName
	err = backend.UploadAsset(ctx, s3Key, reader, "image/jpeg")
	reader.Close()
	if err != nil {
		t.Fatalf("Failed to upload to S3: %v", err)
	}
	t.Logf("Asset uploaded to S3: s3://%s/%s/%s", assetsBucket, testPrefix, s3Key)

	// Verify asset exists in S3
	cfg, _ := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	client := s3.NewFromConfig(cfg)

	fullKey := testPrefix + "/" + s3Key
	headResult, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(assetsBucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		t.Fatalf("Failed to verify S3 object: %v", err)
	}
	t.Logf("Verified S3 object exists, size: %d bytes, storage class: %s", *headResult.ContentLength, headResult.StorageClass)

	// Verify storage class is Glacier Deep Archive
	if headResult.StorageClass != "DEEP_ARCHIVE" {
		t.Errorf("Expected storage class DEEP_ARCHIVE, got %s", headResult.StorageClass)
	}

	// Create and upload manifest
	manifestKey := testPrefix + "-manifest.json"
	manifestEntries := map[string]ManifestEntry{
		testAsset.ID: {
			AssetID:    testAsset.ID,
			Checksum:   testAsset.Checksum,
			BackupPath: fullKey,
			BackedUpAt: time.Now(),
			FileSize:   testAsset.ExifInfo.FileSizeInByte,
		},
	}

	err = backend.UploadManifest(ctx, manifestKey, manifestEntries)
	if err != nil {
		t.Fatalf("Failed to upload manifest: %v", err)
	}
	t.Logf("Manifest uploaded to S3: s3://%s/%s", manifestBucket, manifestKey)

	// Verify manifest
	downloaded, err := backend.DownloadManifest(ctx, manifestKey)
	if err != nil {
		t.Fatalf("Failed to download manifest: %v", err)
	}

	if len(downloaded) != 1 {
		t.Errorf("Expected 1 manifest entry, got %d", len(downloaded))
	}

	if entry, ok := downloaded[testAsset.ID]; !ok {
		t.Error("Asset not found in manifest")
	} else if entry.Checksum != testAsset.Checksum {
		t.Errorf("Checksum mismatch: expected %s, got %s", testAsset.Checksum, entry.Checksum)
	}

	t.Log("S3 backup integration test PASSED")

	// Cleanup (optional - comment out to inspect results)
	t.Log("Cleaning up test data...")
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(assetsBucket),
		Key:    aws.String(fullKey),
	})
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(manifestBucket),
		Key:    aws.String(manifestKey),
	})
	t.Log("Cleanup complete")
}

func TestS3IncrementalBackup_Integration(t *testing.T) {
	skipIfNoAWSCredentials(t)
	assetsBucket, manifestBucket, region, immichServer, immichKey := getTestConfig(t)
	if assetsBucket == "" || manifestBucket == "" {
		t.Skip("Skipping: S3_ASSETS_BUCKET and S3_MANIFEST_BUCKET environment variables required")
	}
	if immichServer == "" || immichKey == "" {
		t.Skip("Skipping: IMMICH_SERVER and IMMICH_API_KEY environment variables required")
	}
	ctx := context.Background()

	// Create Immich client
	immichClient, err := immich.NewImmichClient(immichServer, immichKey)
	if err != nil {
		t.Fatalf("Failed to create Immich client: %v", err)
	}

	_, err = immichClient.ValidateConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Immich: %v", err)
	}

	// Create S3 backend with unique prefix
	testPrefix := "incremental-test-" + time.Now().Format("20060102-150405")
	backend, err := NewS3Backend(ctx, assetsBucket, manifestBucket, region, testPrefix, 0)
	if err != nil {
		t.Fatalf("Failed to create S3 backend: %v", err)
	}

	manifestKey := testPrefix + "-manifest.json"

	// Find 2 small images
	var assets []*immich.Asset
	err = immichClient.GetAllAssetsWithFilter(ctx, func(asset *immich.Asset) error {
		if asset.Type == "IMAGE" && asset.ExifInfo.FileSizeInByte > 0 && asset.ExifInfo.FileSizeInByte < 2*1024*1024 {
			assets = append(assets, asset)
			if len(assets) >= 2 {
				return context.Canceled
			}
		}
		return nil
	})
	if len(assets) < 2 {
		t.Skip("Not enough small images for incremental test")
	}

	t.Logf("Found %d test assets", len(assets))

	// First backup: upload first asset
	t.Log("=== First backup run ===")
	reader1, _ := immichClient.DownloadAsset(ctx, assets[0].ID)
	s3Key1 := "2025/11/" + assets[0].OriginalFileName
	err = backend.UploadAsset(ctx, s3Key1, reader1, "image/jpeg")
	reader1.Close()
	if err != nil {
		t.Fatalf("Failed to upload first asset: %v", err)
	}

	// Save manifest with first asset
	manifest := map[string]ManifestEntry{
		assets[0].ID: {
			AssetID:    assets[0].ID,
			Checksum:   assets[0].Checksum,
			BackupPath: testPrefix + "/" + s3Key1,
			BackedUpAt: time.Now(),
			FileSize:   assets[0].ExifInfo.FileSizeInByte,
		},
	}
	err = backend.UploadManifest(ctx, manifestKey, manifest)
	if err != nil {
		t.Fatalf("Failed to save manifest: %v", err)
	}
	t.Logf("First backup complete: %s", assets[0].OriginalFileName)

	// Second backup: simulate incremental
	t.Log("=== Second backup run (incremental) ===")

	// Load manifest
	loadedManifest, err := backend.DownloadManifest(ctx, manifestKey)
	if err != nil {
		t.Fatalf("Failed to load manifest: %v", err)
	}
	t.Logf("Loaded manifest with %d entries", len(loadedManifest))

	// Check if assets need backup
	skipped := 0
	backed := 0

	for _, asset := range assets {
		entry, exists := loadedManifest[asset.ID]
		if exists && entry.Checksum == asset.Checksum {
			t.Logf("Skipping (already backed up): %s", asset.OriginalFileName)
			skipped++
			continue
		}

		// Need to backup
		reader, _ := immichClient.DownloadAsset(ctx, asset.ID)
		s3Key := "2025/11/" + asset.OriginalFileName
		err = backend.UploadAsset(ctx, s3Key, reader, "image/jpeg")
		reader.Close()
		if err != nil {
			t.Errorf("Failed to upload %s: %v", asset.OriginalFileName, err)
			continue
		}

		loadedManifest[asset.ID] = ManifestEntry{
			AssetID:    asset.ID,
			Checksum:   asset.Checksum,
			BackupPath: testPrefix + "/" + s3Key,
			BackedUpAt: time.Now(),
			FileSize:   asset.ExifInfo.FileSizeInByte,
		}
		t.Logf("Backed up: %s", asset.OriginalFileName)
		backed++
	}

	// Save updated manifest
	err = backend.UploadManifest(ctx, manifestKey, loadedManifest)
	if err != nil {
		t.Fatalf("Failed to save updated manifest: %v", err)
	}

	t.Logf("Incremental backup complete: %d backed up, %d skipped", backed, skipped)

	if skipped != 1 {
		t.Errorf("Expected 1 skipped, got %d", skipped)
	}
	if backed != 1 {
		t.Errorf("Expected 1 backed up, got %d", backed)
	}

	t.Log("Incremental backup test PASSED")

	// Cleanup
	t.Log("Cleaning up...")
	cfg, _ := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	client := s3.NewFromConfig(cfg)

	for _, asset := range assets {
		fullKey := testPrefix + "/2025/11/" + asset.OriginalFileName
		_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(assetsBucket),
			Key:    aws.String(fullKey),
		})
	}
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(manifestBucket),
		Key:    aws.String(manifestKey),
	})
	t.Log("Cleanup complete")
}
