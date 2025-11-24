//go:build integration

package immich

import (
	"context"
	"io"
	"os"
	"testing"
)

func TestDownloadAsset_Integration(t *testing.T) {
	// Integration test against real server
	// Run with: IMMICH_SERVER=http://your-server:2283 IMMICH_API_KEY=your-key go test -tags=integration -v ./immich -run TestDownloadAsset_Integration

	server := os.Getenv("IMMICH_SERVER")
	apiKey := os.Getenv("IMMICH_API_KEY")

	if server == "" || apiKey == "" {
		t.Skip("Skipping integration test: IMMICH_SERVER and IMMICH_API_KEY environment variables required")
	}

	ctx := context.Background()

	// Create client
	client, err := NewImmichClient(server, apiKey)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Validate connection
	user, err := client.ValidateConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to validate connection: %v", err)
	}
	t.Logf("Connected as user: %s %s (%s)", user.FirstName, user.LastName, user.Email)

	// Get assets and find a small image (< 10MB) to download quickly
	var selectedAsset *Asset
	err = client.GetAllAssetsWithFilter(ctx, func(asset *Asset) error {
		// Pick a small image file for faster testing
		if asset.Type == "IMAGE" && asset.ExifInfo.FileSizeInByte > 0 && asset.ExifInfo.FileSizeInByte < 10*1024*1024 {
			selectedAsset = asset
			return io.EOF // Stop iteration
		}
		return nil
	})
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to get assets: %v", err)
	}

	if selectedAsset == nil {
		t.Skip("No suitable small image found on server, skipping download test")
	}

	t.Logf("Downloading asset: ID=%s, FileName=%s, Type=%s, Size=%d bytes",
		selectedAsset.ID, selectedAsset.OriginalFileName, selectedAsset.Type, selectedAsset.ExifInfo.FileSizeInByte)

	reader, err := client.DownloadAsset(ctx, selectedAsset.ID)
	if err != nil {
		t.Fatalf("Failed to download asset %s: %v", selectedAsset.ID, err)
	}
	defer reader.Close()

	// Read and count bytes
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read asset data: %v", err)
	}

	t.Logf("Successfully downloaded %d bytes", len(data))

	// Verify we got some data
	if len(data) == 0 {
		t.Error("Downloaded 0 bytes, expected some data")
	}

	// Verify size matches if available
	if selectedAsset.ExifInfo.FileSizeInByte > 0 && len(data) != selectedAsset.ExifInfo.FileSizeInByte {
		t.Logf("Warning: Downloaded size (%d) differs from expected (%d)", len(data), selectedAsset.ExifInfo.FileSizeInByte)
	}

	t.Log("Download test PASSED")
}
