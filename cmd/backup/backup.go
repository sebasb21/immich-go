// Command Backup - Backup Immich assets to local storage or S3

package backup

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/simulot/immich-go/cmd"
	"github.com/simulot/immich-go/helpers/myflag"
	"github.com/simulot/immich-go/immich"
)

type BackupCmd struct {
	*cmd.SharedFlags

	// Output destination (local path)
	Output string

	// S3 configuration
	S3AssetsBucket   string
	S3ManifestBucket string
	S3Region         string
	S3AssetsPrefix   string

	// Upload configuration
	MaxUploadBandwidth int64 // bytes per second, 0 = unlimited

	// Filtering options
	DateRange immich.DateRange

	// Behavior
	DryRun bool

	// Internal state
	manifest  *Manifest
	s3Backend *S3Backend
	useS3     bool
}

// Progress counters
type backupProgress struct {
	total   int
	scanned int
	skipped int
	backed  int
	errors  int
	bytes   int64
}

func BackupCommand(ctx context.Context, common *cmd.SharedFlags, args []string) error {
	app, err := newCommand(ctx, common, args)
	if err != nil {
		return err
	}
	return app.run(ctx)
}

func newCommand(ctx context.Context, common *cmd.SharedFlags, args []string) (*BackupCmd, error) {
	cmd := flag.NewFlagSet("backup", flag.ExitOnError)

	app := BackupCmd{
		SharedFlags: common,
	}

	app.SharedFlags.SetFlags(cmd)

	cmd.StringVar(&app.Output, "output", "", "Local output directory for backup")

	// S3 flags
	cmd.StringVar(&app.S3AssetsBucket, "s3-assets-bucket", "", "S3 bucket for storing assets")
	cmd.StringVar(&app.S3ManifestBucket, "s3-manifest-bucket", "", "S3 bucket for storing manifest")
	cmd.StringVar(&app.S3Region, "s3-region", "", "AWS region for S3 buckets")
	cmd.StringVar(&app.S3AssetsPrefix, "s3-assets-prefix", "", "Prefix for assets in S3 bucket")

	// Upload configuration
	cmd.Int64Var(&app.MaxUploadBandwidth, "max-upload-bandwidth", 0, "Maximum upload bandwidth in bytes per second (0 = unlimited). Example: 1048576 for 1 MB/s")

	// Filtering
	cmd.Var(&app.DateRange, "date", "Date range for backup (format: YYYY-MM-DD:YYYY-MM-DD)")

	cmd.BoolFunc("dry-run", "Display actions but don't download or upload anything", myflag.BoolFlagFn(&app.DryRun, false))

	err := cmd.Parse(args)
	if err != nil {
		return nil, err
	}

	// Validate required flags
	if app.Output == "" && app.S3AssetsBucket == "" {
		return nil, fmt.Errorf("either -output (local) or -s3-assets-bucket (S3) is required")
	}

	// Determine mode
	app.useS3 = app.S3AssetsBucket != ""

	// Validate S3 configuration
	if app.useS3 {
		if app.S3ManifestBucket == "" {
			return nil, fmt.Errorf("-s3-manifest-bucket is required when using S3")
		}
		if app.S3Region == "" {
			return nil, fmt.Errorf("-s3-region is required when using S3")
		}
	}

	// Start connection to Immich server
	err = app.SharedFlags.Start(ctx)
	if err != nil {
		return nil, err
	}

	return &app, nil
}

func (app *BackupCmd) run(ctx context.Context) error {
	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Track if we were interrupted
	interrupted := false

	go func() {
		sig := <-sigChan
		app.Log.Info(fmt.Sprintf("Received signal %v, initiating graceful shutdown...", sig))
		interrupted = true
		cancel()
	}()

	if app.useS3 {
		app.Log.Info("Starting backup to S3...")
		app.Log.Info(fmt.Sprintf("  Assets bucket: %s", app.S3AssetsBucket))
		app.Log.Info(fmt.Sprintf("  Manifest bucket: %s", app.S3ManifestBucket))
		if app.MaxUploadBandwidth > 0 {
			app.Log.Info(fmt.Sprintf("  Upload bandwidth limit: %s/s", formatBytes(app.MaxUploadBandwidth)))
		}
	} else {
		app.Log.Info("Starting backup to local storage...")
		app.Log.Info(fmt.Sprintf("  Output: %s", app.Output))
	}

	// Get asset statistics
	stats, err := app.Immich.GetAssetStatistics(ctx)
	if err != nil {
		return fmt.Errorf("failed to get asset statistics: %w", err)
	}
	app.Log.Info(fmt.Sprintf("Server has %d assets (%d images, %d videos)", stats.Total, stats.Images, stats.Videos))

	if app.DryRun {
		app.Log.Info("Dry-run mode: no files will be downloaded or uploaded")
	}

	// Initialize storage backend
	if app.useS3 {
		if err := app.initS3(ctx); err != nil {
			return err
		}
	} else {
		if err := app.initLocal(); err != nil {
			return err
		}
	}

	// Process assets
	progress := &backupProgress{total: stats.Total}
	startTime := time.Now()

	err = app.Immich.GetAllAssetsWithFilter(ctx, func(asset *immich.Asset) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return app.processAsset(ctx, asset, progress)
		}
	})

	if err != nil && !interrupted {
		app.Log.Error(fmt.Sprintf("Backup stopped with error: %v", err))
	}

	// Save manifest (use background context to ensure save completes even after cancellation)
	if !app.DryRun && app.manifest != nil {
		if interrupted {
			app.Log.Info("Saving manifest before shutdown...")
		}
		saveCtx := context.Background()
		if err := app.saveManifest(saveCtx); err != nil {
			app.Log.Error(fmt.Sprintf("Failed to save manifest: %v", err))
		} else if interrupted {
			app.Log.Info("Manifest saved successfully")
		}
	}

	// Print summary
	elapsed := time.Since(startTime)
	if interrupted {
		app.Log.Info(fmt.Sprintf("Backup interrupted after %v", elapsed.Round(time.Second)))
	} else {
		app.Log.Info(fmt.Sprintf("Backup complete in %v", elapsed.Round(time.Second)))
	}
	app.Log.Info(fmt.Sprintf("  Scanned: %d", progress.scanned))
	app.Log.Info(fmt.Sprintf("  Skipped (already backed up): %d", progress.skipped))
	app.Log.Info(fmt.Sprintf("  Backed up: %d", progress.backed))
	app.Log.Info(fmt.Sprintf("  Errors: %d", progress.errors))
	app.Log.Info(fmt.Sprintf("  Total bytes: %s", formatBytes(progress.bytes)))

	// Stop listening for signals
	signal.Stop(sigChan)

	return nil
}

func (app *BackupCmd) initS3(ctx context.Context) error {
	var err error
	app.s3Backend, err = NewS3Backend(ctx, app.S3AssetsBucket, app.S3ManifestBucket, app.S3Region, app.S3AssetsPrefix, app.MaxUploadBandwidth)
	if err != nil {
		return fmt.Errorf("failed to initialize S3: %w", err)
	}

	// Download manifest from S3
	manifestKey := "immich-backup-manifest.json"
	entries, err := app.s3Backend.DownloadManifest(ctx, manifestKey)
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}

	// Create manifest with S3 entries
	app.manifest = NewManifest("")
	app.manifest.Entries = entries
	app.Log.Info(fmt.Sprintf("Manifest loaded from S3: %d assets already backed up", app.manifest.Count()))

	return nil
}

func (app *BackupCmd) initLocal() error {
	manifestPath := filepath.Join(app.Output, ".immich-backup-manifest.json")
	app.manifest = NewManifest(manifestPath)

	if err := app.manifest.Load(); err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}
	app.Log.Info(fmt.Sprintf("Manifest loaded: %d assets already backed up", app.manifest.Count()))

	// Ensure output directory exists
	if !app.DryRun {
		if err := os.MkdirAll(app.Output, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	return nil
}

func (app *BackupCmd) saveManifest(ctx context.Context) error {
	if app.useS3 {
		manifestKey := "immich-backup-manifest.json"
		return app.s3Backend.UploadManifest(ctx, manifestKey, app.manifest.Entries)
	}
	return app.manifest.Save()
}

func (app *BackupCmd) processAsset(ctx context.Context, asset *immich.Asset, progress *backupProgress) error {
	progress.scanned++

	// Log progress every 100 assets
	if progress.scanned%100 == 0 {
		app.Log.Info(fmt.Sprintf("Progress: %d/%d scanned, %d backed up, %d skipped",
			progress.scanned, progress.total, progress.backed, progress.skipped))
	}

	// Skip trashed assets
	if asset.IsTrashed {
		return nil
	}

	// Apply date filter if set
	if app.DateRange.IsSet() {
		assetDate := asset.FileCreatedAt.Time
		if assetDate.IsZero() {
			assetDate = asset.FileModifiedAt.Time
		}
		if !app.DateRange.InRange(assetDate) {
			return nil
		}
	}

	// Check if already backed up with same checksum
	if !app.manifest.ShouldBackup(asset.ID, asset.Checksum) {
		progress.skipped++
		return nil
	}

	// Get backup path/key
	backupPath := app.getBackupPath(asset)

	if app.DryRun {
		if app.useS3 {
			app.Log.Info(fmt.Sprintf("Would backup to S3: %s -> s3://%s/%s", asset.OriginalFileName, app.S3AssetsBucket, backupPath))
		} else {
			app.Log.Info(fmt.Sprintf("Would backup: %s -> %s", asset.OriginalFileName, backupPath))
		}
		progress.backed++
		return nil
	}

	// Download and save asset
	var err error
	if app.useS3 {
		err = app.uploadToS3(ctx, asset, backupPath)
	} else {
		err = app.downloadToLocal(ctx, asset, backupPath)
	}

	if err != nil {
		app.Log.Error(fmt.Sprintf("Failed to backup %s: %v", asset.OriginalFileName, err))
		progress.errors++
		return nil // Continue with next asset
	}

	// Update manifest
	app.manifest.Add(ManifestEntry{
		AssetID:    asset.ID,
		Checksum:   asset.Checksum,
		BackupPath: backupPath,
		BackedUpAt: time.Now(),
		FileSize:   asset.ExifInfo.FileSizeInByte,
	})

	progress.backed++
	progress.bytes += int64(asset.ExifInfo.FileSizeInByte)

	// Save manifest periodically (every 10 files)
	if progress.backed%10 == 0 {
		if err := app.saveManifest(ctx); err != nil {
			app.Log.Error(fmt.Sprintf("Failed to save manifest: %v", err))
		}
	}

	return nil
}

func (app *BackupCmd) getBackupPath(asset *immich.Asset) string {
	// Organize by date: YYYY/MM/filename
	date := asset.FileCreatedAt.Time
	if date.IsZero() {
		date = asset.FileModifiedAt.Time
	}
	if date.IsZero() {
		date = time.Now()
	}

	year := date.Format("2006")
	month := date.Format("01")
	filename := sanitizeFilename(asset.OriginalFileName)

	if app.useS3 {
		// For S3, just return the key (prefix is handled by S3Backend)
		return fmt.Sprintf("%s/%s/%s", year, month, filename)
	}
	return filepath.Join(app.Output, year, month, filename)
}

func (app *BackupCmd) uploadToS3(ctx context.Context, asset *immich.Asset, s3Key string) error {
	// Download from Immich
	reader, err := app.Immich.DownloadAsset(ctx, asset.ID)
	if err != nil {
		return fmt.Errorf("failed to download from Immich: %w", err)
	}
	defer reader.Close()

	// Determine content type
	contentType := "application/octet-stream"
	if asset.Type == "IMAGE" {
		contentType = "image/jpeg" // Could be improved with actual detection
	} else if asset.Type == "VIDEO" {
		contentType = "video/mp4"
	}

	// Stream to S3
	fullKey := s3Key
	if app.S3AssetsPrefix != "" {
		fullKey = app.S3AssetsPrefix + "/" + s3Key
	}

	if err := app.s3Backend.UploadAsset(ctx, s3Key, reader, contentType); err != nil {
		return err
	}

	app.Log.Info(fmt.Sprintf("Backed up to S3: %s -> s3://%s/%s", asset.OriginalFileName, app.S3AssetsBucket, fullKey))
	return nil
}

func (app *BackupCmd) downloadToLocal(ctx context.Context, asset *immich.Asset, destPath string) error {
	// Ensure directory exists
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Download from Immich
	reader, err := app.Immich.DownloadAsset(ctx, asset.ID)
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}
	defer reader.Close()

	// Create destination file
	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Copy data
	written, err := io.Copy(file, reader)
	if err != nil {
		os.Remove(destPath) // Clean up partial file
		return fmt.Errorf("failed to write file: %w", err)
	}

	app.Log.Info(fmt.Sprintf("Backed up: %s (%s)", asset.OriginalFileName, formatBytes(written)))
	return nil
}

func sanitizeFilename(name string) string {
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	return replacer.Replace(name)
}

func formatBytes(b int64) string {
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
