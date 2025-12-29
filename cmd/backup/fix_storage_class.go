// Command Backup Fix-Storage-Class - Migrate S3 objects to DEEP_ARCHIVE storage class

package backup

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/simulot/immich-go/cmd"
)

type FixStorageClassCmd struct {
	*cmd.SharedFlags

	S3AssetsBucket   string
	S3ManifestBucket string
	S3Region         string
	S3AssetsPrefix   string

	DryRun      bool
	TargetClass types.StorageClass

	s3Backend *S3Backend
	manifest  map[string]ManifestEntry
}

type migrationProgress struct {
	total            int
	checked          atomic.Int64
	skippedManifest  atomic.Int64 // Skipped based on manifest StorageClass
	missing          atomic.Int64 // Missing in S3, need re-upload
	reuploaded       atomic.Int64 // Successfully re-uploaded
	alreadyOK        atomic.Int64
	migrated         atomic.Int64
	errors           atomic.Int64
}

func fixStorageClass(ctx context.Context, common *cmd.SharedFlags, args []string) error {
	app, err := newFixStorageClassCmd(ctx, common, args)
	if err != nil {
		return err
	}
	return app.run(ctx)
}

func newFixStorageClassCmd(ctx context.Context, common *cmd.SharedFlags, args []string) (*FixStorageClassCmd, error) {
	app := &FixStorageClassCmd{
		SharedFlags: common,
		TargetClass: types.StorageClassDeepArchive,
	}

	fs := flag.NewFlagSet("fix-storage-class", flag.ExitOnError)

	// S3 flags
	fs.StringVar(&app.S3AssetsBucket, "s3-assets-bucket", "", "S3 bucket for assets")
	fs.StringVar(&app.S3ManifestBucket, "s3-manifest-bucket", "", "S3 bucket for manifest")
	fs.StringVar(&app.S3Region, "s3-region", "", "AWS region")
	fs.StringVar(&app.S3AssetsPrefix, "s3-assets-prefix", "", "S3 key prefix (optional)")

	// Migration flags
	fs.BoolVar(&app.DryRun, "dry-run", false, "Show what would be done without making changes")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Validate required flags
	if app.S3AssetsBucket == "" || app.S3ManifestBucket == "" || app.S3Region == "" {
		return nil, fmt.Errorf("s3-assets-bucket, s3-manifest-bucket, and s3-region are required")
	}

	return app, nil
}

func (app *FixStorageClassCmd) run(ctx context.Context) error {
	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var interrupted atomic.Bool
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		app.Log.Info(fmt.Sprintf("Received signal %v, saving manifest...", sig))
		interrupted.Store(true)
		cancel()
	}()

	app.Log.Info("Starting storage class migration...")
	app.Log.Info(fmt.Sprintf("  Assets bucket: %s", app.S3AssetsBucket))
	app.Log.Info(fmt.Sprintf("  Manifest bucket: %s", app.S3ManifestBucket))
	app.Log.Info(fmt.Sprintf("  Target storage class: %s", app.TargetClass))
	if app.DryRun {
		app.Log.Info("  Dry-run mode: no changes will be made")
	}

	// Initialize Immich client (needed for re-uploading missing assets)
	if app.Immich == nil {
		app.Log.Info("Initializing connection to Immich server...")
		if err := app.Start(ctx); err != nil {
			return fmt.Errorf("failed to connect to Immich: %w", err)
		}
	}

	// Initialize S3 backend
	s3Backend, err := NewS3Backend(ctx, app.S3AssetsBucket, app.S3ManifestBucket,
		app.S3Region, app.S3AssetsPrefix, 0)
	if err != nil {
		return fmt.Errorf("failed to initialize S3: %w", err)
	}
	app.s3Backend = s3Backend

	// Download manifest
	app.Log.Info("Downloading manifest from S3...")
	manifest, err := s3Backend.DownloadManifest(ctx, "immich-backup-manifest.json")
	if err != nil {
		return fmt.Errorf("failed to load manifest: %w", err)
	}
	app.manifest = manifest

	progress := &migrationProgress{
		total: len(manifest),
	}

	app.Log.Info(fmt.Sprintf("Found %d assets in manifest", progress.total))

	// Set up worker pool with 5 threads
	const numWorkers = 5
	type workItem struct {
		assetID string
		entry   ManifestEntry
	}

	workChan := make(chan workItem, numWorkers*2)
	var wg sync.WaitGroup
	var manifestMu sync.Mutex

	// Track which assets are currently being processed to avoid duplicates
	processingAssets := make(map[string]bool)
	var processingMu sync.Mutex

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for work := range workChan {
				if interrupted.Load() {
					continue // Drain channel but don't process
				}

				// Check if this asset is already being processed by another worker
				processingMu.Lock()
				if processingAssets[work.assetID] {
					processingMu.Unlock()
					continue // Skip, another worker is handling this
				}
				processingAssets[work.assetID] = true
				processingMu.Unlock()

				// Get latest entry from manifest (may have been updated by another worker)
				manifestMu.Lock()
				entry := manifest[work.assetID]

				// Skip if already processed by another worker
				if entry.StorageClass == string(types.StorageClassDeepArchive) {
					manifestMu.Unlock()
					processingMu.Lock()
					delete(processingAssets, work.assetID)
					processingMu.Unlock()
					progress.skippedManifest.Add(1)
					progress.checked.Add(1)
					continue
				}
				manifestMu.Unlock()

				if err := app.processEntry(ctx, work.assetID, &entry, progress, &manifestMu); err != nil {
					app.Log.Error(fmt.Sprintf("Worker %d: Error processing %s: %v", workerID, entry.BackupPath, err))
				}

				// Always update manifest with the potentially modified entry (even on error, to track progress)
				manifestMu.Lock()
				manifest[work.assetID] = entry
				manifestMu.Unlock()

				// Mark as done processing
				processingMu.Lock()
				delete(processingAssets, work.assetID)
				processingMu.Unlock()
			}
		}(i)
	}

	// Progress reporter and manifest saver goroutine
	progressDone := make(chan struct{})
	var lastSavedCount atomic.Int64
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		defer close(progressDone)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checked := progress.checked.Load()
				skippedManifest := progress.skippedManifest.Load()
				missing := progress.missing.Load()
				reuploaded := progress.reuploaded.Load()
				migrated := progress.migrated.Load()
				alreadyOK := progress.alreadyOK.Load()
				errors := progress.errors.Load()

				app.Log.Info(fmt.Sprintf("Progress: %d/%d checked, %d skipped, %d missing, %d reuploaded, %d migrated, %d OK, %d errors",
					checked, progress.total, skippedManifest, missing, reuploaded, migrated, alreadyOK, errors))

				// Save manifest every 200 operations
				if !app.DryRun && checked > 0 {
					lastSaved := lastSavedCount.Load()
					if checked-lastSaved >= 200 {
						app.Log.Info("Periodic manifest save (every 200 operations)...")
						manifestMu.Lock()
						if err := s3Backend.UploadManifest(ctx, "immich-backup-manifest.json", manifest); err != nil {
							app.Log.Error(fmt.Sprintf("Failed to save manifest: %v", err))
						} else {
							app.Log.Info(fmt.Sprintf("Manifest saved (checked: %d/%d)", checked, progress.total))
							lastSavedCount.Store(checked)
						}
						manifestMu.Unlock()
					}
				}
			}
		}
	}()

	// Create a slice of work items to avoid concurrent map iteration and map write
	workItems := make([]workItem, 0, len(manifest))
	pathToAssetID := make(map[string][]string) // Track duplicate paths
	for assetID, entry := range manifest {
		workItems = append(workItems, workItem{assetID: assetID, entry: entry})
		pathToAssetID[entry.BackupPath] = append(pathToAssetID[entry.BackupPath], assetID)
	}

	// Check for duplicate paths
	duplicatePaths := 0
	for path, assetIDs := range pathToAssetID {
		if len(assetIDs) > 1 {
			duplicatePaths++
			app.Log.Warn(fmt.Sprintf("Duplicate path in manifest: %s has %d asset IDs: %v", path, len(assetIDs), assetIDs))
		}
	}
	if duplicatePaths > 0 {
		app.Log.Warn(fmt.Sprintf("Found %d paths with duplicate asset IDs in manifest", duplicatePaths))
	}

	// Feed work to workers
	for _, work := range workItems {
		if interrupted.Load() {
			app.Log.Info("Interrupted by user, stopping...")
			break
		}

		select {
		case <-ctx.Done():
			app.Log.Info("Context cancelled, stopping...")
			interrupted.Store(true)
			break
		case workChan <- work:
			// Work sent
		}
	}

	// Close work channel and wait for workers to finish
	close(workChan)
	wg.Wait()

	// Stop progress reporter
	cancel()
	<-progressDone

	// Final manifest save
	wasInterrupted := interrupted.Load()
	if !app.DryRun && !wasInterrupted {
		app.Log.Info("Saving final manifest...")
		manifestMu.Lock()
		err := s3Backend.UploadManifest(ctx, "immich-backup-manifest.json", manifest)
		manifestMu.Unlock()
		if err != nil {
			return fmt.Errorf("failed to save manifest: %w", err)
		}
		app.Log.Info("Manifest saved successfully")
	} else if wasInterrupted && !app.DryRun {
		// Save on interruption too
		app.Log.Info("Saving manifest after interruption...")
		saveCtx := context.Background() // Use background context to ensure save completes
		manifestMu.Lock()
		err := s3Backend.UploadManifest(saveCtx, "immich-backup-manifest.json", manifest)
		manifestMu.Unlock()
		if err != nil {
			app.Log.Error(fmt.Sprintf("Failed to save manifest: %v", err))
		} else {
			app.Log.Info("Manifest saved successfully")
		}
	}

	// Print summary
	app.Log.Info("Migration summary:")
	app.Log.Info(fmt.Sprintf("  Total assets: %d", progress.total))
	app.Log.Info(fmt.Sprintf("  Checked: %d", progress.checked.Load()))
	app.Log.Info(fmt.Sprintf("  Skipped (manifest): %d", progress.skippedManifest.Load()))
	app.Log.Info(fmt.Sprintf("  Missing in S3: %d", progress.missing.Load()))
	app.Log.Info(fmt.Sprintf("  Re-uploaded: %d", progress.reuploaded.Load()))
	app.Log.Info(fmt.Sprintf("  Already OK: %d", progress.alreadyOK.Load()))
	app.Log.Info(fmt.Sprintf("  Migrated: %d", progress.migrated.Load()))
	app.Log.Info(fmt.Sprintf("  Errors: %d", progress.errors.Load()))

	errorCount := progress.errors.Load()
	if errorCount > 0 {
		return fmt.Errorf("completed with %d errors", errorCount)
	}

	if wasInterrupted {
		app.Log.Info("Migration interrupted but can be resumed by running the command again")
	} else {
		app.Log.Info("Migration complete!")
	}

	// Stop listening for signals
	signal.Stop(sigChan)

	return nil
}

func (app *FixStorageClassCmd) processEntry(ctx context.Context, assetID string, entry *ManifestEntry, progress *migrationProgress, manifestMu *sync.Mutex) error {
	progress.checked.Add(1)

	// Check current storage class
	currentClass, err := app.s3Backend.GetStorageClass(ctx, entry.BackupPath)
	if err != nil {
		if IsNoSuchKeyError(err) {
			// Asset is in manifest but missing from S3 - need to re-upload
			progress.missing.Add(1)
			app.Log.Warn(fmt.Sprintf("Asset missing in S3: %s (AssetID: %s) - attempting re-upload", entry.BackupPath, assetID))

			if app.DryRun {
				app.Log.Info(fmt.Sprintf("  [DRY-RUN] Would re-upload asset %s from Immich", assetID))
				return nil
			}

			// Download asset from Immich server
			if err := app.reuploadAsset(ctx, assetID, entry, progress); err != nil {
				progress.errors.Add(1)
				return fmt.Errorf("failed to re-upload missing asset %s: %w", assetID, err)
			}

			progress.reuploaded.Add(1)
			app.Log.Info(fmt.Sprintf("✓ Successfully re-uploaded missing asset: %s", entry.BackupPath))
			return nil
		}
		progress.errors.Add(1)
		return fmt.Errorf("failed to check storage class: %w", err)
	}

	// Normalize empty storage class to STANDARD
	if currentClass == "" {
		currentClass = types.StorageClassStandard
	}

	// Check if already correct
	if currentClass == app.TargetClass {
		// Update manifest if not already set
		if entry.StorageClass != string(app.TargetClass) {
			entry.StorageClass = string(app.TargetClass)
		}
		progress.alreadyOK.Add(1)
		return nil
	}

	// Needs migration
	app.Log.Info(fmt.Sprintf("Migrating %s: %s -> %s",
		entry.BackupPath, GetStorageClassName(currentClass), app.TargetClass))

	if app.DryRun {
		progress.migrated.Add(1)
		return nil
	}

	// Migrate
	if err := app.s3Backend.MigrateStorageClass(ctx, entry.BackupPath, app.TargetClass); err != nil {
		progress.errors.Add(1)
		return fmt.Errorf("migration failed: %w", err)
	}

	// Verify
	if err := app.s3Backend.VerifyStorageClass(ctx, entry.BackupPath, app.TargetClass); err != nil {
		progress.errors.Add(1)
		return fmt.Errorf("verification failed: %w", err)
	}

	app.Log.Info(fmt.Sprintf("✓ Successfully migrated %s", entry.BackupPath))

	// Update manifest
	entry.StorageClass = string(app.TargetClass)
	progress.migrated.Add(1)

	return nil
}

// reuploadAsset downloads an asset from Immich and re-uploads it to S3
func (app *FixStorageClassCmd) reuploadAsset(ctx context.Context, assetID string, entry *ManifestEntry, progress *migrationProgress) error {
	app.Log.Info(fmt.Sprintf("Downloading asset %s from Immich server...", assetID))

	// Download asset from Immich
	reader, err := app.Immich.DownloadAsset(ctx, assetID)
	if err != nil {
		return fmt.Errorf("failed to download asset from Immich: %w", err)
	}
	defer reader.Close()

	// Determine content type from file extension
	contentType := "application/octet-stream"
	ext := filepath.Ext(entry.BackupPath)
	switch strings.ToLower(ext) {
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".png":
		contentType = "image/png"
	case ".gif":
		contentType = "image/gif"
	case ".mp4":
		contentType = "video/mp4"
	case ".mov":
		contentType = "video/quicktime"
	case ".heic":
		contentType = "image/heic"
	case ".webp":
		contentType = "image/webp"
	}

	app.Log.Info(fmt.Sprintf("Uploading asset to S3: %s", entry.BackupPath))

	// Upload to S3
	if err := app.s3Backend.UploadAsset(ctx, entry.BackupPath, reader, contentType); err != nil {
		return fmt.Errorf("failed to upload asset to S3: %w", err)
	}

	// Update manifest to mark as DEEP_ARCHIVE (since UploadAsset uses DEEP_ARCHIVE)
	entry.StorageClass = string(types.StorageClassDeepArchive)

	return nil
}
