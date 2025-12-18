// Command Backup Fix-Storage-Class - Migrate S3 objects to DEEP_ARCHIVE storage class

package backup

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	total     int
	checked   int
	alreadyOK int
	migrated  int
	errors    int
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

	interrupted := false
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		app.Log.Info(fmt.Sprintf("Received signal %v, saving manifest...", sig))
		interrupted = true
		cancel()
	}()

	app.Log.Info("Starting storage class migration...")
	app.Log.Info(fmt.Sprintf("  Assets bucket: %s", app.S3AssetsBucket))
	app.Log.Info(fmt.Sprintf("  Manifest bucket: %s", app.S3ManifestBucket))
	app.Log.Info(fmt.Sprintf("  Target storage class: %s", app.TargetClass))
	if app.DryRun {
		app.Log.Info("  Dry-run mode: no changes will be made")
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

	// Process each entry
	count := 0
	for assetID, entry := range manifest {
		if interrupted {
			app.Log.Info("Interrupted by user, stopping...")
			break
		}

		count++
		if err := app.processEntry(ctx, assetID, &entry, progress); err != nil {
			app.Log.Error(fmt.Sprintf("Error processing %s: %v", entry.BackupPath, err))
			progress.errors++
		}

		// Save manifest every 10 objects
		if count%10 == 0 {
			app.Log.Info(fmt.Sprintf("Progress: %d/%d checked, %d migrated, %d already OK, %d errors",
				progress.checked, progress.total, progress.migrated, progress.alreadyOK, progress.errors))

			if !app.DryRun {
				if err := s3Backend.UploadManifest(ctx, "immich-backup-manifest.json", manifest); err != nil {
					app.Log.Error(fmt.Sprintf("Failed to save manifest: %v", err))
				} else {
					app.Log.Info("Manifest saved")
				}
			}
		}
	}

	// Final manifest save
	if !app.DryRun && !interrupted {
		app.Log.Info("Saving final manifest...")
		if err := s3Backend.UploadManifest(ctx, "immich-backup-manifest.json", manifest); err != nil {
			return fmt.Errorf("failed to save manifest: %w", err)
		}
		app.Log.Info("Manifest saved successfully")
	} else if interrupted && !app.DryRun {
		// Save on interruption too
		app.Log.Info("Saving manifest after interruption...")
		saveCtx := context.Background() // Use background context to ensure save completes
		if err := s3Backend.UploadManifest(saveCtx, "immich-backup-manifest.json", manifest); err != nil {
			app.Log.Error(fmt.Sprintf("Failed to save manifest: %v", err))
		} else {
			app.Log.Info("Manifest saved successfully")
		}
	}

	// Print summary
	app.Log.Info("Migration summary:")
	app.Log.Info(fmt.Sprintf("  Total assets: %d", progress.total))
	app.Log.Info(fmt.Sprintf("  Checked: %d", progress.checked))
	app.Log.Info(fmt.Sprintf("  Already OK: %d", progress.alreadyOK))
	app.Log.Info(fmt.Sprintf("  Migrated: %d", progress.migrated))
	app.Log.Info(fmt.Sprintf("  Errors: %d", progress.errors))

	if progress.errors > 0 {
		return fmt.Errorf("completed with %d errors", progress.errors)
	}

	if interrupted {
		app.Log.Info("Migration interrupted but can be resumed by running the command again")
	} else {
		app.Log.Info("Migration complete!")
	}

	// Stop listening for signals
	signal.Stop(sigChan)

	return nil
}

func (app *FixStorageClassCmd) processEntry(ctx context.Context, assetID string, entry *ManifestEntry, progress *migrationProgress) error {
	progress.checked++

	// Check current storage class
	currentClass, err := app.s3Backend.GetStorageClass(ctx, entry.BackupPath)
	if err != nil {
		if IsNoSuchKeyError(err) {
			app.Log.Warn(fmt.Sprintf("Object not found: %s (in manifest but not in S3)", entry.BackupPath))
			return nil // Don't fail, just skip
		}
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
			app.manifest[assetID] = *entry
		}
		progress.alreadyOK++
		return nil
	}

	// Needs migration
	app.Log.Info(fmt.Sprintf("Migrating %s: %s -> %s",
		entry.BackupPath, GetStorageClassName(currentClass), app.TargetClass))

	if app.DryRun {
		progress.migrated++
		return nil
	}

	// Migrate
	if err := app.s3Backend.MigrateStorageClass(ctx, entry.BackupPath, app.TargetClass); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	// Verify
	if err := app.s3Backend.VerifyStorageClass(ctx, entry.BackupPath, app.TargetClass); err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	app.Log.Info(fmt.Sprintf("✓ Successfully migrated %s", entry.BackupPath))

	// Update manifest
	entry.StorageClass = string(app.TargetClass)
	app.manifest[assetID] = *entry
	progress.migrated++

	return nil
}
