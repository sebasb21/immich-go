// Command Backup - Backup Immich assets to local storage or S3

package backup

import (
	"context"
	"fmt"

	"github.com/simulot/immich-go/cmd"
)

// BackupCommand dispatches to backup subcommands
func BackupCommand(ctx context.Context, common *cmd.SharedFlags, args []string) error {
	if len(args) > 0 {
		subcommand := args[0]
		args = args[1:]

		switch subcommand {
		case "run":
			return runBackup(ctx, common, args)
		case "fix-storage-class":
			return fixStorageClass(ctx, common, args)
		default:
			return fmt.Errorf("unknown backup subcommand: %q (available: run, fix-storage-class)", subcommand)
		}
	}

	// Backward compatibility: no subcommand = run backup
	return runBackup(ctx, common, args)
}
