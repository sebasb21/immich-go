package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ManifestEntry tracks a single backed up asset
type ManifestEntry struct {
	AssetID    string    `json:"asset_id"`
	Checksum   string    `json:"checksum"`
	BackupPath string    `json:"backup_path"`
	BackedUpAt time.Time `json:"backed_up_at"`
	FileSize   int       `json:"file_size"`
}

// Manifest tracks all backed up assets
type Manifest struct {
	mu      sync.RWMutex
	Entries map[string]ManifestEntry `json:"entries"` // keyed by AssetID
	path    string
}

// NewManifest creates a new empty manifest
func NewManifest(path string) *Manifest {
	return &Manifest{
		Entries: make(map[string]ManifestEntry),
		path:    path,
	}
}

// Load reads manifest from disk
func (m *Manifest) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.path)
	if os.IsNotExist(err) {
		// No manifest yet, start fresh
		m.Entries = make(map[string]ManifestEntry)
		return nil
	}
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &m.Entries)
}

// Save writes manifest to disk
func (m *Manifest) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Ensure directory exists
	dir := filepath.Dir(m.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(m.Entries, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(m.path, data, 0644)
}

// ShouldBackup checks if an asset needs to be backed up
// Returns true if asset is not in manifest or checksum differs
func (m *Manifest) ShouldBackup(assetID, checksum string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.Entries[assetID]
	if !exists {
		return true
	}
	// Re-backup if checksum changed (asset was modified)
	return entry.Checksum != checksum
}

// Add records a backed up asset
func (m *Manifest) Add(entry ManifestEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Entries[entry.AssetID] = entry
}

// Count returns the number of entries in the manifest
func (m *Manifest) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Entries)
}
