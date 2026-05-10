package offset

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestMemoryStorage(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	// Test save and load
	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
		LSN:        67890,
	}

	err := storage.Save(ctx, "task-1", pos)
	if err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	loaded, err := storage.Load(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	if loaded == nil {
		t.Fatal("Expected non-nil position")
	}

	if loaded.BinlogFile != pos.BinlogFile {
		t.Errorf("Expected binlog file '%s', got '%s'", pos.BinlogFile, loaded.BinlogFile)
	}

	if loaded.BinlogPos != pos.BinlogPos {
		t.Errorf("Expected binlog pos %d, got %d", pos.BinlogPos, loaded.BinlogPos)
	}

	// Test load non-existent
	empty, err := storage.Load(ctx, "non-existent")
	if err != nil {
		t.Fatalf("Failed to load non-existent: %v", err)
	}

	if empty != nil {
		t.Error("Expected nil for non-existent task")
	}

	// Test delete
	err = storage.Delete(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	deleted, err := storage.Load(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to load deleted: %v", err)
	}

	if deleted != nil {
		t.Error("Expected nil after delete")
	}

	// Test close
	err = storage.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
}

func TestMemoryStorageIsolation(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	storage.Save(ctx, "task-1", pos)

	// Modify returned position should not affect stored
	loaded, _ := storage.Load(ctx, "task-1")
	loaded.BinlogFile = "modified"

	loaded2, _ := storage.Load(ctx, "task-1")
	if loaded2.BinlogFile == "modified" {
		t.Error("Position should be isolated (cloned)")
	}
}

func TestFileStorage(t *testing.T) {
	// Create temp file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "offsets.json")

	storage, err := NewFileStorage(path, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Test save and load
	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
		LSN:        67890,
		TxID:       "tx-123",
		SeqNo:      5,
	}

	err = storage.Save(ctx, "task-1", pos)
	if err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	// Flush to disk
	err = storage.flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	loaded, err := storage.Load(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	if loaded == nil {
		t.Fatal("Expected non-nil position")
	}

	if loaded.BinlogFile != pos.BinlogFile {
		t.Errorf("Expected binlog file '%s', got '%s'", pos.BinlogFile, loaded.BinlogFile)
	}

	if loaded.TxID != pos.TxID {
		t.Errorf("Expected TxID '%s', got '%s'", pos.TxID, loaded.TxID)
	}

	// Test persistence - create new storage instance
	storage2, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("Failed to create second storage: %v", err)
	}
	defer storage2.Close()

	loaded2, err := storage2.Load(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to load from second storage: %v", err)
	}

	if loaded2 == nil {
		t.Fatal("Expected position to persist")
	}

	if loaded2.BinlogFile != pos.BinlogFile {
		t.Errorf("Expected persisted binlog file '%s', got '%s'", pos.BinlogFile, loaded2.BinlogFile)
	}
}

func TestFileStorageMultipleTasks(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "offsets.json")

	storage, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Save multiple tasks
	for i := 1; i <= 5; i++ {
		pos := &event.Position{
			BinlogFile: "mysql-bin.000001",
			BinlogPos:  uint32(i * 1000),
		}
		taskID := string(rune('0' + i))
		err := storage.Save(ctx, taskID, pos)
		if err != nil {
			t.Fatalf("Failed to save task %s: %v", taskID, err)
		}
	}

	// Flush and reload
	storage.flush()

	storage2, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("Failed to create second storage: %v", err)
	}
	defer storage2.Close()

	// Verify all tasks
	for i := 1; i <= 5; i++ {
		taskID := string(rune('0' + i))
		loaded, err := storage2.Load(ctx, taskID)
		if err != nil {
			t.Fatalf("Failed to load task %s: %v", taskID, err)
		}
		if loaded == nil {
			t.Errorf("Expected task %s to exist", taskID)
		}
	}
}

func TestFileStorageDelete(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "offsets.json")

	storage, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	storage.Save(ctx, "task-1", pos)
	storage.Save(ctx, "task-2", pos)

	// Delete task-1
	err = storage.Delete(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify task-1 is deleted
	loaded, err := storage.Load(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}
	if loaded != nil {
		t.Error("Expected nil for deleted task")
	}

	// Verify task-2 still exists
	loaded2, err := storage.Load(ctx, "task-2")
	if err != nil {
		t.Fatalf("Failed to load task-2: %v", err)
	}
	if loaded2 == nil {
		t.Error("Expected task-2 to still exist")
	}
}

func TestFileStorageEmptyPath(t *testing.T) {
	_, err := NewFileStorage("", 0)
	if err == nil {
		t.Error("Expected error for empty path")
	}
}

func TestFileStorageDirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nested", "deep", "offsets.json")

	storage, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Verify directory was created
	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		t.Error("Expected directory to be created")
	}
}

func TestNewStorageMemory(t *testing.T) {
	cfg := &Config{
		Backend: "memory",
	}

	storage, err := NewStorage(cfg)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	if _, ok := storage.(*MemoryStorage); !ok {
		t.Error("Expected MemoryStorage")
	}
}

func TestNewStorageFile(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &Config{
		Backend:       "file",
		Path:          filepath.Join(tmpDir, "offsets.json"),
		FlushInterval: 1000,
	}

	storage, err := NewStorage(cfg)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	if _, ok := storage.(*FileStorage); !ok {
		t.Error("Expected FileStorage")
	}
}

func TestNewStorageUnsupported(t *testing.T) {
	cfg := &Config{
		Backend: "unsupported",
	}

	_, err := NewStorage(cfg)
	if err == nil {
		t.Error("Expected error for unsupported backend")
	}
}

func TestConfigDefaults(t *testing.T) {
	// Test default backend (memory)
	cfg := &Config{}
	storage, err := NewStorage(cfg)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	if _, ok := storage.(*MemoryStorage); !ok {
		t.Error("Expected MemoryStorage as default")
	}
}

func TestPositionWithAllFields(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	pos := &event.Position{
		BinlogFile: "mysql-bin.000002",
		BinlogPos:  54321,
		LSN:        98765,
		TxID:       "tx-456",
		SeqNo:      10,
		CommitTime: time.Now(),
	}

	err := storage.Save(ctx, "task-1", pos)
	if err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	loaded, err := storage.Load(ctx, "task-1")
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	if loaded.BinlogFile != pos.BinlogFile {
		t.Errorf("BinlogFile mismatch")
	}
	if loaded.BinlogPos != pos.BinlogPos {
		t.Errorf("BinlogPos mismatch")
	}
	if loaded.LSN != pos.LSN {
		t.Errorf("LSN mismatch")
	}
	if loaded.TxID != pos.TxID {
		t.Errorf("TxID mismatch")
	}
	if loaded.SeqNo != pos.SeqNo {
		t.Errorf("SeqNo mismatch")
	}
}

func TestFileStorageNonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nonexistent", "offsets.json")

	// Should succeed even if file doesn't exist
	storage, err := NewFileStorage(path, 0)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Load should return nil for non-existent task
	ctx := context.Background()
	loaded, err := storage.Load(ctx, "non-existent")
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}
	if loaded != nil {
		t.Error("Expected nil for non-existent task")
	}
}

func TestMemoryStorageConcurrent(t *testing.T) {
	storage := NewMemoryStorage()
	ctx := context.Background()

	// Run concurrent saves
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(n int) {
			taskID := string(rune('A' + n))
			pos := &event.Position{
				BinlogPos: uint32(n),
			}
			storage.Save(ctx, taskID, pos)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all saves completed
	for i := 0; i < 10; i++ {
		taskID := string(rune('A' + i))
		loaded, err := storage.Load(ctx, taskID)
		if err != nil {
			t.Errorf("Failed to load task %s: %v", taskID, err)
		}
		if loaded == nil {
			t.Errorf("Expected task %s to exist", taskID)
		}
	}
}
