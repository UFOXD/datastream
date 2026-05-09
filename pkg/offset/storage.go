// Package offset provides position/offset storage for DataStream connectors.
package offset

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Storage defines the interface for offset storage.
type Storage interface {
	// Save saves the position for a task.
	Save(ctx context.Context, taskID string, pos *event.Position) error

	// Load loads the position for a task.
	Load(ctx context.Context, taskID string) (*event.Position, error)

	// Delete deletes the position for a task.
	Delete(ctx context.Context, taskID string) error

	// Close closes the storage.
	Close() error
}

// Config holds offset storage configuration.
type Config struct {
	// Backend type: memory, file, etcd
	Backend string `json:"backend"`

	// File path for file backend
	Path string `json:"path,omitempty"`

	// Etcd endpoints for etcd backend
	EtcdEndpoints []string `json:"etcdEndpoints,omitempty"`

	// Auto-flush interval in milliseconds
	FlushInterval int `json:"flushInterval,omitempty"`
}

// MemoryStorage is an in-memory offset storage.
type MemoryStorage struct {
	mu     sync.RWMutex
	offset map[string]*event.Position
}

// NewMemoryStorage creates a new memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		offset: make(map[string]*event.Position),
	}
}

// Save saves the position.
func (s *MemoryStorage) Save(ctx context.Context, taskID string, pos *event.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset[taskID] = pos.Clone()
	return nil
}

// Load loads the position.
func (s *MemoryStorage) Load(ctx context.Context, taskID string) (*event.Position, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if pos, ok := s.offset[taskID]; ok {
		return pos.Clone(), nil
	}
	return nil, nil
}

// Delete deletes the position.
func (s *MemoryStorage) Delete(ctx context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.offset, taskID)
	return nil
}

// Close closes the storage.
func (s *MemoryStorage) Close() error {
	return nil
}

// FileStorage is a file-based offset storage.
type FileStorage struct {
	path          string
	flushInterval time.Duration
	mu            sync.RWMutex
	offset        map[string]*event.Position
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewFileStorage creates a new file storage.
func NewFileStorage(path string, flushInterval time.Duration) (*FileStorage, error) {
	if path == "" {
		return nil, fmt.Errorf("path is required")
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	s := &FileStorage{
		path:          path,
		flushInterval: flushInterval,
		offset:        make(map[string]*event.Position),
		stopCh:        make(chan struct{}),
	}

	// Load existing offsets
	if err := s.load(); err != nil {
		log.Warn("failed to load existing offsets, starting fresh",
			zap.String("path", path),
			zap.Error(err))
	}

	// Start auto-flush goroutine
	if flushInterval > 0 {
		s.wg.Add(1)
		go s.autoFlush()
	}

	return s, nil
}

// Save saves the position.
func (s *FileStorage) Save(ctx context.Context, taskID string, pos *event.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset[taskID] = pos.Clone()
	return nil
}

// Load loads the position.
func (s *FileStorage) Load(ctx context.Context, taskID string) (*event.Position, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if pos, ok := s.offset[taskID]; ok {
		return pos.Clone(), nil
	}
	return nil, nil
}

// Delete deletes the position.
func (s *FileStorage) Delete(ctx context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.offset, taskID)
	return nil
}

// Close closes the storage and flushes to disk.
func (s *FileStorage) Close() error {
	close(s.stopCh)
	s.wg.Wait()
	return s.flush()
}

// load loads offsets from disk.
func (s *FileStorage) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist yet
		}
		return fmt.Errorf("failed to read offset file: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	var offsets map[string]*event.Position
	if err := json.Unmarshal(data, &offsets); err != nil {
		return fmt.Errorf("failed to unmarshal offsets: %w", err)
	}

	s.offset = offsets
	return nil
}

// flush writes offsets to disk.
func (s *FileStorage) flush() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := json.MarshalIndent(s.offset, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal offsets: %w", err)
	}

	// Write to temp file first, then rename for atomicity
	tmpPath := s.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write offset file: %w", err)
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("failed to rename offset file: %w", err)
	}

	return nil
}

// autoFlush periodically flushes offsets to disk.
func (s *FileStorage) autoFlush() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := s.flush(); err != nil {
				log.Error("failed to flush offsets", zap.Error(err))
			}
		}
	}
}

// EtcdStorage is an etcd-based offset storage.
type EtcdStorage struct {
	client   *clientv3.Client
	prefix   string
	mu       sync.RWMutex
	offset   map[string]*event.Position
	stopCh   chan struct{}
	wg       sync.WaitGroup
	interval time.Duration
}

// NewEtcdStorage creates a new etcd storage.
func NewEtcdStorage(endpoints []string, prefix string, flushInterval time.Duration) (*EtcdStorage, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints are required")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	if prefix == "" {
		prefix = "/datastream/offsets"
	}

	s := &EtcdStorage{
		client:   client,
		prefix:   prefix,
		offset:   make(map[string]*event.Position),
		stopCh:   make(chan struct{}),
		interval: flushInterval,
	}

	// Load existing offsets
	if err := s.loadAll(context.Background()); err != nil {
		log.Warn("failed to load existing offsets from etcd",
			zap.Strings("endpoints", endpoints),
			zap.Error(err))
	}

	// Start auto-flush goroutine
	if flushInterval > 0 {
		s.wg.Add(1)
		go s.autoFlush()
	}

	return s, nil
}

// Save saves the position.
func (s *EtcdStorage) Save(ctx context.Context, taskID string, pos *event.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset[taskID] = pos.Clone()

	// Immediately persist to etcd
	data, err := pos.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal position: %w", err)
	}

	key := fmt.Sprintf("%s/%s", s.prefix, taskID)
	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to save position to etcd: %w", err)
	}

	return nil
}

// Load loads the position.
func (s *EtcdStorage) Load(ctx context.Context, taskID string) (*event.Position, error) {
	s.mu.RLock()
	if pos, ok := s.offset[taskID]; ok {
		s.mu.RUnlock()
		return pos.Clone(), nil
	}
	s.mu.RUnlock()

	// Try to load from etcd
	key := fmt.Sprintf("%s/%s", s.prefix, taskID)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get position from etcd: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var pos event.Position
	if err := pos.UnmarshalBinary(resp.Kvs[0].Value); err != nil {
		return nil, fmt.Errorf("failed to unmarshal position: %w", err)
	}

	s.mu.Lock()
	s.offset[taskID] = &pos
	s.mu.Unlock()

	return &pos, nil
}

// Delete deletes the position.
func (s *EtcdStorage) Delete(ctx context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.offset, taskID)

	key := fmt.Sprintf("%s/%s", s.prefix, taskID)
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete position from etcd: %w", err)
	}

	return nil
}

// Close closes the storage.
func (s *EtcdStorage) Close() error {
	close(s.stopCh)
	s.wg.Wait()
	return s.client.Close()
}

// loadAll loads all offsets from etcd.
func (s *EtcdStorage) loadAll(ctx context.Context) error {
	resp, err := s.client.Get(ctx, s.prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get offsets from etcd: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, kv := range resp.Kvs {
		var pos event.Position
		if err := pos.UnmarshalBinary(kv.Value); err != nil {
			log.Warn("failed to unmarshal position",
				zap.String("key", string(kv.Key)),
				zap.Error(err))
			continue
		}

		// Extract task ID from key
		key := string(kv.Key)
		taskID := key[len(s.prefix)+1:]
		s.offset[taskID] = &pos
	}

	return nil
}

// autoFlush periodically flushes offsets to etcd.
func (s *EtcdStorage) autoFlush() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := s.flushAll(context.Background()); err != nil {
				log.Error("failed to flush offsets to etcd", zap.Error(err))
			}
		}
	}
}

// flushAll flushes all offsets to etcd.
func (s *EtcdStorage) flushAll(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for taskID, pos := range s.offset {
		data, err := pos.MarshalBinary()
		if err != nil {
			log.Error("failed to marshal position",
				zap.String("taskId", taskID),
				zap.Error(err))
			continue
		}

		key := fmt.Sprintf("%s/%s", s.prefix, taskID)
		if _, err := s.client.Put(ctx, key, string(data)); err != nil {
			log.Error("failed to save position to etcd",
				zap.String("taskId", taskID),
				zap.Error(err))
		}
	}

	return nil
}

// NewStorage creates a new storage based on the backend type.
func NewStorage(cfg *Config) (Storage, error) {
	switch cfg.Backend {
	case "memory", "":
		return NewMemoryStorage(), nil
	case "file":
		flushInterval := time.Duration(cfg.FlushInterval) * time.Millisecond
		if flushInterval == 0 {
			flushInterval = 1 * time.Second
		}
		return NewFileStorage(cfg.Path, flushInterval)
	case "etcd":
		flushInterval := time.Duration(cfg.FlushInterval) * time.Millisecond
		if flushInterval == 0 {
			flushInterval = 1 * time.Second
		}
		return NewEtcdStorage(cfg.EtcdEndpoints, "", flushInterval)
	default:
		return nil, fmt.Errorf("unsupported backend: %s", cfg.Backend)
	}
}
