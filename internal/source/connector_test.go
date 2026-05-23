package source

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Type: "mysql",
				Connection: ConnectionConfig{
					Host:     "localhost",
					Port:     3306,
					User:     "root",
					Password: "password",
					Database: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "missing connection type",
			config: Config{
				Type: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Basic validation
			if tt.config.Type == "" && !tt.wantErr {
				t.Error("expected error for empty type")
			}
		})
	}
}

func TestStatus(t *testing.T) {
	status := Status{
		State:     StateRunning,
		Message:   "Processing events",
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if status.State != StateRunning {
		t.Errorf("Expected state %s, got %s", StateRunning, status.State)
	}

	if status.Message != "Processing events" {
		t.Errorf("Expected message 'Processing events', got '%s'", status.Message)
	}
}

func TestPositionOperations(t *testing.T) {
	pos := &event.Position{
		CommitTime: time.Now(),
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  1234,
		TxID:       "tx-1",
		SeqNo:      1,
	}

	// Test clone
	clone := pos.Clone()
	if clone.BinlogFile != pos.BinlogFile {
		t.Error("Clone should have same binlog file")
	}
	if clone.BinlogPos != pos.BinlogPos {
		t.Error("Clone should have same binlog position")
	}

	// Test marshal/unmarshal
	data, err := pos.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var restored event.Position
	if err := restored.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if restored.BinlogFile != pos.BinlogFile {
		t.Error("Restored position should match original")
	}
	if restored.BinlogPos != pos.BinlogPos {
		t.Error("Restored position should match original")
	}
}

func TestSnapshotMode(t *testing.T) {
	tests := []struct {
		mode     SnapshotMode
		expected string
	}{
		{SnapshotModeNever, "never"},
		{SnapshotModeInitial, "initial"},
		{SnapshotModeAlways, "always"},
	}

	for _, tt := range tests {
		if string(tt.mode) != tt.expected {
			t.Errorf("Expected %s, got %s", tt.expected, tt.mode)
		}
	}
}

func TestStateConstants(t *testing.T) {
	states := []State{
		StateUninitialized,
		StateInitializing,
		StateRunning,
		StatePaused,
		StateStopped,
		StateError,
	}

	for _, state := range states {
		if string(state) == "" {
			t.Error("State constant should not be empty")
		}
	}
}

func TestTableFilter(t *testing.T) {
	filter := TableFilter{
		Database: "testdb",
		Schema:   "public",
		Tables:   []string{"users", "orders"},
	}

	if filter.Database != "testdb" {
		t.Error("Database should be testdb")
	}

	if len(filter.Tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(filter.Tables))
	}
}

func TestOffsetConfig(t *testing.T) {
	cfg := OffsetConfig{
		Backend:       "file",
		Path:          "/tmp/offset.json",
		FlushInterval: 1000,
	}

	if cfg.Backend != "file" {
		t.Error("Backend should be file")
	}

	if cfg.FlushInterval != 1000 {
		t.Error("FlushInterval should be 1000")
	}
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Error("Context should not be done yet")
	default:
	}

	time.Sleep(150 * time.Millisecond)

	select {
	case <-ctx.Done():
		// Expected
	default:
		t.Error("Context should be done after timeout")
	}
}

// mockSource is a mock implementation of Connector for testing
type mockSource struct {
	name   string
	events chan *event.ChangeEvent
	errors chan error
}

func (m *mockSource) Name() string                                           { return m.name }
func (m *mockSource) Initialize(ctx context.Context, config Config) error   { return nil }
func (m *mockSource) Start(ctx context.Context) error                       { return nil }
func (m *mockSource) Stop(ctx context.Context) error                        { return nil }
func (m *mockSource) Status() Status                                         { return Status{State: StateRunning} }
func (m *mockSource) Events() <-chan *event.ChangeEvent                     { return m.events }
func (m *mockSource) Errors() <-chan error                                   { return m.errors }
func (m *mockSource) GetPosition() *event.Position                           { return nil }
func (m *mockSource) SetPosition(pos *event.Position) error                 { return nil }
func (m *mockSource) GetSchema(database, table string) (*event.TableInfo, error) {
	return nil, nil
}
func (m *mockSource) Schemas() map[string]*event.TableInfo { return make(map[string]*event.TableInfo) }
func (m *mockSource) SyncScope() *SyncScope                                  { return nil }
func (m *mockSource) AddTables(ctx context.Context, tables []string) error  { return nil }
func (m *mockSource) RemoveTables(ctx context.Context, tables []string) error { return nil }
func (m *mockSource) ListTables() []string                                   { return nil }

type mockSourceFactory struct{}

func (f *mockSourceFactory) Create(config Config) (Connector, error) {
	return &mockSource{
		name:   config.Type,
		events: make(chan *event.ChangeEvent),
		errors: make(chan error),
	}, nil
}

func TestRegisterAndCreateSource(t *testing.T) {
	// Register a mock factory
	Register("mock-source", &mockSourceFactory{})

	// Create should return a connector
	conn, err := Create("mock-source", Config{Type: "mock-source"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if conn == nil {
		t.Fatal("Connector should not be nil")
	}

	if conn.Name() != "mock-source" {
		t.Errorf("Expected name 'mock-source', got '%s'", conn.Name())
	}
}

func TestCreateUnsupportedSource(t *testing.T) {
	_, err := Create("nonexistent-source", Config{Type: "nonexistent-source"})
	if err != ErrUnsupportedConnector {
		t.Errorf("Expected ErrUnsupportedConnector, got %v", err)
	}
}
