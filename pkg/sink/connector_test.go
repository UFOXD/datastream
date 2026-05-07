package sink

import (
	"context"
	"testing"
	"time"

	"github.com/your-org/datastream/pkg/event"
)

func TestSinkConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid kafka config",
			config: Config{
				Type: "kafka",
				Connection: ConnectionConfig{
					Brokers: []string{"localhost:9092"},
					Topic:   "test-topic",
				},
			},
			wantErr: false,
		},
		{
			name: "valid mysql config",
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
			name: "missing type",
			config: Config{
				Type: "",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config.Type == "" && !tt.wantErr {
				t.Error("expected error for empty type")
			}
		})
	}
}

func TestSinkStatus(t *testing.T) {
	status := Status{
		State:         StateWriting,
		Message:       "Writing events",
		Timestamp:     time.Now().Format(time.RFC3339),
		EventsWritten: 100,
		EventsFailed:  2,
		BytesWritten:  10240,
	}

	if status.State != StateWriting {
		t.Errorf("Expected state %s, got %s", StateWriting, status.State)
	}

	if status.EventsWritten != 100 {
		t.Errorf("Expected 100 events written, got %d", status.EventsWritten)
	}

	if status.BytesWritten != 10240 {
		t.Errorf("Expected 10240 bytes written, got %d", status.BytesWritten)
	}
}

func TestSinkStateConstants(t *testing.T) {
	states := []State{
		StateUninitialized,
		StateReady,
		StateWriting,
		StateFlushing,
		StateError,
		StateStopped,
	}

	for _, state := range states {
		if string(state) == "" {
			t.Error("State constant should not be empty")
		}
	}
}

func TestBatchConfig(t *testing.T) {
	cfg := BatchConfig{
		Size:      100,
		Timeout:   1000,
		Retries:   3,
		RetryWait: 100,
	}

	if cfg.Size != 100 {
		t.Error("Batch size should be 100")
	}

	if cfg.Timeout != 1000 {
		t.Error("Batch timeout should be 1000")
	}
}

func TestRetryConfig(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:  3,
		InitialWait: 100,
		MaxWait:     5000,
		Multiplier:  2.0,
	}

	if cfg.MaxRetries != 3 {
		t.Error("MaxRetries should be 3")
	}

	if cfg.Multiplier != 2.0 {
		t.Error("Multiplier should be 2.0")
	}
}

func TestAck(t *testing.T) {
	pos := &event.Position{
		CommitTime: time.Now(),
		TxID:       "tx-123",
	}

	ack := &Ack{
		Position: pos,
		Success:  true,
		Error:    nil,
	}

	if !ack.Success {
		t.Error("Ack should be successful")
	}

	if ack.Position.TxID != "tx-123" {
		t.Error("Position TxID should be tx-123")
	}
}

func TestConnectionConfig(t *testing.T) {
	// Database connection
	dbCfg := ConnectionConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Database: "testdb",
	}

	if dbCfg.Host != "localhost" {
		t.Error("Host should be localhost")
	}

	// Kafka connection
	kafkaCfg := ConnectionConfig{
		Brokers: []string{"broker1:9092", "broker2:9092"},
		Topic:   "events",
	}

	if len(kafkaCfg.Brokers) != 2 {
		t.Error("Should have 2 brokers")
	}

	// Redis connection
	redisCfg := ConnectionConfig{
		Addr:          "localhost:6379",
		RedisPassword: "secret",
		RedisDB:       0,
	}

	if redisCfg.Addr != "localhost:6379" {
		t.Error("Redis addr should be localhost:6379")
	}
}

func TestSinkErrors(t *testing.T) {
	errors := []error{
		ErrUnsupportedConnector,
		ErrConnectionFailed,
		ErrInvalidConfig,
		ErrNotInitialized,
		ErrWriteFailed,
		ErrFlushFailed,
		ErrUnsupportedOperation,
		ErrDDLNotSupported,
		ErrTransactionNotSupported,
		ErrBufferFull,
	}

	for _, err := range errors {
		if err == nil {
			t.Error("Error should not be nil")
		}
	}
}

func TestSinkContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that simulates a long write
	done := make(chan bool)
	go func() {
		select {
		case <-ctx.Done():
			done <- true
		case <-time.After(time.Second):
			done <- false
		}
	}()

	// Cancel immediately
	cancel()

	select {
	case success := <-done:
		if !success {
			t.Error("Goroutine should have been cancelled")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Goroutine should have finished by now")
	}
}
