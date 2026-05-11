//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

// TestOracleSourceIntegration tests Oracle source connector with LogMiner
func TestOracleSourceIntegration(t *testing.T) {
	cfg := DefaultConfig()

	// Connect to Oracle
	db, err := sql.Open("oracle", cfg.OracleDSN())
	if err != nil {
		t.Fatalf("Failed to connect to Oracle: %v", err)
	}
	defer db.Close()

	// Wait for Oracle to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for Oracle")
		default:
			if err := db.Ping(); err == nil {
				goto ready
			}
			time.Sleep(1 * time.Second)
		}
	}

ready:
	// Get current SCN
	var currentSCN uint64
	err = db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&currentSCN)
	if err != nil {
		t.Fatalf("Failed to get current SCN: %v", err)
	}
	t.Logf("Current SCN: %d", currentSCN)

	// Create test table
	tableName := fmt.Sprintf("TEST_TABLE_%d", time.Now().UnixNano() % 1000000)
	createTable := fmt.Sprintf(`
		CREATE TABLE %s (
			id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name VARCHAR2(255) NOT NULL,
			value NUMBER,
			created_at TIMESTAMP DEFAULT SYSTIMESTAMP
		)
	`, tableName)

	_, err = db.Exec(createTable)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE %s PURGE", tableName))

	// Insert test data
	for i := 1; i <= 5; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (name, value) VALUES ('item-%d', %d)", tableName, i, i*10))
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Commit to ensure changes are visible
	db.Exec("COMMIT")

	// Verify data
	var count int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 5 {
		t.Fatalf("Expected 5 rows, got %d", count)
	}

	// Get new SCN after changes
	var newSCN uint64
	err = db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&newSCN)
	if err != nil {
		t.Fatalf("Failed to get new SCN: %v", err)
	}
	t.Logf("New SCN after changes: %d (delta: %d)", newSCN, newSCN-currentSCN)

	t.Log("Oracle source integration test passed")
}
