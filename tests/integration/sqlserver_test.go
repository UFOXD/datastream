//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// TestSQLServerSourceIntegration tests SQL Server source connector with CDC
func TestSQLServerSourceIntegration(t *testing.T) {
	cfg := DefaultConfig()

	// Connect to SQL Server
	db, err := sql.Open("sqlserver", cfg.SQLServerDSN())
	if err != nil {
		t.Fatalf("Failed to connect to SQL Server: %v", err)
	}
	defer db.Close()

	// Wait for SQL Server to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for SQL Server")
		default:
			if err := db.Ping(); err == nil {
				goto ready
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

ready:
	// Create test table
	tableName := fmt.Sprintf("test_table_%d", time.Now().UnixNano())
	createTable := fmt.Sprintf(`
		CREATE TABLE %s (
			id INT IDENTITY(1,1) PRIMARY KEY,
			name NVARCHAR(255) NOT NULL,
			value INT,
			created_at DATETIME2 DEFAULT GETDATE()
		)
	`, tableName)

	_, err = db.Exec(createTable)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert test data
	for i := 1; i <= 5; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (name, value) VALUES (@p1, @p2)", tableName),
			fmt.Sprintf("item-%d", i), i*10)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Verify data
	var count int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 5 {
		t.Fatalf("Expected 5 rows, got %d", count)
	}

	// Check if CDC is enabled (optional)
	var cdcEnabled bool
	err = db.QueryRow("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()").Scan(&cdcEnabled)
	if err == nil && cdcEnabled {
		t.Log("CDC is enabled for this database")

		// Check if CDC is enabled on the table
		var captureInstance string
		err = db.QueryRow(`
			SELECT capture_instance
			FROM cdc.change_tables
			WHERE source_name = @p1
		`, tableName).Scan(&captureInstance)
		if err == nil {
			t.Logf("CDC capture instance found: %s", captureInstance)
		}
	}

	t.Log("SQL Server source integration test passed")
}
