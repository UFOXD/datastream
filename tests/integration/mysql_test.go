// +build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func TestMySQLConnection(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fixture.WaitForMySQL(ctx); err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}

	t.Log("MySQL connection successful")
}

func TestMySQLTableOperations(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fixture.WaitForMySQL(ctx); err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}

	// Create test table
	schema := `
		CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) UNIQUE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)
	`

	if err := fixture.SetupMySQLTable("users", schema); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	if err := fixture.InsertMySQL("users", "name, email", "'test-user', 'test@example.com'"); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	t.Log("MySQL table operations successful")
}
