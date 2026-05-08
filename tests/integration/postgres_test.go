// +build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func TestPostgresConnection(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fixture.WaitForPostgres(ctx); err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	t.Log("PostgreSQL connection successful")
}

func TestPostgresTableOperations(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fixture.WaitForPostgres(ctx); err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	// Create test table
	schema := `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) UNIQUE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`

	if err := fixture.SetupPostgresTable("users", schema); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	if err := fixture.InsertPostgres("users", "name, email", "'test-user', 'test@example.com'"); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	t.Log("PostgreSQL table operations successful")
}
