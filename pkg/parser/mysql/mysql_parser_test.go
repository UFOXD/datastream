package mysql

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/parser"
)

func TestNewParser(t *testing.T) {
	p := NewParser()
	if p == nil {
		t.Fatal("Parser should not be nil")
	}
	if p.visitor == nil {
		t.Fatal("Visitor should not be nil")
	}
}

func TestParseCreateDatabase(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	tests := []struct {
		name     string
		ddl      string
		expected string
	}{
		{
			name:     "simple create database",
			ddl:      "CREATE DATABASE testdb",
			expected: "testdb",
		},
		{
			name:     "create database with if not exists",
			ddl:      "CREATE DATABASE IF NOT EXISTS testdb",
			expected: "testdb",
		},
		{
			name:     "create database with backticks",
			ddl:      "CREATE DATABASE `testdb`",
			expected: "testdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.Parse(ctx, tt.ddl)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if result.Type != parser.DDLTypeCreateDatabase {
				t.Errorf("Expected DDLTypeCreateDatabase, got %s", result.Type)
			}
			if result.Database != tt.expected {
				t.Errorf("Expected database %s, got %s", tt.expected, result.Database)
			}
		})
	}
}

func TestParseDropDatabase(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "DROP DATABASE testdb")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeDropDatabase {
		t.Errorf("Expected DDLTypeDropDatabase, got %s", result.Type)
	}
	if result.Database != "testdb" {
		t.Errorf("Expected testdb, got %s", result.Database)
	}
}

func TestParseCreateTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	tests := []struct {
		name      string
		ddl       string
		db        string
		table     string
	}{
		{
			name:  "simple create table",
			ddl:   "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
			db:    "",
			table: "users",
		},
		{
			name:  "create table with database",
			ddl:   "CREATE TABLE testdb.users (id INT)",
			db:    "testdb",
			table: "users",
		},
		{
			name:  "create table if not exists",
			ddl:   "CREATE TABLE IF NOT EXISTS users (id INT NOT NULL AUTO_INCREMENT)",
			db:    "",
			table: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.Parse(ctx, tt.ddl)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if result.Type != parser.DDLTypeCreateTable {
				t.Errorf("Expected DDLTypeCreateTable, got %s", result.Type)
			}
			if result.Database != tt.db {
				t.Errorf("Expected database %s, got %s", tt.db, result.Database)
			}
			if result.Table != tt.table {
				t.Errorf("Expected table %s, got %s", tt.table, result.Table)
			}
			if result.TableChanges == nil {
				t.Fatal("TableChanges should not be nil for CREATE TABLE")
			}
		})
	}
}

func TestParseDropTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "DROP TABLE testdb.users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeDropTable {
		t.Errorf("Expected DDLTypeDropTable, got %s", result.Type)
	}
	if result.Database != "testdb" {
		t.Errorf("Expected testdb, got %s", result.Database)
	}
	if result.Table != "users" {
		t.Errorf("Expected users, got %s", result.Table)
	}
}

func TestParseAlterTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	tests := []struct {
		name         string
		ddl          string
		expectAdd    int
		expectDrop   int
		expectModify int
	}{
		{
			name:      "add column",
			ddl:       "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			expectAdd: 1,
		},
		{
			name:       "drop column",
			ddl:        "ALTER TABLE users DROP COLUMN old_field",
			expectDrop: 1,
		},
		{
			name:         "modify column",
			ddl:          "ALTER TABLE users MODIFY COLUMN name VARCHAR(255)",
			expectModify: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.Parse(ctx, tt.ddl)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if result.Type != parser.DDLTypeAlterTable {
				t.Errorf("Expected DDLTypeAlterTable, got %s", result.Type)
			}
			if result.TableChanges == nil {
				t.Fatal("TableChanges should not be nil")
			}
			if len(result.TableChanges.AddedColumns) != tt.expectAdd {
				t.Errorf("Expected %d added columns, got %d", tt.expectAdd, len(result.TableChanges.AddedColumns))
			}
			if len(result.TableChanges.DroppedColumns) != tt.expectDrop {
				t.Errorf("Expected %d dropped columns, got %d", tt.expectDrop, len(result.TableChanges.DroppedColumns))
			}
			if len(result.TableChanges.ModifiedColumns) != tt.expectModify {
				t.Errorf("Expected %d modified columns, got %d", tt.expectModify, len(result.TableChanges.ModifiedColumns))
			}
		})
	}
}

func TestParseCreateIndex(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "CREATE INDEX idx_name ON users (name)")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeCreateIndex {
		t.Errorf("Expected DDLTypeCreateIndex, got %s", result.Type)
	}
	if result.IndexChanges == nil {
		t.Fatal("IndexChanges should not be nil")
	}
	if result.IndexChanges.IndexName != "idx_name" {
		t.Errorf("Expected idx_name, got %s", result.IndexChanges.IndexName)
	}
	if result.Table != "users" {
		t.Errorf("Expected users, got %s", result.Table)
	}
}

func TestParseDropIndex(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "DROP INDEX idx_name ON users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeDropIndex {
		t.Errorf("Expected DDLTypeDropIndex, got %s", result.Type)
	}
	if result.IndexChanges == nil {
		t.Fatal("IndexChanges should not be nil")
	}
	if result.IndexChanges.IndexName != "idx_name" {
		t.Errorf("Expected idx_name, got %s", result.IndexChanges.IndexName)
	}
}

func TestParseCreateView(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "CREATE VIEW user_view AS SELECT * FROM users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeCreateView {
		t.Errorf("Expected DDLTypeCreateView, got %s", result.Type)
	}
	if result.Table != "user_view" {
		t.Errorf("Expected user_view, got %s", result.Table)
	}
}

func TestParseDropView(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "DROP VIEW user_view")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeDropView {
		t.Errorf("Expected DDLTypeDropView, got %s", result.Type)
	}
	if result.Table != "user_view" {
		t.Errorf("Expected user_view, got %s", result.Table)
	}
}

func TestParseTruncate(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "TRUNCATE TABLE users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeTruncate {
		t.Errorf("Expected DDLTypeTruncate, got %s", result.Type)
	}
	if result.Table != "users" {
		t.Errorf("Expected users, got %s", result.Table)
	}
}

func TestParseUnknown(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	result, err := p.Parse(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if result.Type != parser.DDLTypeUnknown {
		t.Errorf("Expected DDLTypeUnknown, got %s", result.Type)
	}
}

func TestSupportedTypes(t *testing.T) {
	p := NewParser()
	types := p.SupportedTypes()

	expectedCount := 12
	if len(types) != expectedCount {
		t.Errorf("Expected %d types, got %d", expectedCount, len(types))
	}
}

func TestParserImplementsInterface(t *testing.T) {
	var _ parser.DDLParser = NewParser()
}
