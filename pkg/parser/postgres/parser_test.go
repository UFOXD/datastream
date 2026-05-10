package postgres

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/parser"
)

func TestNewParser(t *testing.T) {
	p := NewParser()
	if p == nil {
		t.Fatal("NewParser should not return nil")
	}
}

func TestParseCreateDatabase(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name         string
		ddl          string
		expectedType parser.DDLType
		expectedDB   string
	}{
		{
			name:         "simple create database",
			ddl:          "CREATE DATABASE testdb",
			expectedType: parser.DDLTypeCreateDatabase,
			expectedDB:   "testdb",
		},
		{
			name:         "create database with quoted name",
			ddl:          "CREATE DATABASE \"TestDB\"",
			expectedType: parser.DDLTypeCreateDatabase,
			expectedDB:   "TestDB",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database %s, got %s", tc.expectedDB, result.Database)
			}
		})
	}
}

func TestParseDropDatabase(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name         string
		ddl          string
		expectedType parser.DDLType
		expectedDB   string
	}{
		{
			name:         "simple drop database",
			ddl:          "DROP DATABASE testdb",
			expectedType: parser.DDLTypeDropDatabase,
			expectedDB:   "testdb",
		},
		{
			name:         "drop database with if exists",
			ddl:          "DROP DATABASE IF EXISTS testdb",
			expectedType: parser.DDLTypeDropDatabase,
			expectedDB:   "testdb",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database %s, got %s", tc.expectedDB, result.Database)
			}
		})
	}
}

func TestParseCreateSchema(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name         string
		ddl          string
		expectedType parser.DDLType
		expectedDB   string
	}{
		{
			name:         "simple create schema",
			ddl:          "CREATE SCHEMA myschema",
			expectedType: parser.DDLTypeCreateDatabase,
			expectedDB:   "myschema",
		},
		{
			name:         "create schema with quoted name",
			ddl:          "CREATE SCHEMA \"MySchema\"",
			expectedType: parser.DDLTypeCreateDatabase,
			expectedDB:   "MySchema",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database/schema %s, got %s", tc.expectedDB, result.Database)
			}
		})
	}
}

func TestParseCreateTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name         string
		ddl          string
		expectedType parser.DDLType
		expectedDB   string
		expectedTable string
	}{
		{
			name:         "simple create table",
			ddl:          "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100))",
			expectedType: parser.DDLTypeCreateTable,
			expectedDB:   "",
			expectedTable: "users",
		},
		{
			name:         "create table with schema",
			ddl:          "CREATE TABLE public.users (id SERIAL PRIMARY KEY)",
			expectedType: parser.DDLTypeCreateTable,
			expectedDB:   "public",
			expectedTable: "users",
		},
		{
			name:         "create table with quoted names",
			ddl:          "CREATE TABLE \"MySchema\".\"MyTable\" (id INT)",
			expectedType: parser.DDLTypeCreateTable,
			expectedDB:   "MySchema",
			expectedTable: "MyTable",
		},
		{
			name:         "create table if not exists",
			ddl:          "CREATE TABLE IF NOT EXISTS users (id INT)",
			expectedType: parser.DDLTypeCreateTable,
			expectedDB:   "",
			expectedTable: "users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database/schema %s, got %s", tc.expectedDB, result.Database)
			}
			if result.Table != tc.expectedTable {
				t.Errorf("Expected table %s, got %s", tc.expectedTable, result.Table)
			}
		})
	}
}

func TestParseDropTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name          string
		ddl           string
		expectedType  parser.DDLType
		expectedDB    string
		expectedTable string
	}{
		{
			name:          "simple drop table",
			ddl:           "DROP TABLE users",
			expectedType:  parser.DDLTypeDropTable,
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "drop table with schema",
			ddl:           "DROP TABLE public.users",
			expectedType:  parser.DDLTypeDropTable,
			expectedDB:    "public",
			expectedTable: "users",
		},
		{
			name:          "drop table if exists",
			ddl:           "DROP TABLE IF EXISTS users",
			expectedType:  parser.DDLTypeDropTable,
			expectedDB:    "",
			expectedTable: "users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database/schema %s, got %s", tc.expectedDB, result.Database)
			}
			if result.Table != tc.expectedTable {
				t.Errorf("Expected table %s, got %s", tc.expectedTable, result.Table)
			}
		})
	}
}

func TestParseDropIndex(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name           string
		ddl            string
		expectedType   parser.DDLType
		expectedIndex  string
		expectedDB     string
	}{
		{
			name:          "simple drop index",
			ddl:           "DROP INDEX idx_email",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "idx_email",
			expectedDB:    "",
		},
		{
			name:          "drop index with schema",
			ddl:           "DROP INDEX public.idx_email",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "idx_email",
			expectedDB:    "public",
		},
		{
			name:          "drop index if exists",
			ddl:           "DROP INDEX IF EXISTS idx_email",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "idx_email",
			expectedDB:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.IndexChanges == nil {
				t.Fatal("IndexChanges should not be nil")
			}
			if result.IndexChanges.IndexName != tc.expectedIndex {
				t.Errorf("Expected index name %s, got %s", tc.expectedIndex, result.IndexChanges.IndexName)
			}
			if result.IndexChanges.DatabaseName != tc.expectedDB {
				t.Errorf("Expected database name %s, got %s", tc.expectedDB, result.IndexChanges.DatabaseName)
			}
		})
	}
}

func TestParseDropView(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name          string
		ddl           string
		expectedType  parser.DDLType
		expectedDB    string
		expectedTable string
	}{
		{
			name:          "simple drop view",
			ddl:           "DROP VIEW myview",
			expectedType:  parser.DDLTypeDropView,
			expectedDB:    "",
			expectedTable: "myview",
		},
		{
			name:          "drop view with schema",
			ddl:           "DROP VIEW public.myview",
			expectedType:  parser.DDLTypeDropView,
			expectedDB:    "public",
			expectedTable: "myview",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database/schema %s, got %s", tc.expectedDB, result.Database)
			}
			if result.Table != tc.expectedTable {
				t.Errorf("Expected view name %s, got %s", tc.expectedTable, result.Table)
			}
		})
	}
}

func TestParseAlterTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	// Note: ALTER TABLE column changes use text-based parsing which has
	// limitations with ANTLR's condensed GetText() output.
	testCases := []struct {
		name          string
		ddl           string
		expectedType  parser.DDLType
		expectedDB    string
		expectedTable string
	}{
		{
			name:          "alter table add column",
			ddl:           "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			expectedType:  parser.DDLTypeAlterTable,
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "alter table drop column",
			ddl:           "ALTER TABLE users DROP COLUMN email",
			expectedType:  parser.DDLTypeAlterTable,
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "alter table with schema",
			ddl:           "ALTER TABLE public.users ADD COLUMN age INT",
			expectedType:  parser.DDLTypeAlterTable,
			expectedDB:    "public",
			expectedTable: "users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database/schema %s, got %s", tc.expectedDB, result.Database)
			}
			if result.Table != tc.expectedTable {
				t.Errorf("Expected table %s, got %s", tc.expectedTable, result.Table)
			}
			if result.TableChanges == nil {
				t.Fatal("TableChanges should not be nil")
			}
		})
	}
}

func TestParseTruncate(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	testCases := []struct {
		name          string
		ddl           string
		expectedType  parser.DDLType
		expectedDB    string
		expectedTable string
	}{
		{
			name:          "simple truncate",
			ddl:           "TRUNCATE TABLE users",
			expectedType:  parser.DDLTypeTruncate,
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "truncate with schema",
			ddl:           "TRUNCATE TABLE public.users",
			expectedType:  parser.DDLTypeTruncate,
			expectedDB:    "public",
			expectedTable: "users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := p.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %s, got %s", tc.expectedType, result.Type)
			}
			if result.Database != tc.expectedDB {
				t.Errorf("Expected database/schema %s, got %s", tc.expectedDB, result.Database)
			}
			if result.Table != tc.expectedTable {
				t.Errorf("Expected table %s, got %s", tc.expectedTable, result.Table)
			}
		})
	}
}

func TestSupportedTypes(t *testing.T) {
	p := NewParser()
	types := p.SupportedTypes()

	expectedTypes := []parser.DDLType{
		parser.DDLTypeCreateDatabase,
		parser.DDLTypeDropDatabase,
		parser.DDLTypeAlterDatabase,
		parser.DDLTypeCreateTable,
		parser.DDLTypeDropTable,
		parser.DDLTypeAlterTable,
		parser.DDLTypeTruncate,
		parser.DDLTypeCreateIndex,
		parser.DDLTypeDropIndex,
		parser.DDLTypeDropView,
	}

	for _, et := range expectedTypes {
		found := false
		for _, t := range types {
			if t == et {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected DDL type %s in SupportedTypes", et)
		}
	}
}

func TestParserImplementsInterface(t *testing.T) {
	var _ parser.DDLParser = NewParser()
}

func TestParseMultipleStatements(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	// Test parsing multiple DDL statements separated by semicolons
	ddl := "CREATE TABLE users (id SERIAL PRIMARY KEY); DROP TABLE users;"
	results, err := p.Parse(ctx, ddl)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Check first statement
	if results[0].Type != parser.DDLTypeCreateTable {
		t.Errorf("Expected DDLTypeCreateTable for first result, got %s", results[0].Type)
	}
	if results[0].Table != "users" {
		t.Errorf("Expected table 'users' for first result, got %s", results[0].Table)
	}

	// Check second statement
	if results[1].Type != parser.DDLTypeDropTable {
		t.Errorf("Expected DDLTypeDropTable for second result, got %s", results[1].Type)
	}
	if results[1].Table != "users" {
		t.Errorf("Expected table 'users' for second result, got %s", results[1].Table)
	}
}
