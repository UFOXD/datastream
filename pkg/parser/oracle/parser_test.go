package oracle

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

func TestParseCreateTable(t *testing.T) {
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
			name:          "simple create table",
			ddl:           "CREATE TABLE users (id NUMBER PRIMARY KEY, name VARCHAR2(100))",
			expectedType:  parser.DDLTypeCreateTable,
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "create table with schema",
			ddl:           "CREATE TABLE SCHEMA.USERS (ID NUMBER PRIMARY KEY, NAME VARCHAR2(100))",
			expectedType:  parser.DDLTypeCreateTable,
			expectedDB:    "SCHEMA",
			expectedTable: "USERS",
		},
		{
			name:          "create table with quoted names",
			ddl:           "CREATE TABLE \"MySchema\".\"MyTable\" (id NUMBER)",
			expectedType:  parser.DDLTypeCreateTable,
			expectedDB:    "MySchema",
			expectedTable: "MyTable",
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
			ddl:           "DROP TABLE SCHEMA.USERS",
			expectedType:  parser.DDLTypeDropTable,
			expectedDB:    "SCHEMA",
			expectedTable: "USERS",
		},
		{
			name:          "drop table with quoted name",
			ddl:           "DROP TABLE \"MySchema\".\"MyTable\"",
			expectedType:  parser.DDLTypeDropTable,
			expectedDB:    "MySchema",
			expectedTable: "MyTable",
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
		name          string
		ddl           string
		expectedType  parser.DDLType
		expectedIndex string
		expectedDB    string
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
			ddl:           "DROP INDEX SCHEMA.IDX_EMAIL",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "IDX_EMAIL",
			expectedDB:    "SCHEMA",
		},
		{
			name:          "drop index with quoted name",
			ddl:           "DROP INDEX \"MySchema\".\"IDX_EMAIL\"",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "IDX_EMAIL",
			expectedDB:    "MySchema",
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
			ddl:           "DROP VIEW SCHEMA.MYVIEW",
			expectedType:  parser.DDLTypeDropView,
			expectedDB:    "SCHEMA",
			expectedTable: "MYVIEW",
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

	// Note: ALTER TABLE uses text-based parsing for column changes which has
	// limitations with ANTLR's condensed GetText() output. Basic detection works.
	testCases := []struct {
		name          string
		ddl           string
		expectedType  parser.DDLType
		expectedDB    string
		expectedTable string
	}{
		{
			name:          "alter table add column",
			ddl:           "ALTER TABLE users ADD email VARCHAR2(255)",
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
			ddl:           "ALTER TABLE SCHEMA.USERS ADD age NUMBER",
			expectedType:  parser.DDLTypeAlterTable,
			expectedDB:    "SCHEMA",
			expectedTable: "USERS",
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
			ddl:           "TRUNCATE TABLE SCHEMA.USERS",
			expectedType:  parser.DDLTypeTruncate,
			expectedDB:    "SCHEMA",
			expectedTable: "USERS",
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

func TestParseCreateIndex(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	// Note: CREATE INDEX uses text-based parsing which has limitations
	// with ANTLR's condensed GetText() output. Basic type detection works.
	testCases := []struct {
		name         string
		ddl          string
		expectedType parser.DDLType
	}{
		{
			name:         "simple create index",
			ddl:          "CREATE INDEX idx_email ON users(email)",
			expectedType: parser.DDLTypeCreateIndex,
		},
		{
			name:         "create unique index",
			ddl:          "CREATE UNIQUE INDEX idx_email ON SCHEMA.USERS(email)",
			expectedType: parser.DDLTypeCreateIndex,
		},
		{
			name:         "create bitmap index",
			ddl:          "CREATE BITMAP INDEX idx_status ON users(status)",
			expectedType: parser.DDLTypeCreateIndex,
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
		})
	}
}

func TestSupportedTypes(t *testing.T) {
	p := NewParser()
	types := p.SupportedTypes()

	expectedTypes := []parser.DDLType{
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
	ddl := "CREATE TABLE users (id NUMBER PRIMARY KEY); DROP TABLE users;"
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
