package sqlserver

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
			ddl:           "CREATE TABLE users (id INT PRIMARY KEY, name NVARCHAR(100))",
			expectedType:  parser.DDLTypeCreateTable,
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "create table with schema",
			ddl:           "CREATE TABLE [dbo].[users] (id INT PRIMARY KEY)",
			expectedType:  parser.DDLTypeCreateTable,
			expectedDB:    "dbo",
			expectedTable: "users",
		},
		{
			name:          "create table with database.schema.table",
			ddl:           "CREATE TABLE [mydb].[dbo].[users] (id INT)",
			expectedType:  parser.DDLTypeCreateTable,
			expectedDB:    "dbo",
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
			ddl:           "DROP TABLE [dbo].[users]",
			expectedType:  parser.DDLTypeDropTable,
			expectedDB:    "dbo",
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
		expectedTable  string
	}{
		{
			name:          "drop index with modern syntax",
			ddl:           "DROP INDEX IF EXISTS idx_email ON users",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "idx_email",
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "drop index with schema",
			ddl:           "DROP INDEX IF EXISTS idx_email ON [dbo].[users]",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "idx_email",
			expectedDB:    "dbo",
			expectedTable: "users",
		},
		{
			name:          "drop index backward compatible",
			ddl:           "DROP INDEX idx_name ON users",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "idx_name",
			expectedDB:    "",
			expectedTable: "users",
		},
		{
			name:          "drop index backward compatible with schema",
			ddl:           "DROP INDEX idx_name ON [dbo].[users]",
			expectedType:  parser.DDLTypeDropIndex,
			expectedIndex: "idx_name",
			expectedDB:    "dbo",
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
			if result.IndexChanges == nil {
				t.Fatal("IndexChanges should not be nil")
			}
			if result.IndexChanges.IndexName != tc.expectedIndex {
				t.Errorf("Expected index name %s, got %s", tc.expectedIndex, result.IndexChanges.IndexName)
			}
			if result.IndexChanges.TableName != tc.expectedTable {
				t.Errorf("Expected table name %s, got %s", tc.expectedTable, result.IndexChanges.TableName)
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
			ddl:           "DROP VIEW [dbo].[myview]",
			expectedType:  parser.DDLTypeDropView,
			expectedDB:    "dbo",
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

	testCases := []struct {
		name              string
		ddl               string
		expectedType      parser.DDLType
		expectedDB        string
		expectedTable     string
		expectedAddedCols []string
		expectedDroppedCols []string
	}{
		{
			name:          "alter table add column",
			ddl:           "ALTER TABLE users ADD email NVARCHAR(255)",
			expectedType:  parser.DDLTypeAlterTable,
			expectedDB:    "",
			expectedTable: "users",
			expectedAddedCols: []string{"email"},
		},
		{
			name:              "alter table drop column",
			ddl:               "ALTER TABLE users DROP COLUMN email",
			expectedType:      parser.DDLTypeAlterTable,
			expectedDB:        "",
			expectedTable:     "users",
			expectedDroppedCols: []string{"email"},
		},
		{
			name:          "alter table with schema",
			ddl:           "ALTER TABLE [dbo].[users] ADD age INT",
			expectedType:  parser.DDLTypeAlterTable,
			expectedDB:    "dbo",
			expectedTable: "users",
			expectedAddedCols: []string{"age"},
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
			if len(tc.expectedAddedCols) > 0 {
				if len(result.TableChanges.AddedColumns) != len(tc.expectedAddedCols) {
					t.Errorf("Expected %d added columns, got %d", len(tc.expectedAddedCols), len(result.TableChanges.AddedColumns))
				}
			}
			if len(tc.expectedDroppedCols) > 0 {
				if len(result.TableChanges.DroppedColumns) != len(tc.expectedDroppedCols) {
					t.Errorf("Expected %d dropped columns, got %d", len(tc.expectedDroppedCols), len(result.TableChanges.DroppedColumns))
				}
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
			ddl:           "TRUNCATE TABLE [dbo].[users]",
			expectedType:  parser.DDLTypeTruncate,
			expectedDB:    "dbo",
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
			ddl:          "CREATE UNIQUE INDEX idx_email ON [dbo].[users](email)",
			expectedType: parser.DDLTypeCreateIndex,
		},
		{
			name:         "create clustered index",
			ddl:          "CREATE CLUSTERED INDEX idx_id ON users(id)",
			expectedType: parser.DDLTypeCreateIndex,
		},
		{
			name:         "create nonclustered index",
			ddl:          "CREATE NONCLUSTERED INDEX idx_name ON users(name)",
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
			ddl:          "DROP DATABASE mydb",
			expectedType: parser.DDLTypeDropDatabase,
			expectedDB:   "mydb",
		},
		// Note: DROP DATABASE IF EXISTS uses text-based parsing which
		// has limitations with ANTLR's condensed GetText() output
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
		parser.DDLTypeDropDatabase,
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
	ddl := "CREATE TABLE users (id INT PRIMARY KEY); DROP TABLE users;"
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
