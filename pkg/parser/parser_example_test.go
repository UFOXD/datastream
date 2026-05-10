package parser_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/mysql"
	"github.com/UFOXD/datastream/pkg/parser/oracle"
	"github.com/UFOXD/datastream/pkg/parser/postgres"
	"github.com/UFOXD/datastream/pkg/parser/sqlserver"
)

func TestParseResultFormat(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name    string
		parser  parser.DDLParser
		ddl     string
	}{
		{
			name:   "MySQL CREATE TABLE",
			parser: mysql.NewParser(),
			ddl:    "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, email VARCHAR(255))",
		},
		{
			name:   "MySQL ALTER TABLE ADD COLUMN",
			parser: mysql.NewParser(),
			ddl:    "ALTER TABLE users ADD COLUMN age INT DEFAULT 0",
		},
		{
			name:   "MySQL ALTER TABLE DROP COLUMN",
			parser: mysql.NewParser(),
			ddl:    "ALTER TABLE users DROP COLUMN age",
		},
		{
			name:   "MySQL DROP INDEX",
			parser: mysql.NewParser(),
			ddl:    "DROP INDEX idx_email ON users",
		},
		{
			name:   "MySQL CREATE INDEX",
			parser: mysql.NewParser(),
			ddl:    "CREATE INDEX idx_name ON users (name)",
		},
		{
			name:   "MySQL DROP TABLE",
			parser: mysql.NewParser(),
			ddl:    "DROP TABLE IF EXISTS users",
		},
		{
			name:   "PostgreSQL CREATE TABLE",
			parser: postgres.NewParser(),
			ddl:    "CREATE TABLE public.users (id SERIAL PRIMARY KEY, name VARCHAR(100))",
		},
		{
			name:   "PostgreSQL DROP INDEX",
			parser: postgres.NewParser(),
			ddl:    "DROP INDEX IF EXISTS public.idx_email",
		},
		{
			name:   "Oracle CREATE TABLE",
			parser: oracle.NewParser(),
			ddl:    "CREATE TABLE SCHEMA.USERS (ID NUMBER PRIMARY KEY, NAME VARCHAR2(100))",
		},
		{
			name:   "Oracle DROP INDEX",
			parser: oracle.NewParser(),
			ddl:    "DROP INDEX SCHEMA.IDX_EMAIL",
		},
		{
			name:   "SQL Server CREATE TABLE",
			parser: sqlserver.NewParser(),
			ddl:    "CREATE TABLE [dbo].[users] (id INT PRIMARY KEY, name NVARCHAR(100))",
		},
		{
			name:   "SQL Server DROP INDEX",
			parser: sqlserver.NewParser(),
			ddl:    "DROP INDEX IF EXISTS idx_email ON users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := tc.parser.Parse(ctx, tc.ddl)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if len(results) == 0 {
				t.Fatal("Expected at least one result")
			}
			result := results[0]

			// Pretty print JSON
			jsonBytes, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				t.Fatalf("JSON marshal error: %v", err)
			}

			fmt.Printf("\n=== %s ===\n", tc.name)
			fmt.Printf("DDL: %s\n", tc.ddl)
			fmt.Printf("Result:\n%s\n", string(jsonBytes))
		})
	}
}

func TestDropIndexOutput(t *testing.T) {
	ctx := context.Background()

	// Focus on DROP INDEX for MySQL
	p := mysql.NewParser()

	ddls := []string{
		"DROP INDEX idx_email ON users",
		"DROP INDEX idx_name ON mydb.users",
	}

	for _, ddl := range ddls {
		results, err := p.Parse(ctx, ddl)
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		if len(results) == 0 {
			t.Fatal("Expected at least one result")
		}
		result := results[0]

		fmt.Printf("\n=== MySQL DROP INDEX ===\n")
		fmt.Printf("DDL: %s\n", ddl)
		fmt.Printf("Type: %s\n", result.Type)
		fmt.Printf("Table: %s\n", result.Table)
		fmt.Printf("Database: %s\n", result.Database)
		if result.IndexChanges != nil {
			fmt.Printf("IndexName: %s\n", result.IndexChanges.IndexName)
			fmt.Printf("Operation: %s\n", result.IndexChanges.Operation)
			fmt.Printf("TableName: %s\n", result.IndexChanges.TableName)
			fmt.Printf("DatabaseName: %s\n", result.IndexChanges.DatabaseName)
		}

		// Full JSON output
		jsonBytes, _ := json.MarshalIndent(result, "", "  ")
		fmt.Printf("Full JSON:\n%s\n", string(jsonBytes))
	}
}
