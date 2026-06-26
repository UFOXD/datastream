package sqlserver

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

func TestApplyDDL_CreateTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	tests := []struct {
		name          string
		ddl           string
		expectedDB    string
		expectedTable string
		expectedCols  []event.ColumnInfo
	}{
		{
			name:          "simple create table",
			ddl:           "CREATE TABLE users (id INT PRIMARY KEY, name NVARCHAR(100), email NVARCHAR(255) NULL)",
			expectedDB:    "",
			expectedTable: "users",
			expectedCols: []event.ColumnInfo{
				{Name: "id", Type: "INT"},                                          // PRIMARY KEY -> Nullable false
				{Name: "name", Type: "NVARCHAR", Length: 100, Nullable: true},  // default nullable
				{Name: "email", Type: "NVARCHAR", Length: 255, Nullable: true}, // explicit NULL
			},
		},
		{
			name:          "create table with schema",
			ddl:           "CREATE TABLE [dbo].[users] (id INT NOT NULL, age INT)",
			expectedDB:    "dbo",
			expectedTable: "users",
			expectedCols: []event.ColumnInfo{
				{Name: "id", Type: "INT"},                            // NOT NULL
				{Name: "age", Type: "INT", Nullable: true},           // default nullable
			},
		},
		{
			name:          "create table with decimal type",
			ddl:           "CREATE TABLE orders (id INT, amount DECIMAL(10,2))",
			expectedDB:    "",
			expectedTable: "orders",
			expectedCols: []event.ColumnInfo{
				{Name: "id", Type: "INT", Nullable: true},
				{Name: "amount", Type: "DECIMAL", Length: 10, Scale: 2, Nullable: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.ApplyDDL(ctx, nil, tt.ddl)
			if err != nil {
				t.Fatalf("ApplyDDL failed: %v", err)
			}
			if result.Type != parser.DDLTypeCreateTable {
				t.Errorf("expected type %s, got %s", parser.DDLTypeCreateTable, result.Type)
			}
			if result.NewTableInfo == nil {
				t.Fatal("NewTableInfo should not be nil for CREATE TABLE")
			}
			if result.NewTableInfo.Database != tt.expectedDB {
				t.Errorf("expected database %q, got %q", tt.expectedDB, result.NewTableInfo.Database)
			}
			if result.NewTableInfo.Table != tt.expectedTable {
				t.Errorf("expected table %q, got %q", tt.expectedTable, result.NewTableInfo.Table)
			}
			if len(result.NewTableInfo.Columns) != len(tt.expectedCols) {
				t.Fatalf("expected %d columns, got %d", len(tt.expectedCols), len(result.NewTableInfo.Columns))
			}
			for i, expected := range tt.expectedCols {
				actual := result.NewTableInfo.Columns[i]
				if actual.Name != expected.Name {
					t.Errorf("column[%d]: expected name %q, got %q", i, expected.Name, actual.Name)
				}
				if actual.Type != expected.Type {
					t.Errorf("column[%d]: expected type %q, got %q", i, expected.Type, actual.Type)
				}
				if expected.Length != 0 && actual.Length != expected.Length {
					t.Errorf("column[%d]: expected length %d, got %d", i, expected.Length, actual.Length)
				}
				if expected.Scale != 0 && actual.Scale != expected.Scale {
					t.Errorf("column[%d]: expected scale %d, got %d", i, expected.Scale, actual.Scale)
				}
				if actual.Nullable != expected.Nullable {
					t.Errorf("column[%d]: expected nullable %v, got %v", i, expected.Nullable, actual.Nullable)
				}
			}
		})
	}
}

func TestApplyDDL_AlterTable_AddColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "dbo",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "NVARCHAR", Length: 100},
		},
	}

	tests := []struct {
		name           string
		ddl            string
		expectedCols   []event.ColumnInfo
		expectedAdded  int
	}{
		{
			name:          "add single column",
			ddl:           "ALTER TABLE users ADD email NVARCHAR(255)",
			expectedAdded: 1,
			expectedCols: []event.ColumnInfo{
				{Name: "id", Type: "INT"},
				{Name: "name", Type: "NVARCHAR", Length: 100},
				{Name: "email", Type: "NVARCHAR", Length: 255, Nullable: true},
			},
		},
		{
			name:          "add column with schema",
			ddl:           "ALTER TABLE [dbo].[users] ADD age INT",
			expectedAdded: 1,
			expectedCols: []event.ColumnInfo{
				{Name: "id", Type: "INT"},
				{Name: "name", Type: "NVARCHAR", Length: 100},
				{Name: "age", Type: "INT", Nullable: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.ApplyDDL(ctx, oldTable, tt.ddl)
			if err != nil {
				t.Fatalf("ApplyDDL failed: %v", err)
			}
			if result.Type != parser.DDLTypeAlterTable {
				t.Errorf("expected type %s, got %s", parser.DDLTypeAlterTable, result.Type)
			}
			if result.NewTableInfo == nil {
				t.Fatal("NewTableInfo should not be nil for ALTER TABLE")
			}
			if len(result.NewTableInfo.Columns) != len(tt.expectedCols) {
				t.Fatalf("expected %d columns, got %d", len(tt.expectedCols), len(result.NewTableInfo.Columns))
			}
			for i, expected := range tt.expectedCols {
				actual := result.NewTableInfo.Columns[i]
				if actual.Name != expected.Name {
					t.Errorf("column[%d]: expected name %q, got %q", i, expected.Name, actual.Name)
				}
				if actual.Type != expected.Type {
					t.Errorf("column[%d]: expected type %q, got %q", i, expected.Type, actual.Type)
				}
			}
		})
	}
}

func TestApplyDDL_AlterTable_DropColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "dbo",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "NVARCHAR", Length: 100},
			{Name: "email", Type: "NVARCHAR", Length: 255},
		},
	}

	result, err := p.ApplyDDL(ctx, oldTable, "ALTER TABLE users DROP COLUMN email")
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.Type != parser.DDLTypeAlterTable {
		t.Errorf("expected type %s, got %s", parser.DDLTypeAlterTable, result.Type)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil for ALTER TABLE")
	}
	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("expected 2 columns after drop, got %d", len(result.NewTableInfo.Columns))
	}
	if result.NewTableInfo.Columns[0].Name != "id" {
		t.Errorf("expected first column 'id', got %q", result.NewTableInfo.Columns[0].Name)
	}
	if result.NewTableInfo.Columns[1].Name != "name" {
		t.Errorf("expected second column 'name', got %q", result.NewTableInfo.Columns[1].Name)
	}
}

func TestApplyDDL_AlterTable_AlterColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "dbo",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "NVARCHAR", Length: 100},
		},
	}

	result, err := p.ApplyDDL(ctx, oldTable, "ALTER TABLE users ALTER COLUMN name NVARCHAR(200)")
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.Type != parser.DDLTypeAlterTable {
		t.Errorf("expected type %s, got %s", parser.DDLTypeAlterTable, result.Type)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil for ALTER TABLE")
	}
	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.NewTableInfo.Columns))
	}
	nameCol := result.NewTableInfo.Columns[1]
	if nameCol.Name != "name" {
		t.Errorf("expected column name 'name', got %q", nameCol.Name)
	}
	if nameCol.Type != "NVARCHAR" {
		t.Errorf("expected type 'NVARCHAR', got %q", nameCol.Type)
	}
	if nameCol.Length != 200 {
		t.Errorf("expected length 200, got %d", nameCol.Length)
	}
}

func TestApplyDDL_DropTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "dbo",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT"},
		},
	}

	result, err := p.ApplyDDL(ctx, oldTable, "DROP TABLE users")
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.Type != parser.DDLTypeDropTable {
		t.Errorf("expected type %s, got %s", parser.DDLTypeDropTable, result.Type)
	}
	if result.NewTableInfo != nil {
		t.Error("NewTableInfo should be nil for DROP TABLE")
	}
}

func TestApplyDDL_AlterTable_NoOldTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	_, err := p.ApplyDDL(ctx, nil, "ALTER TABLE users ADD email NVARCHAR(255)")
	if err == nil {
		t.Fatal("expected error when oldTable is nil for ALTER TABLE")
	}
}
