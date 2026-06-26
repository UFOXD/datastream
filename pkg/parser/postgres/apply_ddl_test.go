package postgres

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

func TestApplyDDL_CreateTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := `CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		email TEXT,
		age INTEGER
	)`

	result, err := p.ApplyDDL(ctx, nil, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.Type != parser.DDLTypeCreateTable {
		t.Errorf("Expected type %s, got %s", parser.DDLTypeCreateTable, result.Type)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil for CREATE TABLE")
	}

	if result.NewTableInfo.Table != "users" {
		t.Errorf("Expected table name 'users', got %q", result.NewTableInfo.Table)
	}

	if len(result.NewTableInfo.Columns) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(result.NewTableInfo.Columns))
	}

	// Verify column details
	// event.ColumnInfo.Type stores base type, Length stores params
	tests := []struct {
		name     string
		typ      string
		length   int
		nullable bool
	}{
		{"id", "SERIAL", 0, false},       // PRIMARY KEY implies NOT NULL
		{"name", "VARCHAR", 100, false},   // explicit NOT NULL
		{"email", "TEXT", 0, true},
		{"age", "INTEGER", 0, true},
	}

	for i, tt := range tests {
		col := result.NewTableInfo.Columns[i]
		if col.Name != tt.name {
			t.Errorf("column %d: expected name %q, got %q", i, tt.name, col.Name)
		}
		if col.Type != tt.typ {
			t.Errorf("column %d (%s): expected type %q, got %q", i, tt.name, tt.typ, col.Type)
		}
		if col.Length != tt.length {
			t.Errorf("column %d (%s): expected length %d, got %d", i, tt.name, tt.length, col.Length)
		}
		if col.Nullable != tt.nullable {
			t.Errorf("column %d (%s): expected nullable %v, got %v", i, tt.name, tt.nullable, col.Nullable)
		}
	}
}

func TestApplyDDL_AlterTable_AddColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "name", Type: "VARCHAR(100)", Nullable: true},
		},
	}

	ddl := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.Type != parser.DDLTypeAlterTable {
		t.Errorf("Expected type %s, got %s", parser.DDLTypeAlterTable, result.Type)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil for ALTER TABLE")
	}

	if len(result.NewTableInfo.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.NewTableInfo.Columns))
	}

	added := result.NewTableInfo.Columns[2]
	if added.Name != "email" {
		t.Errorf("Expected column name 'email', got %q", added.Name)
	}
	if added.Type != "VARCHAR" {
		t.Errorf("Expected column type 'VARCHAR', got %q", added.Type)
	}
	if added.Length != 255 {
		t.Errorf("Expected column length 255, got %d", added.Length)
	}
	if !added.Nullable {
		t.Error("Expected column to be nullable by default")
	}
}

func TestApplyDDL_AlterTable_DropColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "name", Type: "VARCHAR(100)", Nullable: true},
			{Name: "email", Type: "TEXT", Nullable: true},
		},
	}

	ddl := "ALTER TABLE users DROP COLUMN email"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("Expected 2 columns after drop, got %d", len(result.NewTableInfo.Columns))
	}

	for _, col := range result.NewTableInfo.Columns {
		if col.Name == "email" {
			t.Error("Column 'email' should have been dropped")
		}
	}
}

func TestApplyDDL_AlterTable_RenameColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "name", Type: "VARCHAR(100)", Nullable: true},
		},
	}

	ddl := "ALTER TABLE users RENAME COLUMN name TO username"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.Type != parser.DDLTypeAlterTable {
		t.Errorf("Expected type %s, got %s", parser.DDLTypeAlterTable, result.Type)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.NewTableInfo.Columns))
	}

	renamed := result.NewTableInfo.Columns[1]
	if renamed.Name != "username" {
		t.Errorf("Expected column name 'username', got %q", renamed.Name)
	}
	// Type should be preserved
	if renamed.Type != "VARCHAR(100)" {
		t.Errorf("Expected column type 'VARCHAR(100)' preserved after rename, got %q", renamed.Type)
	}
}

func TestApplyDDL_DropTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
		},
	}

	ddl := "DROP TABLE users"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.Type != parser.DDLTypeDropTable {
		t.Errorf("Expected type %s, got %s", parser.DDLTypeDropTable, result.Type)
	}

	if result.NewTableInfo != nil {
		t.Error("NewTableInfo should be nil for DROP TABLE")
	}
}

func TestApplyDDL_AlterTable_SetNotNull(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "email", Type: "TEXT", Nullable: true},
		},
	}

	ddl := "ALTER TABLE users ALTER COLUMN email SET NOT NULL"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	emailCol := result.NewTableInfo.Columns[1]
	if emailCol.Nullable {
		t.Error("Expected email column to be NOT NULL after SET NOT NULL")
	}
}

func TestApplyDDL_AlterTable_DropNotNull(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "email", Type: "TEXT", Nullable: false},
		},
	}

	ddl := "ALTER TABLE users ALTER COLUMN email DROP NOT NULL"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	emailCol := result.NewTableInfo.Columns[1]
	if !emailCol.Nullable {
		t.Error("Expected email column to be nullable after DROP NOT NULL")
	}
}

func TestApplyDDL_AlterTable_AlterColumnType(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "age", Type: "SMALLINT", Nullable: true},
		},
	}

	ddl := "ALTER TABLE users ALTER COLUMN age TYPE BIGINT"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	ageCol := result.NewTableInfo.Columns[1]
	if ageCol.Type != "BIGINT" {
		t.Errorf("Expected column type 'BIGINT', got %q", ageCol.Type)
	}
}

func TestApplyDDL_NilOldTable_AlterTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "ALTER TABLE users ADD COLUMN email TEXT"
	_, err := p.ApplyDDL(ctx, nil, ddl)
	if err == nil {
		t.Error("Expected error when oldTable is nil for ALTER TABLE")
	}
}

func TestApplyDDL_CreateTable_WithSchema(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "CREATE TABLE public.users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"

	result, err := p.ApplyDDL(ctx, nil, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL error: %v", err)
	}

	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	if result.NewTableInfo.Database != "public" {
		t.Errorf("Expected database 'public', got %q", result.NewTableInfo.Database)
	}
	if result.NewTableInfo.Table != "users" {
		t.Errorf("Expected table 'users', got %q", result.NewTableInfo.Table)
	}

	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.NewTableInfo.Columns))
	}

	if result.NewTableInfo.Columns[0].Nullable {
		t.Error("id column with PRIMARY KEY should not be nullable")
	}
	if result.NewTableInfo.Columns[1].Nullable {
		t.Error("name column with NOT NULL should not be nullable")
	}
}

func TestParseTypeString(t *testing.T) {
	tests := []struct {
		input      string
		baseType   string
		length     int
		scale      int
	}{
		{"VARCHAR(255)", "VARCHAR", 255, 0},
		{"NUMERIC(10,2)", "NUMERIC", 10, 2},
		{"INTEGER", "INTEGER", 0, 0},
		{"", "", 0, 0},
		{"TEXT", "TEXT", 0, 0},
		{"DECIMAL(18,6)", "DECIMAL", 18, 6},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			baseType, length, scale := parseTypeString(tt.input)
			if baseType != tt.baseType {
				t.Errorf("baseType: expected %q, got %q", tt.baseType, baseType)
			}
			if length != tt.length {
				t.Errorf("length: expected %d, got %d", tt.length, length)
			}
			if scale != tt.scale {
				t.Errorf("scale: expected %d, got %d", tt.scale, scale)
			}
		})
	}
}
