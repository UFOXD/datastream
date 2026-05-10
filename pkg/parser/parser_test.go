package parser

import (
	"context"
	"testing"
)

func TestDDLTypeConstants(t *testing.T) {
	types := []DDLType{
		DDLTypeCreateDatabase,
		DDLTypeDropDatabase,
		DDLTypeAlterDatabase,
		DDLTypeCreateTable,
		DDLTypeDropTable,
		DDLTypeAlterTable,
		DDLTypeRenameTable,
		DDLTypeTruncate,
		DDLTypeCreateIndex,
		DDLTypeDropIndex,
		DDLTypeCreateView,
		DDLTypeDropView,
		DDLTypeUnknown,
	}

	for _, dt := range types {
		if string(dt) == "" {
			t.Error("DDLType constant should not be empty")
		}
	}
}

func TestTableOperationConstants(t *testing.T) {
	ops := []TableOperation{
		TableOpCreate,
		TableOpAlter,
		TableOpDrop,
		TableOpRename,
	}

	for _, op := range ops {
		if string(op) == "" {
			t.Error("TableOperation constant should not be empty")
		}
	}
}

func TestDDLResult(t *testing.T) {
	result := &DDLResult{
		Type:      DDLTypeCreateTable,
		Database:  "testdb",
		Table:     "users",
		Statement: "CREATE TABLE users (id INT PRIMARY KEY)",
		TableChanges: &TableChanges{
			Operation: TableOpCreate,
			Table: &TableInfo{
				Database: "testdb",
				Name:     "users",
				Columns: []ColumnInfo{
					{Name: "id", Type: "INT", Nullable: false},
				},
			},
		},
	}

	if result.Type != DDLTypeCreateTable {
		t.Errorf("Expected DDLTypeCreateTable, got %s", result.Type)
	}
	if result.Database != "testdb" {
		t.Errorf("Expected testdb, got %s", result.Database)
	}
	if result.Table != "users" {
		t.Errorf("Expected users, got %s", result.Table)
	}
	if result.TableChanges == nil {
		t.Error("TableChanges should not be nil")
	}
}

func TestTableChanges(t *testing.T) {
	changes := &TableChanges{
		Operation: TableOpAlter,
		AddedColumns: []ColumnInfo{
			{Name: "email", Type: "VARCHAR(255)", Nullable: true},
		},
		DroppedColumns: []string{"old_column"},
		ModifiedColumns: []ColumnModification{
			{
				Old: ColumnInfo{Name: "name", Type: "VARCHAR(100)"},
				New: ColumnInfo{Name: "name", Type: "VARCHAR(255)"},
			},
		},
	}

	if len(changes.AddedColumns) != 1 {
		t.Errorf("Expected 1 added column, got %d", len(changes.AddedColumns))
	}
	if len(changes.DroppedColumns) != 1 {
		t.Errorf("Expected 1 dropped column, got %d", len(changes.DroppedColumns))
	}
	if len(changes.ModifiedColumns) != 1 {
		t.Errorf("Expected 1 modified column, got %d", len(changes.ModifiedColumns))
	}
}

func TestColumnInfo(t *testing.T) {
	col := ColumnInfo{
		Name:          "id",
		Type:          "INT",
		Nullable:      false,
		DefaultValue:  "0",
		Comment:       "Primary key",
		AutoIncrement: true,
	}

	if col.Name != "id" {
		t.Errorf("Expected id, got %s", col.Name)
	}
	if col.Nullable {
		t.Error("Column should not be nullable")
	}
	if !col.AutoIncrement {
		t.Error("Column should be auto increment")
	}
}

func TestIndexInfo(t *testing.T) {
	idx := IndexInfo{
		Name:    "idx_email",
		Type:    "BTREE",
		Columns: []string{"email"},
		Unique:  true,
	}

	if idx.Name != "idx_email" {
		t.Errorf("Expected idx_email, got %s", idx.Name)
	}
	if !idx.Unique {
		t.Error("Index should be unique")
	}
}

func TestIndexChanges(t *testing.T) {
	changes := &IndexChanges{
		IndexName:    "idx_name",
		TableName:    "users",
		DatabaseName: "testdb",
		Operation:    "create",
		Index: &IndexInfo{
			Name:    "idx_name",
			Columns: []string{"name"},
		},
	}

	if changes.Operation != "create" {
		t.Errorf("Expected create, got %s", changes.Operation)
	}
}

func TestPrimaryKeyChange(t *testing.T) {
	pkChange := &PrimaryKeyChange{
		OldColumns: []string{"id"},
		NewColumns: []string{"id", "tenant_id"},
	}

	if len(pkChange.OldColumns) != 1 {
		t.Errorf("Expected 1 old column, got %d", len(pkChange.OldColumns))
	}
	if len(pkChange.NewColumns) != 2 {
		t.Errorf("Expected 2 new columns, got %d", len(pkChange.NewColumns))
	}
}

// mockParser is a simple mock for testing
type mockParser struct {
	supportedTypes []DDLType
}

func (m *mockParser) Parse(ctx context.Context, ddl string) (*DDLResult, error) {
	return &DDLResult{
		Type:      DDLTypeUnknown,
		Statement: ddl,
	}, nil
}

func (m *mockParser) SupportedTypes() []DDLType {
	return m.supportedTypes
}

func TestDDLParserInterface(t *testing.T) {
	var _ DDLParser = &mockParser{}
}
