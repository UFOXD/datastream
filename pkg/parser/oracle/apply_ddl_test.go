package oracle

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

func TestApplyDDL_CreateTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "CREATE TABLE users (id NUMBER PRIMARY KEY, name VARCHAR2(100), age NUMBER(3))"
	result, err := p.ApplyDDL(ctx, nil, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.Type != parser.DDLTypeCreateTable {
		t.Errorf("Expected DDLTypeCreateTable, got %s", result.Type)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil for CREATE TABLE")
	}
}

func TestApplyDDL_AlterTable_AddColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "NUMBER", Nullable: false},
			{Name: "name", Type: "VARCHAR2", Length: 100, Nullable: true},
		},
	}

	ddl := "ALTER TABLE users ADD (email VARCHAR2(255))"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.Type != parser.DDLTypeAlterTable {
		t.Errorf("Expected DDLTypeAlterTable, got %s", result.Type)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil for ALTER TABLE")
	}

	// Should have 3 columns now: id, name, email
	if len(result.NewTableInfo.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.NewTableInfo.Columns))
	}
	found := false
	for _, col := range result.NewTableInfo.Columns {
		if col.Name == "email" {
			found = true
			if col.Type != "VARCHAR2" {
				t.Errorf("Expected email type VARCHAR2, got %s", col.Type)
			}
			if col.Length != 255 {
				t.Errorf("Expected email length 255, got %d", col.Length)
			}
		}
	}
	if !found {
		t.Error("email column not found after ADD")
	}
}

func TestApplyDDL_AlterTable_DropColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "NUMBER", Nullable: false},
			{Name: "name", Type: "VARCHAR2", Length: 100, Nullable: true},
			{Name: "email", Type: "VARCHAR2", Length: 255, Nullable: true},
		},
	}

	ddl := "ALTER TABLE users DROP (email)"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	// Should have 2 columns: id, name
	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.NewTableInfo.Columns))
	}
	for _, col := range result.NewTableInfo.Columns {
		if col.Name == "email" {
			t.Error("email column should have been dropped")
		}
	}
}

func TestApplyDDL_AlterTable_ModifyColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "NUMBER", Nullable: false},
			{Name: "name", Type: "VARCHAR2", Length: 100, Nullable: true},
		},
	}

	ddl := "ALTER TABLE users MODIFY (name VARCHAR2(200))"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	// name column should now be VARCHAR2(200)
	for _, col := range result.NewTableInfo.Columns {
		if col.Name == "name" {
			if col.Type != "VARCHAR2" {
				t.Errorf("Expected name type VARCHAR2, got %s", col.Type)
			}
			if col.Length != 200 {
				t.Errorf("Expected name length 200, got %d", col.Length)
			}
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
			{Name: "id", Type: "NUMBER", Nullable: false},
			{Name: "name", Type: "VARCHAR2", Length: 100, Nullable: true},
		},
	}

	ddl := "ALTER TABLE users RENAME COLUMN name TO full_name"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}

	foundOld := false
	foundNew := false
	for _, col := range result.NewTableInfo.Columns {
		if col.Name == "name" {
			foundOld = true
		}
		if col.Name == "full_name" {
			foundNew = true
			if col.Type != "VARCHAR2" {
				t.Errorf("Expected full_name type VARCHAR2, got %s", col.Type)
			}
			if col.Length != 100 {
				t.Errorf("Expected full_name length 100, got %d", col.Length)
			}
		}
	}
	if foundOld {
		t.Error("old column 'name' should have been renamed")
	}
	if !foundNew {
		t.Error("new column 'full_name' not found")
	}
}

func TestApplyDDL_DropTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "NUMBER", Nullable: false},
		},
	}

	ddl := "DROP TABLE users"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.Type != parser.DDLTypeDropTable {
		t.Errorf("Expected DDLTypeDropTable, got %s", result.Type)
	}
	if result.NewTableInfo != nil {
		t.Error("NewTableInfo should be nil for DROP TABLE")
	}
}

func TestApplyDDL_AlterTable_WithSchema(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "MYSCHEMA",
		Table:    "USERS",
		Columns: []event.ColumnInfo{
			{Name: "ID", Type: "NUMBER", Nullable: false},
		},
	}

	ddl := "ALTER TABLE MYSCHEMA.USERS ADD (EMAIL VARCHAR2(255))"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}
	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.NewTableInfo.Columns))
	}
}

func TestApplyDDL_DropTable_NilOldTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "DROP TABLE users"
	result, err := p.ApplyDDL(ctx, nil, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.NewTableInfo != nil {
		t.Error("NewTableInfo should be nil for DROP TABLE")
	}
}

func TestApplyDDL_AlterTable_NilOldTable_Error(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "ALTER TABLE users ADD (email VARCHAR2(255))"
	_, err := p.ApplyDDL(ctx, nil, ddl)
	if err == nil {
		t.Error("Expected error for ALTER TABLE with nil oldTable")
	}
}
