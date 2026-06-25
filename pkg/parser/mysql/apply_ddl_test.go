package mysql

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

func TestApplyDDL_CreateTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "CREATE TABLE users (id INT NOT NULL AUTO_INCREMENT, name VARCHAR(100) NOT NULL, email VARCHAR(255) DEFAULT 'none', age INT)"
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
	if result.NewTableInfo.Database != "" {
		t.Errorf("Expected empty database, got %s", result.NewTableInfo.Database)
	}
	if result.NewTableInfo.Table != "users" {
		t.Errorf("Expected table 'users', got %s", result.NewTableInfo.Table)
	}
	if len(result.NewTableInfo.Columns) < 3 {
		t.Fatalf("Expected at least 3 columns, got %d", len(result.NewTableInfo.Columns))
	}

	// Verify column details
	col0 := result.NewTableInfo.Columns[0]
	if col0.Name != "id" {
		t.Errorf("Expected column name 'id', got %s", col0.Name)
	}
	if col0.Nullable {
		t.Error("Expected id column to be NOT NULL")
	}

	col1 := result.NewTableInfo.Columns[1]
	if col1.Name != "name" {
		t.Errorf("Expected column name 'name', got %s", col1.Name)
	}
	if col1.Type != "VARCHAR" {
		t.Errorf("Expected type 'VARCHAR', got %s", col1.Type)
	}
	if col1.Length != 100 {
		t.Errorf("Expected length 100, got %d", col1.Length)
	}
	if col1.Nullable {
		t.Error("Expected name column to be NOT NULL")
	}
}

func TestApplyDDL_AlterTable_AddColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true, Length: 100},
		},
	}

	ddl := "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}
	if len(result.NewTableInfo.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.NewTableInfo.Columns))
	}

	newCol := result.NewTableInfo.Columns[2]
	if newCol.Name != "email" {
		t.Errorf("Expected column name 'email', got %s", newCol.Name)
	}
	if newCol.Type != "VARCHAR" {
		t.Errorf("Expected type 'VARCHAR', got %s", newCol.Type)
	}
	if newCol.Length != 255 {
		t.Errorf("Expected length 255, got %d", newCol.Length)
	}
	if newCol.Nullable {
		t.Error("Expected email column to be NOT NULL")
	}

	// Original table should not be modified
	if len(oldTable.Columns) != 2 {
		t.Error("Original table should not be modified")
	}
}

func TestApplyDDL_AlterTable_DropColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true, Length: 100},
			{Name: "email", Type: "VARCHAR", Nullable: true, Length: 255},
		},
	}

	ddl := "ALTER TABLE users DROP COLUMN email"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
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
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true, Length: 100},
		},
	}

	ddl := "ALTER TABLE users MODIFY COLUMN name VARCHAR(255) NOT NULL"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.NewTableInfo.Columns))
	}

	modCol := result.NewTableInfo.Columns[1]
	if modCol.Name != "name" {
		t.Errorf("Expected column name 'name', got %s", modCol.Name)
	}
	if modCol.Type != "VARCHAR" {
		t.Errorf("Expected type 'VARCHAR', got %s", modCol.Type)
	}
	if modCol.Length != 255 {
		t.Errorf("Expected length 255, got %d", modCol.Length)
	}
	if modCol.Nullable {
		t.Error("Expected name column to be NOT NULL after MODIFY")
	}
}

func TestApplyDDL_AlterTable_ChangeColumn(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true, Length: 100},
		},
	}

	ddl := "ALTER TABLE users CHANGE COLUMN name full_name VARCHAR(200) NOT NULL"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if len(result.NewTableInfo.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(result.NewTableInfo.Columns))
	}

	changedCol := result.NewTableInfo.Columns[1]
	if changedCol.Name != "full_name" {
		t.Errorf("Expected column name 'full_name', got %s", changedCol.Name)
	}
	if changedCol.Type != "VARCHAR" {
		t.Errorf("Expected type 'VARCHAR', got %s", changedCol.Type)
	}
	if changedCol.Length != 200 {
		t.Errorf("Expected length 200, got %d", changedCol.Length)
	}
	if changedCol.Nullable {
		t.Error("Expected full_name column to be NOT NULL")
	}
}

func TestApplyDDL_DropTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
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

func TestApplyDDL_AlterTable_NilOldTable(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
	_, err := p.ApplyDDL(ctx, nil, ddl)
	if err == nil {
		t.Error("Expected error when oldTable is nil for ALTER TABLE")
	}
}

func TestApplyDDL_CreateTable_WithDatabase(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "CREATE TABLE testdb.users (id INT PRIMARY KEY, name VARCHAR(100))"
	result, err := p.ApplyDDL(ctx, nil, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if result.NewTableInfo == nil {
		t.Fatal("NewTableInfo should not be nil")
	}
	if result.NewTableInfo.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got %s", result.NewTableInfo.Database)
	}
	if result.NewTableInfo.Table != "users" {
		t.Errorf("Expected table 'users', got %s", result.NewTableInfo.Table)
	}
}

func TestApplyDDL_AlterTable_MultipleOperations(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	oldTable := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true, Length: 100},
			{Name: "old_col", Type: "INT", Nullable: true},
		},
	}

	ddl := "ALTER TABLE users DROP COLUMN old_col, ADD COLUMN email VARCHAR(255)"
	result, err := p.ApplyDDL(ctx, oldTable, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}
	if len(result.NewTableInfo.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.NewTableInfo.Columns))
	}

	// old_col should be gone
	for _, col := range result.NewTableInfo.Columns {
		if col.Name == "old_col" {
			t.Error("old_col should have been dropped")
		}
	}

	// email should be added
	found := false
	for _, col := range result.NewTableInfo.Columns {
		if col.Name == "email" {
			found = true
			break
		}
	}
	if !found {
		t.Error("email column should have been added")
	}
}

func TestApplyDDL_DecimalType(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	ddl := "CREATE TABLE accounts (id INT, balance DECIMAL(10,2))"
	result, err := p.ApplyDDL(ctx, nil, ddl)
	if err != nil {
		t.Fatalf("ApplyDDL failed: %v", err)
	}

	var balanceCol *event.ColumnInfo
	for i, col := range result.NewTableInfo.Columns {
		if col.Name == "balance" {
			balanceCol = &result.NewTableInfo.Columns[i]
			break
		}
	}
	if balanceCol == nil {
		t.Fatal("balance column not found")
	}
	if balanceCol.Type != "DECIMAL" {
		t.Errorf("Expected type 'DECIMAL', got %s", balanceCol.Type)
	}
	if balanceCol.Length != 10 {
		t.Errorf("Expected length 10, got %d", balanceCol.Length)
	}
	if balanceCol.Scale != 2 {
		t.Errorf("Expected scale 2, got %d", balanceCol.Scale)
	}
}
