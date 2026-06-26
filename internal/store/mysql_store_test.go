package store

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
)

func TestSanitizeTaskID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"task-1", "task_1"},
		{"my_task", "my_task"},
		{"task@#$%", "task____"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, sanitizeTaskID(tt.input))
		})
	}
}

func TestMarshalUnmarshalPosition(t *testing.T) {
	pos := &event.Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  100,
		TxID:       "uuid:1-500",
		CommitTime: time.Now().Truncate(time.Second),
	}

	data, err := marshalJSON(pos)
	assert.NoError(t, err)

	got, err := unmarshalPosition(data)
	assert.NoError(t, err)
	assert.Equal(t, pos.BinlogFile, got.BinlogFile)
	assert.Equal(t, pos.BinlogPos, got.BinlogPos)
	assert.Equal(t, pos.TxID, got.TxID)
}

func TestMarshalUnmarshalTableInfo(t *testing.T) {
	ti := &event.TableInfo{
		Database: "testdb",
		Table:    "users",
		Columns: []event.ColumnInfo{
			{Name: "id", Type: "INT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true, Length: 255},
		},
		PrimaryKeyColumns: []string{"id"},
	}

	data, err := marshalJSON(ti)
	assert.NoError(t, err)

	got, err := unmarshalTableInfo(data)
	assert.NoError(t, err)
	assert.Equal(t, ti.Database, got.Database)
	assert.Equal(t, ti.Table, got.Table)
	assert.Len(t, got.Columns, 2)
	assert.Equal(t, "id", got.Columns[0].Name)
	assert.Equal(t, []string{"id"}, got.PrimaryKeyColumns)
}

func TestDDLStateRow_RoundTrip(t *testing.T) {
	ti := &event.TableInfo{
		Database: "mydb",
		Table:    "users",
		Columns:  []event.ColumnInfo{{Name: "id", Type: "INT"}},
	}
	rec := &DDLStateRow{
		DBName:          "mydb",
		TableName:       "users",
		DDL:             "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		LastSuccessInfo: ti,
		Status:          "failed",
		ErrorMsg:        "column already exists",
		RetryCount:      3,
	}

	data, err := marshalJSON(rec.LastSuccessInfo)
	assert.NoError(t, err)

	got, err := unmarshalTableInfo(data)
	assert.NoError(t, err)
	assert.Equal(t, "mydb", got.Database)
	assert.Equal(t, "users", got.Table)
	assert.Equal(t, "failed", rec.Status)
}
