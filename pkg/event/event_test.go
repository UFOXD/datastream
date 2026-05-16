package event

import (
	"testing"
	"time"
)

func TestGenerateEventID(t *testing.T) {
	source := &SourceInfo{
		Connector: "mysql",
		Database:  "testdb",
	}
	ts := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	id := GenerateEventID(source, ts, 1)

	expected := "mysql:testdb:20260507120000.000:1"
	if id != expected {
		t.Errorf("Expected ID '%s', got '%s'", expected, id)
	}
}

func TestChangeEventIsSnapshot(t *testing.T) {
	event := &ChangeEvent{
		Source: SourceInfo{Snapshot: true},
	}

	if !event.IsSnapshot() {
		t.Error("Expected IsSnapshot() to return true")
	}

	event.Source.Snapshot = false
	if event.IsSnapshot() {
		t.Error("Expected IsSnapshot() to return false")
	}
}

func TestChangeEventIsDDL(t *testing.T) {
	event := &ChangeEvent{Type: EventTypeDDL}
	if !event.IsDDL() {
		t.Error("Expected IsDDL() to return true")
	}

	event.Type = EventTypeInsert
	if event.IsDDL() {
		t.Error("Expected IsDDL() to return false")
	}
}

func TestPositionCompare(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		p1       *Position
		p2       *Position
		expected int
	}{
		{
			name: "p1 before p2 by time",
			p1: &Position{
				CommitTime: now,
				SeqNo:      1,
			},
			p2: &Position{
				CommitTime: now.Add(time.Second),
				SeqNo:      1,
			},
			expected: -1,
		},
		{
			name: "p1 after p2 by time",
			p1: &Position{
				CommitTime: now.Add(time.Second),
				SeqNo:      1,
			},
			p2: &Position{
				CommitTime: now,
				SeqNo:      1,
			},
			expected: 1,
		},
		{
			name: "equal positions",
			p1: &Position{
				CommitTime: now,
				SeqNo:      1,
			},
			p2: &Position{
				CommitTime: now,
				SeqNo:      1,
			},
			expected: 0,
		},
		{
			name: "same time, different seqno",
			p1: &Position{
				CommitTime: now,
				SeqNo:      1,
			},
			p2: &Position{
				CommitTime: now,
				SeqNo:      2,
			},
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.p1.Compare(tt.p2)
			if err != nil {
				t.Fatalf("Compare failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestPositionMarshalBinary(t *testing.T) {
	now := time.Now()
	p := &Position{
		CommitTime: now,
		TxID:       "tx-123",
		SeqNo:      5,
		Total:      10,
	}

	data, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var p2 Position
	if err := p2.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if p2.TxID != p.TxID {
		t.Errorf("Expected TxID '%s', got '%s'", p.TxID, p2.TxID)
	}
	if p2.SeqNo != p.SeqNo {
		t.Errorf("Expected SeqNo %d, got %d", p.SeqNo, p2.SeqNo)
	}
}

func TestTableInfoHasKeyColumns(t *testing.T) {
	table := &TableInfo{
		Database:          "testdb",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
	}

	if !table.HasPrimaryKey() {
		t.Error("Expected HasPrimaryKey() to return true")
	}

	if len(table.GetKeyColumns()) != 1 || table.GetKeyColumns()[0] != "id" {
		t.Error("Expected GetKeyColumns() to return ['id']")
	}

	// Table without primary key but with unique key
	table2 := &TableInfo{
		UniqueKeyColumns: []string{"email"},
	}

	if table2.HasPrimaryKey() {
		t.Error("Expected HasPrimaryKey() to return false")
	}

	if !table2.HasUniqueKey() {
		t.Error("Expected HasUniqueKey() to return true")
	}
}

func TestRowDataOperations(t *testing.T) {
	row := NewRowData()
	row.Set("id", 1, "int")
	row.Set("name", "test", "varchar")
	row.SetNull("deleted_at", "timestamp")

	// Test Get
	val, ok := row.Get("id")
	if !ok || val != 1 {
		t.Errorf("Expected Get('id') to return 1, got %v", val)
	}

	// Test GetField
	field, ok := row.GetField("name")
	if !ok || field.Value != "test" {
		t.Errorf("Expected GetField('name') to return 'test'")
	}

	// Test NULL field
	nullField, ok := row.GetField("deleted_at")
	if !ok || !nullField.Null {
		t.Error("Expected 'deleted_at' to be NULL")
	}
}

func TestHeartbeatEvent(t *testing.T) {
	source := SourceInfo{
		Connector: "mysql",
		Database:  "testdb",
	}
	pos := Position{
		CommitTime: time.Now(),
		TxID:       "tx-1",
	}

	hb := NewHeartbeat(source, pos)
	event := hb.ToChangeEvent()

	if event.Type != EventTypeHeartbeat {
		t.Errorf("Expected type %s, got %s", EventTypeHeartbeat, event.Type)
	}
}

func TestChangeEvent_Size_Nil(t *testing.T) {
	var e *ChangeEvent
	if got := e.Size(); got != 0 {
		t.Errorf("nil ChangeEvent size = %d, want 0", got)
	}
}

func TestChangeEvent_Size_Empty(t *testing.T) {
	e := &ChangeEvent{}
	if got := e.Size(); got <= 0 {
		t.Errorf("empty ChangeEvent size = %d, want > 0 (fixed overhead)", got)
	}
}

func TestChangeEvent_Size_WithFields(t *testing.T) {
	e := &ChangeEvent{
		Source: SourceInfo{Database: "db1"},
		Table:  TableInfo{Database: "db1", Table: "users"},
		After: RowData{Fields: map[string]Field{
			"id":   {Name: "id", Value: int64(42)},
			"name": {Name: "name", Value: "alice"},
		}},
	}
	got := e.Size()
	want := 3 + 3 + 5 + 64 + 2 + 16 + 4 + 5
	if got < want {
		t.Errorf("Size() = %d, want >= %d", got, want)
	}
}

func TestChangeEvent_Size_StringAndBytes(t *testing.T) {
	e := &ChangeEvent{
		After: RowData{Fields: map[string]Field{
			"s": {Name: "s", Value: "hello"},
			"b": {Name: "b", Value: []byte("world!!")},
		}},
	}
	got := e.Size()
	if got < 64+1+5+1+7 {
		t.Errorf("Size() = %d, want larger", got)
	}
}
