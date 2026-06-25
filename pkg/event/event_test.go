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

func TestPositionComparePostgres(t *testing.T) {
	tests := []struct {
		name     string
		p1       *Position
		p2       *Position
		expected int
	}{
		{
			name:     "p1 LSN less than p2",
			p1:       &Position{LSN: 100},
			p2:       &Position{LSN: 200},
			expected: -1,
		},
		{
			name:     "p1 LSN greater than p2",
			p1:       &Position{LSN: 300},
			p2:       &Position{LSN: 200},
			expected: 1,
		},
		{
			name:     "equal LSN",
			p1:       &Position{LSN: 200},
			p2:       &Position{LSN: 200},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.p1.Compare(tt.p2, SourceTypePostgres)
			if err != nil {
				t.Fatalf("Compare failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestPositionCompareOracle(t *testing.T) {
	tests := []struct {
		name     string
		p1       *Position
		p2       *Position
		expected int
	}{
		{name: "less", p1: &Position{SCN: 100}, p2: &Position{SCN: 200}, expected: -1},
		{name: "greater", p1: &Position{SCN: 300}, p2: &Position{SCN: 200}, expected: 1},
		{name: "equal", p1: &Position{SCN: 200}, p2: &Position{SCN: 200}, expected: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.p1.Compare(tt.p2, SourceTypeOracle)
			if err != nil {
				t.Fatalf("Compare failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestPositionCompareSQLServer(t *testing.T) {
	tests := []struct {
		name     string
		p1       *Position
		p2       *Position
		expected int
	}{
		{
			name:     "different ChangeLsn",
			p1:       &Position{ChangeLsn: "0x00000025:000001D8:0001", SeqVal: "0x01"},
			p2:       &Position{ChangeLsn: "0x00000025:000001D9:0001", SeqVal: "0x01"},
			expected: -1,
		},
		{
			name:     "same ChangeLsn, different SeqVal",
			p1:       &Position{ChangeLsn: "0x00000025:000001D8:0001", SeqVal: "0x01"},
			p2:       &Position{ChangeLsn: "0x00000025:000001D8:0001", SeqVal: "0x02"},
			expected: -1,
		},
		{
			name:     "equal",
			p1:       &Position{ChangeLsn: "0x00000025:000001D8:0001", SeqVal: "0x01"},
			p2:       &Position{ChangeLsn: "0x00000025:000001D8:0001", SeqVal: "0x01"},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.p1.Compare(tt.p2, SourceTypeSQLServer)
			if err != nil {
				t.Fatalf("Compare failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestPositionCompareMySQLFilePos(t *testing.T) {
	tests := []struct {
		name     string
		p1       *Position
		p2       *Position
		expected int
	}{
		{
			name:     "different binlog file",
			p1:       &Position{BinlogFile: "mysql-bin.000001", BinlogPos: 100},
			p2:       &Position{BinlogFile: "mysql-bin.000002", BinlogPos: 100},
			expected: -1,
		},
		{
			name:     "same file, different pos",
			p1:       &Position{BinlogFile: "mysql-bin.000001", BinlogPos: 100},
			p2:       &Position{BinlogFile: "mysql-bin.000001", BinlogPos: 200},
			expected: -1,
		},
		{
			name:     "equal",
			p1:       &Position{BinlogFile: "mysql-bin.000001", BinlogPos: 100},
			p2:       &Position{BinlogFile: "mysql-bin.000001", BinlogPos: 100},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.p1.Compare(tt.p2, SourceTypeMySQLFilePos)
			if err != nil {
				t.Fatalf("Compare failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestPositionCompareUnsupportedSources(t *testing.T) {
	p := &Position{TxID: "uuid:1-500"}

	_, err := p.Compare(&Position{TxID: "uuid:1-500"}, SourceTypeMySQLGTID)
	if err == nil {
		t.Error("Expected error for MySQL GTID, got nil")
	}

	_, err = p.Compare(&Position{}, SourceTypeMongoDB)
	if err == nil {
		t.Error("Expected error for MongoDB, got nil")
	}

	_, err = p.Compare(&Position{}, SourceTypeUnspecified)
	if err == nil {
		t.Error("Expected error for unspecified source type, got nil")
	}
}

func TestPositionCloneNewFields(t *testing.T) {
	p := &Position{
		BinlogFile:  "mysql-bin.000001",
		BinlogPos:   100,
		LSN:         200,
		SCN:         300,
		ChangeLsn:   "0x01",
		SeqVal:      "0x02",
		ResumeToken: []byte{0x01, 0x02, 0x03},
		CommitTime:  time.Now(),
		TxID:        "tx-1",
	}

	clone := p.Clone()

	// Verify all fields copied
	if clone.SeqVal != p.SeqVal {
		t.Errorf("SeqVal: expected %s, got %s", p.SeqVal, clone.SeqVal)
	}
	if string(clone.ResumeToken) != string(p.ResumeToken) {
		t.Errorf("ResumeToken mismatch")
	}

	// Verify deep copy (modifying clone doesn't affect original)
	clone.ResumeToken[0] = 0xFF
	if p.ResumeToken[0] == 0xFF {
		t.Error("Clone is not a deep copy for ResumeToken")
	}
}

func TestPositionIsZeroNewFields(t *testing.T) {
	p := &Position{}
	if !p.IsZero() {
		t.Error("Expected zero position")
	}

	p.SeqVal = "0x01"
	if p.IsZero() {
		t.Error("Expected non-zero position with SeqVal set")
	}

	p2 := &Position{}
	p2.ResumeToken = []byte{0x01}
	if p2.IsZero() {
		t.Error("Expected non-zero position with ResumeToken set")
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
