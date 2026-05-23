package event

import (
	"bytes"
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

// ---- ChangeEvent additional tests ----

func TestChangeEvent_IsHeartbeat(t *testing.T) {
	tests := []struct {
		name     string
		typ      EventType
		expected bool
	}{
		{"heartbeat type", EventTypeHeartbeat, true},
		{"insert type", EventTypeInsert, false},
		{"ddl type", EventTypeDDL, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ChangeEvent{Type: tt.typ}
			if got := e.IsHeartbeat(); got != tt.expected {
				t.Errorf("IsHeartbeat() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestChangeEvent_IsDataEvent(t *testing.T) {
	tests := []struct {
		name     string
		typ      EventType
		expected bool
	}{
		{"insert", EventTypeInsert, true},
		{"update", EventTypeUpdate, true},
		{"delete", EventTypeDelete, true},
		{"ddl", EventTypeDDL, false},
		{"heartbeat", EventTypeHeartbeat, false},
		{"truncate", EventTypeTruncate, false},
		{"tombstone", EventTypeTombstone, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ChangeEvent{Type: tt.typ}
			if got := e.IsDataEvent(); got != tt.expected {
				t.Errorf("IsDataEvent() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestChangeEvent_Size_BeforeFields(t *testing.T) {
	e := &ChangeEvent{
		Before: RowData{Fields: map[string]Field{
			"id": {Name: "id", Value: int64(1)},
		}},
	}
	got := e.Size()
	if got < 64+2+16 {
		t.Errorf("Size() with Before fields = %d, want >= %d", got, 64+2+16)
	}
}

func TestChangeEvent_Size_NilValue(t *testing.T) {
	e := &ChangeEvent{
		After: RowData{Fields: map[string]Field{
			"x": {Name: "x", Value: nil},
		}},
	}
	got := e.Size()
	// nil value contributes 0, plus key name "x" (1 byte), plus fixed 64
	if got < 64+1 {
		t.Errorf("Size() with nil value = %d, want >= %d", got, 64+1)
	}
}

// ---- Position additional tests ----

func TestPosition_String(t *testing.T) {
	tests := []struct {
		name     string
		pos      Position
		expected string
	}{
		{
			name: "with txID",
			pos: Position{
				TxID:  "tx-abc",
				SeqNo: 3,
				Total: 10,
			},
			expected: "tx-abc:3:10",
		},
		{
			name: "without txID",
			pos: Position{
				CommitTime: time.Date(2026, 1, 15, 10, 30, 45, 0, time.UTC),
				SeqNo:      7,
			},
			expected: "20260115103045:7",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.pos.String()
			if got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestPosition_IsZero(t *testing.T) {
	tests := []struct {
		name     string
		pos      Position
		expected bool
	}{
		{"zero position", Position{}, true},
		{"has commit time", Position{CommitTime: time.Now()}, false},
		{"has binlog file", Position{BinlogFile: "mysql-bin.000001"}, false},
		{"has LSN", Position{LSN: 12345}, false},
		{"has SCN", Position{SCN: 99}, false},
		{"has timestamp", Position{Timestamp: 1000}, false},
		{"has changeLsn", Position{ChangeLsn: "0x000001"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.pos.IsZero(); got != tt.expected {
				t.Errorf("IsZero() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestPosition_Clone(t *testing.T) {
	now := time.Now()
	original := &Position{
		BinlogFile: "mysql-bin.000003",
		BinlogPos:  54321,
		LSN:        999,
		SCN:        888,
		Timestamp:  777,
		Order:      2,
		ChangeLsn:  "0xABC",
		CommitTime: now,
		TxID:       "tx-clone",
		SeqNo:      5,
		Total:      20,
	}

	cloned := original.Clone()

	// Verify all fields match
	if cloned.BinlogFile != original.BinlogFile {
		t.Errorf("BinlogFile = %q, want %q", cloned.BinlogFile, original.BinlogFile)
	}
	if cloned.BinlogPos != original.BinlogPos {
		t.Errorf("BinlogPos = %d, want %d", cloned.BinlogPos, original.BinlogPos)
	}
	if cloned.LSN != original.LSN {
		t.Errorf("LSN = %d, want %d", cloned.LSN, original.LSN)
	}
	if cloned.SCN != original.SCN {
		t.Errorf("SCN = %d, want %d", cloned.SCN, original.SCN)
	}
	if cloned.Timestamp != original.Timestamp {
		t.Errorf("Timestamp = %d, want %d", cloned.Timestamp, original.Timestamp)
	}
	if cloned.Order != original.Order {
		t.Errorf("Order = %d, want %d", cloned.Order, original.Order)
	}
	if cloned.ChangeLsn != original.ChangeLsn {
		t.Errorf("ChangeLsn = %q, want %q", cloned.ChangeLsn, original.ChangeLsn)
	}
	if !cloned.CommitTime.Equal(original.CommitTime) {
		t.Errorf("CommitTime = %v, want %v", cloned.CommitTime, original.CommitTime)
	}
	if cloned.TxID != original.TxID {
		t.Errorf("TxID = %q, want %q", cloned.TxID, original.TxID)
	}
	if cloned.SeqNo != original.SeqNo {
		t.Errorf("SeqNo = %d, want %d", cloned.SeqNo, original.SeqNo)
	}
	if cloned.Total != original.Total {
		t.Errorf("Total = %d, want %d", cloned.Total, original.Total)
	}

	// Verify it's a different pointer
	if cloned == original {
		t.Error("Clone() should return a different pointer")
	}

	// Verify mutation independence
	cloned.BinlogFile = "changed"
	if original.BinlogFile == "changed" {
		t.Error("mutating clone should not affect original")
	}
}

func TestPosition_Compare_SameTimeHigherSeqNo(t *testing.T) {
	now := time.Now()
	p1 := &Position{CommitTime: now, SeqNo: 5}
	p2 := &Position{CommitTime: now, SeqNo: 3}

	result, err := p1.Compare(p2)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}
	if result != 1 {
		t.Errorf("Expected 1 (p1 > p2), got %d", result)
	}
}

// ---- RowData additional tests ----

func TestRowData_Get_NilReceiver(t *testing.T) {
	var r *RowData
	val, ok := r.Get("anything")
	if ok || val != nil {
		t.Errorf("Get on nil RowData should return (nil, false), got (%v, %v)", val, ok)
	}
}

func TestRowData_Get_MissingField(t *testing.T) {
	r := NewRowData()
	r.Set("exists", 1, "int")

	val, ok := r.Get("missing")
	if ok || val != nil {
		t.Errorf("Get on missing field should return (nil, false), got (%v, %v)", val, ok)
	}
}

func TestRowData_GetField_NilReceiver(t *testing.T) {
	var r *RowData
	field, ok := r.GetField("anything")
	if ok {
		t.Error("GetField on nil RowData should return false")
	}
	if field != (Field{}) {
		t.Errorf("GetField on nil RowData should return zero Field, got %+v", field)
	}
}

func TestRowData_GetField_MissingField(t *testing.T) {
	r := NewRowData()
	_, ok := r.GetField("missing")
	if ok {
		t.Error("GetField on missing field should return false")
	}
}

func TestRowData_Clone(t *testing.T) {
	original := NewRowData()
	original.Set("id", 1, "int")
	original.Set("name", "alice", "varchar")

	cloned := original.Clone()

	// Verify fields match
	v, ok := cloned.Get("id")
	if !ok || v != 1 {
		t.Errorf("cloned.Get('id') = (%v, %v), want (1, true)", v, ok)
	}
	v, ok = cloned.Get("name")
	if !ok || v != "alice" {
		t.Errorf("cloned.Get('name') = (%v, %v), want ('alice', true)", v, ok)
	}

	// Verify mutation independence
	cloned.Set("id", 999, "int")
	origVal, _ := original.Get("id")
	if origVal == 999 {
		t.Error("mutating clone should not affect original")
	}
}

func TestRowData_Clone_Nil(t *testing.T) {
	var r *RowData
	cloned := r.Clone()
	if cloned != nil {
		t.Errorf("Clone of nil RowData should return nil, got %+v", cloned)
	}
}

func TestRowData_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		row      *RowData
		expected bool
	}{
		{"nil RowData", nil, true},
		{"empty fields", NewRowData(), true},
		{"with fields", func() *RowData {
			r := NewRowData()
			r.Set("x", 1, "int")
			return r
		}(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.row.IsEmpty(); got != tt.expected {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestRowData_MarshalJSON(t *testing.T) {
	// Non-nil RowData
	r := NewRowData()
	r.Set("id", 1, "int")

	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("MarshalJSON returned empty data")
	}

	// Nil RowData
	var nilRow *RowData
	data, err = nilRow.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON on nil failed: %v", err)
	}
	if string(data) != "null" {
		t.Errorf("MarshalJSON on nil should return 'null', got %q", string(data))
	}
}

func TestRowData_ColumnNames(t *testing.T) {
	r := NewRowData()
	r.Set("name", "alice", "varchar")
	r.Set("id", 1, "int")
	r.Set("email", "a@b.com", "varchar")

	names := r.ColumnNames()
	if len(names) != 3 {
		t.Fatalf("ColumnNames() returned %d names, want 3", len(names))
	}

	// Check all names exist (order is not guaranteed due to map)
	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	for _, expected := range []string{"id", "name", "email"} {
		if !nameSet[expected] {
			t.Errorf("ColumnNames() missing %q", expected)
		}
	}
}

func TestRowData_ColumnNames_Nil(t *testing.T) {
	var r *RowData
	names := r.ColumnNames()
	if names != nil {
		t.Errorf("ColumnNames on nil RowData should return nil, got %v", names)
	}
}

func TestRowData_ColumnNames_Empty(t *testing.T) {
	r := NewRowData()
	names := r.ColumnNames()
	if len(names) != 0 {
		t.Errorf("ColumnNames on empty RowData should return empty slice, got %v", names)
	}
}

// ---- SchemaInfo tests ----

func TestSchemaInfo_GetColumn(t *testing.T) {
	s := &SchemaInfo{
		Columns: []ColumnSchema{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "varchar"},
			{Name: "email", Type: "varchar"},
		},
	}

	// Found
	col := s.GetColumn("name")
	if col == nil {
		t.Fatal("GetColumn('name') returned nil")
	}
	if col.Name != "name" || col.Type != "varchar" {
		t.Errorf("GetColumn('name') = %+v, want name=name type=varchar", *col)
	}

	// Not found
	col = s.GetColumn("nonexistent")
	if col != nil {
		t.Errorf("GetColumn('nonexistent') should return nil, got %+v", *col)
	}
}

func TestSchemaInfo_Clone(t *testing.T) {
	original := &SchemaInfo{
		Version: 5,
		Columns: []ColumnSchema{
			{Name: "id", Type: "int", Optional: false},
			{Name: "name", Type: "varchar", Optional: true, Default: "unknown"},
		},
		PrimaryKey: []string{"id"},
	}

	cloned := original.Clone()

	// Verify values
	if cloned.Version != original.Version {
		t.Errorf("Version = %d, want %d", cloned.Version, original.Version)
	}
	if len(cloned.Columns) != len(original.Columns) {
		t.Fatalf("Columns len = %d, want %d", len(cloned.Columns), len(original.Columns))
	}
	if cloned.Columns[0].Name != "id" || cloned.Columns[1].Name != "name" {
		t.Error("Cloned columns don't match original")
	}
	if len(cloned.PrimaryKey) != 1 || cloned.PrimaryKey[0] != "id" {
		t.Error("Cloned PrimaryKey doesn't match original")
	}

	// Verify independence
	cloned.Columns[0].Name = "changed"
	if original.Columns[0].Name == "changed" {
		t.Error("mutating cloned Columns should not affect original")
	}
	cloned.PrimaryKey[0] = "changed"
	if original.PrimaryKey[0] == "changed" {
		t.Error("mutating cloned PrimaryKey should not affect original")
	}
}

func TestSchemaInfo_Clone_Nil(t *testing.T) {
	var s *SchemaInfo
	cloned := s.Clone()
	if cloned != nil {
		t.Errorf("Clone of nil SchemaInfo should return nil, got %+v", cloned)
	}
}

func TestSchemaInfo_IsCompatible(t *testing.T) {
	tests := []struct {
		name     string
		s1       *SchemaInfo
		s2       *SchemaInfo
		expected bool
	}{
		{
			name: "compatible schemas",
			s1: &SchemaInfo{
				Columns: []ColumnSchema{
					{Name: "id", Type: "int"},
					{Name: "name", Type: "varchar"},
				},
			},
			s2: &SchemaInfo{
				Columns: []ColumnSchema{
					{Name: "id", Type: "int"},
					{Name: "name", Type: "varchar"},
					{Name: "extra", Type: "text"},
				},
			},
			expected: true,
		},
		{
			name: "incompatible - missing column",
			s1: &SchemaInfo{
				Columns: []ColumnSchema{
					{Name: "id", Type: "int"},
					{Name: "missing", Type: "varchar"},
				},
			},
			s2: &SchemaInfo{
				Columns: []ColumnSchema{
					{Name: "id", Type: "int"},
				},
			},
			expected: false,
		},
		{
			name: "incompatible - type mismatch",
			s1: &SchemaInfo{
				Columns: []ColumnSchema{
					{Name: "id", Type: "int"},
				},
			},
			s2: &SchemaInfo{
				Columns: []ColumnSchema{
					{Name: "id", Type: "varchar"},
				},
			},
			expected: false,
		},
		{
			name:     "nil self",
			s1:       nil,
			s2:       &SchemaInfo{},
			expected: false,
		},
		{
			name:     "nil other",
			s1:       &SchemaInfo{},
			s2:       nil,
			expected: false,
		},
		{
			name:     "both nil",
			s1:       nil,
			s2:       nil,
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s1.IsCompatible(tt.s2); got != tt.expected {
				t.Errorf("IsCompatible() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// ---- SourceInfo tests ----

func TestSourceInfo_String(t *testing.T) {
	s := &SourceInfo{
		Connector: "mysql",
		Host:      "localhost",
		Port:      3306,
		Database:  "testdb",
	}
	expected := "mysql://localhost:3306/testdb"
	if got := s.String(); got != expected {
		t.Errorf("String() = %q, want %q", got, expected)
	}
}

// ---- TableInfo additional tests ----

func TestTableInfo_String(t *testing.T) {
	tests := []struct {
		name     string
		table    TableInfo
		expected string
	}{
		{
			name:     "without schema",
			table:    TableInfo{Database: "mydb", Table: "users"},
			expected: "mydb.users",
		},
		{
			name:     "with schema",
			table:    TableInfo{Database: "mydb", Schema: "public", Table: "users"},
			expected: "mydb.public.users",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.table.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTableInfo_GetColumn(t *testing.T) {
	table := &TableInfo{
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "name", Type: "varchar", Nullable: true, Length: 255},
		},
	}

	// Found
	col := table.GetColumn("name")
	if col == nil {
		t.Fatal("GetColumn('name') returned nil")
	}
	if col.Name != "name" || col.Type != "varchar" || col.Length != 255 {
		t.Errorf("GetColumn('name') = %+v, unexpected values", *col)
	}

	// Not found
	col = table.GetColumn("nonexistent")
	if col != nil {
		t.Errorf("GetColumn('nonexistent') should return nil, got %+v", *col)
	}
}

func TestTableInfo_Clone(t *testing.T) {
	original := &TableInfo{
		Database:          "mydb",
		Schema:            "public",
		Table:             "users",
		PrimaryKeyColumns: []string{"id"},
		UniqueKeyColumns:  []string{"email"},
		Columns: []ColumnInfo{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "email", Type: "varchar", Nullable: false, Length: 255},
		},
	}

	cloned := original.Clone()

	// Verify values
	if cloned.Database != original.Database || cloned.Schema != original.Schema || cloned.Table != original.Table {
		t.Error("Cloned basic fields don't match original")
	}
	if len(cloned.PrimaryKeyColumns) != 1 || cloned.PrimaryKeyColumns[0] != "id" {
		t.Error("Cloned PrimaryKeyColumns don't match")
	}
	if len(cloned.UniqueKeyColumns) != 1 || cloned.UniqueKeyColumns[0] != "email" {
		t.Error("Cloned UniqueKeyColumns don't match")
	}
	if len(cloned.Columns) != 2 {
		t.Fatalf("Cloned Columns len = %d, want 2", len(cloned.Columns))
	}

	// Verify independence
	cloned.PrimaryKeyColumns[0] = "changed"
	if original.PrimaryKeyColumns[0] == "changed" {
		t.Error("mutating cloned PrimaryKeyColumns should not affect original")
	}
	cloned.Columns[0].Name = "changed"
	if original.Columns[0].Name == "changed" {
		t.Error("mutating cloned Columns should not affect original")
	}
}

func TestTableInfo_Clone_NilSlices(t *testing.T) {
	original := &TableInfo{
		Database: "mydb",
		Table:    "users",
	}

	cloned := original.Clone()

	if cloned.PrimaryKeyColumns != nil {
		t.Errorf("Clone of nil PrimaryKeyColumns should be nil, got %v", cloned.PrimaryKeyColumns)
	}
	if cloned.UniqueKeyColumns != nil {
		t.Errorf("Clone of nil UniqueKeyColumns should be nil, got %v", cloned.UniqueKeyColumns)
	}
	if cloned.Columns != nil {
		t.Errorf("Clone of nil Columns should be nil, got %v", cloned.Columns)
	}
}

func TestTableInfo_HasUniqueKey_False(t *testing.T) {
	table := &TableInfo{Database: "db", Table: "t"}
	if table.HasUniqueKey() {
		t.Error("HasUniqueKey() should return false for empty UniqueKeyColumns")
	}
}

func TestTableInfo_GetKeyColumns_UniqueKey(t *testing.T) {
	table := &TableInfo{
		UniqueKeyColumns: []string{"email", "name"},
	}
	keys := table.GetKeyColumns()
	if len(keys) != 2 || keys[0] != "email" || keys[1] != "name" {
		t.Errorf("GetKeyColumns() = %v, want [email name]", keys)
	}
}

func TestTableInfo_GetKeyColumns_NoKeys(t *testing.T) {
	table := &TableInfo{}
	keys := table.GetKeyColumns()
	if keys != nil {
		t.Errorf("GetKeyColumns() with no keys = %v, want nil", keys)
	}
}

// ---- DDL tests ----

func TestNewDDLEvent(t *testing.T) {
	source := SourceInfo{
		Connector: "mysql",
		Database:  "testdb",
	}
	ddlInfo := DDLInfo{
		Type:      DDLTypeCreate,
		Statement: "CREATE TABLE users (id INT)",
		Database:  "testdb",
		Table:     "users",
	}
	pos := Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  1000,
	}

	evt := NewDDLEvent(source, ddlInfo, pos, "CREATE TABLE users (id INT)")

	if evt.Type != EventTypeDDL {
		t.Errorf("Type = %v, want %v", evt.Type, EventTypeDDL)
	}
	if evt.Source.Connector != "mysql" {
		t.Errorf("Source.Connector = %q, want 'mysql'", evt.Source.Connector)
	}
	if evt.DDL.Type != DDLTypeCreate {
		t.Errorf("DDL.Type = %v, want %v", evt.DDL.Type, DDLTypeCreate)
	}
	if evt.DDL.Statement != "CREATE TABLE users (id INT)" {
		t.Errorf("DDL.Statement = %q, unexpected", evt.DDL.Statement)
	}
	if evt.ID == "" {
		t.Error("ID should not be empty")
	}
	if evt.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
}

func TestDDLEvent_IsCreate(t *testing.T) {
	tests := []struct {
		name     string
		ddlType  DDLType
		isCreate bool
		isAlter  bool
		isDrop   bool
	}{
		{"create", DDLTypeCreate, true, false, false},
		{"alter", DDLTypeAlter, false, true, false},
		{"drop", DDLTypeDrop, false, false, true},
		{"rename", DDLTypeRename, false, false, false},
		{"truncate", DDLTypeTruncate, false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := &DDLEvent{DDL: DDLInfo{Type: tt.ddlType}}
			if got := evt.IsCreate(); got != tt.isCreate {
				t.Errorf("IsCreate() = %v, want %v", got, tt.isCreate)
			}
			if got := evt.IsAlter(); got != tt.isAlter {
				t.Errorf("IsAlter() = %v, want %v", got, tt.isAlter)
			}
			if got := evt.IsDrop(); got != tt.isDrop {
				t.Errorf("IsDrop() = %v, want %v", got, tt.isDrop)
			}
		})
	}
}

// ---- TransactionInfo tests ----

func TestTransactionInfo_IsBegin(t *testing.T) {
	tests := []struct {
		name     string
		tx       *TransactionInfo
		expected bool
	}{
		{"nil", nil, false},
		{"first event", &TransactionInfo{EventIndex: 0, TotalEvents: 5}, true},
		{"middle event", &TransactionInfo{EventIndex: 2, TotalEvents: 5}, false},
		{"last event", &TransactionInfo{EventIndex: 4, TotalEvents: 5}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tx.IsBegin(); got != tt.expected {
				t.Errorf("IsBegin() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTransactionInfo_IsEnd(t *testing.T) {
	tests := []struct {
		name     string
		tx       *TransactionInfo
		expected bool
	}{
		{"nil", nil, false},
		{"first event of 5", &TransactionInfo{EventIndex: 0, TotalEvents: 5}, false},
		{"last event of 5", &TransactionInfo{EventIndex: 4, TotalEvents: 5}, true},
		{"single event", &TransactionInfo{EventIndex: 0, TotalEvents: 1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tx.IsEnd(); got != tt.expected {
				t.Errorf("IsEnd() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTransactionInfo_IsSingleEvent(t *testing.T) {
	tests := []struct {
		name     string
		tx       *TransactionInfo
		expected bool
	}{
		{"nil", nil, false},
		{"single event", &TransactionInfo{TotalEvents: 1}, true},
		{"multiple events", &TransactionInfo{TotalEvents: 5}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tx.IsSingleEvent(); got != tt.expected {
				t.Errorf("IsSingleEvent() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// ---- HeartbeatEvent additional tests ----

func TestNewHeartbeat_Fields(t *testing.T) {
	source := SourceInfo{
		Connector: "postgresql",
		Host:      "pg-host",
		Port:      5432,
		Database:  "mydb",
	}
	pos := Position{
		LSN:        12345,
		CommitTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	hb := NewHeartbeat(source, pos)

	if hb.Source.Connector != "postgresql" {
		t.Errorf("Source.Connector = %q, want 'postgresql'", hb.Source.Connector)
	}
	if hb.Position.LSN != 12345 {
		t.Errorf("Position.LSN = %d, want 12345", hb.Position.LSN)
	}
	if hb.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
}

func TestHeartbeatEvent_ToChangeEvent_Fields(t *testing.T) {
	now := time.Now()
	hb := &HeartbeatEvent{
		Source: SourceInfo{
			Connector: "mysql",
			Database:  "db1",
		},
		Timestamp: now,
		Position: Position{
			BinlogFile: "mysql-bin.000001",
			BinlogPos:  999,
		},
	}

	evt := hb.ToChangeEvent()

	if evt.Type != EventTypeHeartbeat {
		t.Errorf("Type = %v, want %v", evt.Type, EventTypeHeartbeat)
	}
	if evt.Source.Connector != "mysql" {
		t.Errorf("Source.Connector = %q, want 'mysql'", evt.Source.Connector)
	}
	if !evt.Timestamp.Equal(now) {
		t.Errorf("Timestamp = %v, want %v", evt.Timestamp, now)
	}
	if evt.Position.BinlogFile != "mysql-bin.000001" {
		t.Errorf("Position.BinlogFile = %q, want 'mysql-bin.000001'", evt.Position.BinlogFile)
	}
	if evt.ID == "" {
		t.Error("ID should not be empty")
	}
}

func TestPositionGTIDField(t *testing.T) {
	pos := &Position{
		GTID:       "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
		CommitTime: time.Now(),
	}
	if pos.GTID == "" {
		t.Error("GTID should be set")
	}
	if pos.IsZero() {
		t.Error("Position with GTID should not be zero")
	}
	cloned := pos.Clone()
	if cloned.GTID != pos.GTID {
		t.Errorf("Clone GTID = %q, want %q", cloned.GTID, pos.GTID)
	}
}

func TestPositionResumeTokenField(t *testing.T) {
	token := []byte(`{"_data": "826470..."}`)
	pos := &Position{
		ResumeToken: token,
		CommitTime:  time.Now(),
	}
	if pos.ResumeToken == nil {
		t.Error("ResumeToken should be set")
	}
	cloned := pos.Clone()
	if !bytes.Equal(cloned.ResumeToken, pos.ResumeToken) {
		t.Error("Clone should copy ResumeToken")
	}
	// Mutate original should not affect clone
	pos.ResumeToken[0] = 0xFF
	if cloned.ResumeToken[0] == 0xFF {
		t.Error("Clone ResumeToken should be independent copy")
	}
}
