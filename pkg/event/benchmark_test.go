package event

import (
	"testing"
	"time"
)

// BenchmarkChangeEventCreation benchmarks creating change events
func BenchmarkChangeEventCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = &ChangeEvent{
			ID:   "test-event",
			Type: EventTypeInsert,
			Source: SourceInfo{
				Connector: "mysql",
				Database:  "test",
			},
			Table: TableInfo{
				Database: "test",
				Table:    "users",
			},
			After: RowData{
				Fields: map[string]Field{
					"id":    {Name: "id", Value: 1},
					"name":  {Name: "name", Value: "test"},
					"email": {Name: "email", Value: "test@example.com"},
				},
			},
			Position: Position{
				BinlogFile: "mysql-bin.000001",
				BinlogPos:  12345,
			},
		}
	}
}

// BenchmarkRowDataAccess benchmarks accessing row data fields
func BenchmarkRowDataAccess(b *testing.B) {
	rowData := RowData{
		Fields: map[string]Field{
			"id":     {Name: "id", Value: 1},
			"name":   {Name: "name", Value: "test"},
			"email":  {Name: "email", Value: "test@example.com"},
			"status": {Name: "status", Value: "active"},
			"count":  {Name: "count", Value: 100},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = rowData.Get("id")
		_, _ = rowData.Get("name")
		_, _ = rowData.Get("email")
		_, _ = rowData.Get("status")
		_, _ = rowData.Get("count")
	}
}

// BenchmarkRowDataSet benchmarks setting row data fields
func BenchmarkRowDataSet(b *testing.B) {
	rowData := RowData{
		Fields: make(map[string]Field),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowData.Set("id", i, "int")
		rowData.Set("name", "test", "varchar")
		rowData.Set("value", i*100, "int")
	}
}

// BenchmarkPositionMarshal benchmarks position serialization
func BenchmarkPositionMarshal(b *testing.B) {
	pos := &Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = pos.MarshalBinary()
	}
}

// BenchmarkPositionUnmarshal benchmarks position deserialization
func BenchmarkPositionUnmarshal(b *testing.B) {
	pos := &Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}
	data, _ := pos.MarshalBinary()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newPos := &Position{}
		_ = newPos.UnmarshalBinary(data)
	}
}

// BenchmarkPositionClone benchmarks position cloning
func BenchmarkPositionClone(b *testing.B) {
	pos := &Position{
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  12345,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pos.Clone()
	}
}

// BenchmarkGenerateEventID benchmarks event ID generation
func BenchmarkGenerateEventID(b *testing.B) {
	source := &SourceInfo{
		Connector: "mysql",
		Database:  "test",
	}
	ts := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GenerateEventID(source, ts, i)
	}
}
