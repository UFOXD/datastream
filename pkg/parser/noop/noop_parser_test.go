package noop

import (
	"context"
	"testing"

	"github.com/UFOXD/datastream/pkg/parser"
)

func TestNewParser(t *testing.T) {
	p := NewParser()
	if p == nil {
		t.Fatal("Parser should not be nil")
	}
}

func TestParse(t *testing.T) {
	p := NewParser()
	ctx := context.Background()

	tests := []struct {
		name string
		ddl  string
	}{
		{
			name: "any ddl statement",
			ddl:  "CREATE TABLE users (id INT)",
		},
		{
			name: "empty string",
			ddl:  "",
		},
		{
			name: "random text",
			ddl:  "some random text that is not DDL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.Parse(ctx, tt.ddl)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if result == nil {
				t.Fatal("Result should not be nil")
			}
			if result.Type != parser.DDLTypeUnknown {
				t.Errorf("Expected DDLTypeUnknown, got %s", result.Type)
			}
			if result.Statement != tt.ddl {
				t.Errorf("Expected statement %s, got %s", tt.ddl, result.Statement)
			}
		})
	}
}

func TestSupportedTypes(t *testing.T) {
	p := NewParser()
	types := p.SupportedTypes()

	if types == nil {
		t.Fatal("SupportedTypes should not return nil")
	}
	if len(types) != 0 {
		t.Errorf("Expected empty slice, got %d types", len(types))
	}
}

func TestParserImplementsInterface(t *testing.T) {
	var _ parser.DDLParser = NewParser()
}
