package filter

import (
	"testing"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestExpressionFilter_TableMatch(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		event      *event.ChangeEvent
		expected   bool
	}{
		{
			name:       "exact table match",
			expression: "table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "users"},
			},
			expected: true,
		},
		{
			name:       "table not match",
			expression: "table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "orders"},
			},
			expected: false,
		},
		{
			name:       "database match",
			expression: "database == 'inventory'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Database: "inventory", Table: "users"},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(&ExpressionConfig{Expression: tt.expression})
			if err != nil {
				t.Fatalf("NewExpressionFilter failed: %v", err)
			}

			result, err := filter.Filter(tt.event)
			if err != nil {
				t.Fatalf("Filter failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Filter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExpressionFilter_LogicalOperators(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		event      *event.ChangeEvent
		expected   bool
	}{
		{
			name:       "AND operator - both true",
			expression: "database == 'db1' && table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Database: "db1", Table: "users"},
			},
			expected: true,
		},
		{
			name:       "AND operator - one false",
			expression: "database == 'db1' && table == 'users'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Database: "db1", Table: "orders"},
			},
			expected: false,
		},
		{
			name:       "OR operator - one true",
			expression: "table == 'users' || table == 'orders'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "users"},
			},
			expected: true,
		},
		{
			name:       "OR operator - both false",
			expression: "table == 'users' || table == 'orders'",
			event: &event.ChangeEvent{
				Table: event.TableInfo{Table: "products"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(&ExpressionConfig{Expression: tt.expression})
			if err != nil {
				t.Fatalf("NewExpressionFilter failed: %v", err)
			}

			result, err := filter.Filter(tt.event)
			if err != nil {
				t.Fatalf("Filter failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Filter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExpressionFilter_FieldAccess(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		event      *event.ChangeEvent
		expected   bool
	}{
		{
			name:       "after field comparison",
			expression: "after.age > 18",
			event: &event.ChangeEvent{
				After: event.RowData{
					Fields: map[string]event.Field{"age": {Name: "age", Value: 25}},
				},
			},
			expected: true,
		},
		{
			name:       "after field equality",
			expression: "after.status == 'active'",
			event: &event.ChangeEvent{
				After: event.RowData{
					Fields: map[string]event.Field{"status": {Name: "status", Value: "active"}},
				},
			},
			expected: true,
		},
		{
			name:       "regex match",
			expression: "after.email =~ '.*@example.com'",
			event: &event.ChangeEvent{
				After: event.RowData{
					Fields: map[string]event.Field{"email": {Name: "email", Value: "user@example.com"}},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewExpressionFilter(&ExpressionConfig{Expression: tt.expression})
			if err != nil {
				t.Fatalf("NewExpressionFilter failed: %v", err)
			}

			result, err := filter.Filter(tt.event)
			if err != nil {
				t.Fatalf("Filter failed: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Filter() = %v, want %v", result, tt.expected)
			}
		})
	}
}
