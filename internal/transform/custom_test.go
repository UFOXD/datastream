package transform

import (
	"context"
	"testing"
	"time"

	"github.com/UFOXD/datastream/pkg/event"
)

func TestCustomTransformer_NativeTransform(t *testing.T) {
	config := &CustomTransformerConfig{
		Name: "test-transformer",
		Type: "native",
		TransformFunc: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			e.After.Fields["transformed"] = event.Field{Name: "transformed", Value: true}
			return e, nil
		},
	}

	transformer, err := NewCustomTransformer(config)
	if err != nil {
		t.Fatalf("NewCustomTransformer failed: %v", err)
	}

	e := &event.ChangeEvent{
		After: event.RowData{
			Fields: map[string]event.Field{"id": {Name: "id", Value: 1}},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result.After.Fields["transformed"].Value != true {
		t.Error("Expected transformed field to be true")
	}
}

func TestAddFieldTransformer(t *testing.T) {
	transformer := NewAddFieldTransformer("source", "mysql")

	e := &event.ChangeEvent{
		After: event.RowData{
			Fields: map[string]event.Field{"id": {Name: "id", Value: 1}},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result.After.Fields["source"].Value != "mysql" {
		t.Error("Expected source field to be 'mysql'")
	}
}

func TestRemoveFieldTransformer(t *testing.T) {
	transformer := NewRemoveFieldTransformer("password")

	e := &event.ChangeEvent{
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":       {Name: "id", Value: 1},
				"password": {Name: "password", Value: "secret"},
			},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if _, exists := result.After.Fields["password"]; exists {
		t.Error("Expected password field to be removed")
	}
}

func TestRenameFieldTransformer(t *testing.T) {
	transformer := NewRenameFieldTransformer("old_name", "new_name")

	e := &event.ChangeEvent{
		After: event.RowData{
			Fields: map[string]event.Field{
				"id":       {Name: "id", Value: 1},
				"old_name": {Name: "old_name", Value: "value"},
			},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if _, exists := result.After.Fields["old_name"]; exists {
		t.Error("Expected old_name to be removed")
	}
	if result.After.Fields["new_name"].Value != "value" {
		t.Error("Expected new_name to have value 'value'")
	}
}

func TestScriptTransformerRegistry(t *testing.T) {
	registry := NewScriptTransformerRegistry()

	transformer := NewAddFieldTransformer("test", 123)
	registry.Register("test", transformer)

	t2, ok := registry.Get("test")
	if !ok {
		t.Fatal("Expected to find transformer")
	}
	if t2.Name() != "add-field-test" {
		t.Errorf("Expected name 'add-field-test', got '%s'", t2.Name())
	}

	names := registry.List()
	if len(names) != 1 {
		t.Errorf("Expected 1 transformer, got %d", len(names))
	}

	registry.Remove("test")
	if _, ok := registry.Get("test"); ok {
		t.Error("Expected transformer to be removed")
	}
}

func TestTimestampTransformer(t *testing.T) {
	transformer := NewTimestampTransformer("event_time")

	ts := time.Now()
	e := &event.ChangeEvent{
		Timestamp: ts,
		After: event.RowData{
			Fields: map[string]event.Field{"id": {Name: "id", Value: 1}},
		},
	}

	result, err := transformer.Transform(e)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result.After.Fields["event_time"].Value != ts {
		t.Error("Expected event_time to match event timestamp")
	}
}
