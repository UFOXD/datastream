package transform

import (
	"context"
	"fmt"
	"sync"

	"github.com/UFOXD/datastream/pkg/event"
)

// TransformFunc is a function that transforms an event.
type TransformFunc func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error)

// CustomTransformer allows custom transformation logic.
type CustomTransformer struct {
	name        string
	transformFn TransformFunc
}

// CustomTransformerConfig holds configuration.
type CustomTransformerConfig struct {
	// Name is the transformer name
	Name string `json:"name" toml:"name"`

	// Type is the transformer type (lua, wasm, native)
	Type string `json:"type" toml:"type"`

	// Script is the script content (for lua/wasm)
	Script string `json:"script" toml:"script"`

	// TransformFunc is the native transform function
	TransformFunc TransformFunc `json:"-"`
}

// NewCustomTransformer creates a custom transformer.
func NewCustomTransformer(config *CustomTransformerConfig) (*CustomTransformer, error) {
	t := &CustomTransformer{
		name: config.Name,
	}

	switch config.Type {
	case "native":
		if config.TransformFunc == nil {
			return nil, fmt.Errorf("native transformer requires TransformFunc")
		}
		t.transformFn = config.TransformFunc

	case "lua":
		// Placeholder for Lua script transformer
		// Would require a Lua VM integration
		return nil, fmt.Errorf("lua transformer not yet implemented")

	case "wasm":
		// Placeholder for WASM transformer
		// Would require a WASM runtime integration
		return nil, fmt.Errorf("wasm transformer not yet implemented")

	default:
		return nil, fmt.Errorf("unknown transformer type: %s", config.Type)
	}

	return t, nil
}

// Transform applies the custom transformation.
func (t *CustomTransformer) Transform(e *event.ChangeEvent) (*event.ChangeEvent, error) {
	if t.transformFn == nil {
		return e, nil
	}
	return t.transformFn(context.Background(), e)
}

// Name returns the transformer name.
func (t *CustomTransformer) Name() string {
	return t.name
}

// ScriptTransformerRegistry manages script-based transformers.
type ScriptTransformerRegistry struct {
	mu           sync.RWMutex
	transformers map[string]*CustomTransformer
}

// NewScriptTransformerRegistry creates a registry.
func NewScriptTransformerRegistry() *ScriptTransformerRegistry {
	return &ScriptTransformerRegistry{
		transformers: make(map[string]*CustomTransformer),
	}
}

// Register registers a transformer.
func (r *ScriptTransformerRegistry) Register(name string, t *CustomTransformer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transformers[name] = t
}

// Get retrieves a transformer by name.
func (r *ScriptTransformerRegistry) Get(name string) (*CustomTransformer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.transformers[name]
	return t, ok
}

// Remove removes a transformer.
func (r *ScriptTransformerRegistry) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.transformers, name)
}

// List returns all registered transformer names.
func (r *ScriptTransformerRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.transformers))
	for name := range r.transformers {
		names = append(names, name)
	}
	return names
}

// Built-in transformers

// NewAddFieldTransformer creates a transformer that adds a static field.
func NewAddFieldTransformer(fieldName string, value interface{}) *CustomTransformer {
	return &CustomTransformer{
		name: "add-field-" + fieldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if !e.After.IsEmpty() {
				if e.After.Fields == nil {
					e.After.Fields = make(map[string]event.Field)
				}
				e.After.Fields[fieldName] = event.Field{
					Name:  fieldName,
					Value: value,
					Type:  "string",
				}
			}
			return e, nil
		},
	}
}

// NewRemoveFieldTransformer creates a transformer that removes a field.
func NewRemoveFieldTransformer(fieldName string) *CustomTransformer {
	return &CustomTransformer{
		name: "remove-field-" + fieldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if !e.After.IsEmpty() && e.After.Fields != nil {
				delete(e.After.Fields, fieldName)
			}
			if !e.Before.IsEmpty() && e.Before.Fields != nil {
				delete(e.Before.Fields, fieldName)
			}
			return e, nil
		},
	}
}

// NewRenameFieldTransformer creates a transformer that renames a field.
func NewRenameFieldTransformer(oldName, newName string) *CustomTransformer {
	return &CustomTransformer{
		name: "rename-field-" + oldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if !e.After.IsEmpty() && e.After.Fields != nil {
				if v, ok := e.After.Fields[oldName]; ok {
					delete(e.After.Fields, oldName)
					v.Name = newName
					e.After.Fields[newName] = v
				}
			}
			if !e.Before.IsEmpty() && e.Before.Fields != nil {
				if v, ok := e.Before.Fields[oldName]; ok {
					delete(e.Before.Fields, oldName)
					v.Name = newName
					e.Before.Fields[newName] = v
				}
			}
			return e, nil
		},
	}
}

// NewTimestampTransformer creates a transformer that adds a timestamp field.
func NewTimestampTransformer(fieldName string) *CustomTransformer {
	return &CustomTransformer{
		name: "timestamp-" + fieldName,
		transformFn: func(ctx context.Context, e *event.ChangeEvent) (*event.ChangeEvent, error) {
			if !e.After.IsEmpty() {
				if e.After.Fields == nil {
					e.After.Fields = make(map[string]event.Field)
				}
				e.After.Fields[fieldName] = event.Field{
					Name:  fieldName,
					Value: e.Timestamp,
					Type:  "timestamp",
				}
			}
			return e, nil
		},
	}
}
