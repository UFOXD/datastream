package parser

import (
	"testing"
)

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("Registry should not be nil")
	}
	if r.parsers == nil {
		t.Error("parsers map should be initialized")
	}
}

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	mockParser := &mockParser{supportedTypes: []DDLType{DDLTypeCreateTable}}

	r.Register("mysql", mockParser)

	p := r.Get("mysql")
	if p == nil {
		t.Fatal("Parser should not be nil after registration")
	}

	types := p.SupportedTypes()
	if len(types) != 1 || types[0] != DDLTypeCreateTable {
		t.Errorf("Expected [DDLTypeCreateTable], got %v", types)
	}
}

func TestRegistryGetNonExistent(t *testing.T) {
	r := NewRegistry()

	p := r.Get("nonexistent")
	if p != nil {
		t.Error("Expected nil for non-existent parser")
	}
}

func TestRegistryUnregister(t *testing.T) {
	r := NewRegistry()
	mockParser := &mockParser{}

	r.Register("mysql", mockParser)
	r.Unregister("mysql")

	p := r.Get("mysql")
	if p != nil {
		t.Error("Expected nil after unregister")
	}
}

func TestRegistryList(t *testing.T) {
	r := NewRegistry()
	mockParser := &mockParser{}

	r.Register("mysql", mockParser)
	r.Register("postgres", mockParser)
	r.Register("mongodb", mockParser)

	types := r.List()
	if len(types) != 3 {
		t.Errorf("Expected 3 types, got %d", len(types))
	}

	// Check all types are present
	typeMap := make(map[string]bool)
	for _, t := range types {
		typeMap[t] = true
	}

	if !typeMap["mysql"] || !typeMap["postgres"] || !typeMap["mongodb"] {
		t.Error("Expected mysql, postgres, and mongodb in list")
	}
}

func TestDefaultRegistry(t *testing.T) {
	if DefaultRegistry == nil {
		t.Fatal("DefaultRegistry should not be nil")
	}
}

func TestRegister(t *testing.T) {
	mockParser := &mockParser{}
	Register("test-connector", mockParser)

	p := Get("test-connector")
	if p == nil {
		t.Error("Parser should be registered in default registry")
	}

	// Cleanup
	DefaultRegistry.Unregister("test-connector")
}

func TestGet(t *testing.T) {
	mockParser := &mockParser{}
	Register("test-get", mockParser)

	p := Get("test-get")
	if p == nil {
		t.Error("Get should return registered parser")
	}

	p = Get("nonexistent-parser")
	if p != nil {
		t.Error("Get should return nil for non-existent parser")
	}

	// Cleanup
	DefaultRegistry.Unregister("test-get")
}

