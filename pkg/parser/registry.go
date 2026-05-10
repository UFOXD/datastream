package parser

import "sync"

// Registry manages registered DDL parsers.
type Registry struct {
	mu      sync.RWMutex
	parsers map[string]DDLParser
}

// NewRegistry creates a new parser registry.
func NewRegistry() *Registry {
	return &Registry{
		parsers: make(map[string]DDLParser),
	}
}

// Register registers a parser for a connector type.
func (r *Registry) Register(connectorType string, parser DDLParser) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.parsers[connectorType] = parser
}

// Get retrieves a parser for a connector type.
// Returns nil if no parser is registered.
func (r *Registry) Get(connectorType string) DDLParser {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.parsers[connectorType]
}

// Unregister removes a parser for a connector type.
func (r *Registry) Unregister(connectorType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.parsers, connectorType)
}

// List returns all registered connector types.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.parsers))
	for t := range r.parsers {
		types = append(types, t)
	}
	return types
}

// DefaultRegistry is the global default parser registry.
var DefaultRegistry = NewRegistry()

// Register registers a parser with the default registry.
func Register(connectorType string, parser DDLParser) {
	DefaultRegistry.Register(connectorType, parser)
}

// Get retrieves a parser from the default registry.
func Get(connectorType string) DDLParser {
	return DefaultRegistry.Get(connectorType)
}
