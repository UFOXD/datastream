// Package noop provides a no-op DDL parser for databases that don't need SQL parsing.
// PostgreSQL, MongoDB, Oracle, and SQL Server provide structured DDL events
// through their CDC mechanisms, so no SQL parsing is required.
package noop

import (
	"context"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
)

// Parser is a no-op parser that returns unknown results.
// Used for databases whose CDC mechanisms already provide structured output.
type Parser struct{}

// NewParser creates a new no-op parser.
func NewParser() *Parser {
	return &Parser{}
}

// Parse returns a basic DDLResult with unknown type.
// The actual DDL handling is done by the database's CDC mechanism.
func (p *Parser) Parse(ctx context.Context, ddl string) ([]*parser.DDLResult, error) {
	return []*parser.DDLResult{{
		Type:      parser.DDLTypeUnknown,
		Statement: ddl,
	}}, nil
}

// SupportedTypes returns an empty list as this parser doesn't parse any DDL types.
func (p *Parser) SupportedTypes() []parser.DDLType {
	return []parser.DDLType{}
}

// ApplyDDL is a no-op implementation that returns nil, nil.
func (p *Parser) ApplyDDL(_ context.Context, _ *event.TableInfo, _ string) (*parser.DDLResult, error) {
	return nil, nil
}

// Ensure Parser implements DDLParser interface
var _ parser.DDLParser = (*Parser)(nil)
