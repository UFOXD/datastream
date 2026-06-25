// Package postgres provides a PostgreSQL DDL parser using ANTLR.
package postgres

import (
	"context"
	"fmt"

	"github.com/UFOXD/datastream/pkg/event"
	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/postgres/generated"
	"github.com/antlr4-go/antlr/v4"
)

// Parser is a PostgreSQL DDL parser using ANTLR.
type Parser struct {
	visitor *DDLVisitor
}

// NewParser creates a new PostgreSQL DDL parser.
func NewParser() *Parser {
	return &Parser{
		visitor: NewDDLVisitor(),
	}
}

// Parse parses one or more DDL statements and returns structured results.
func (p *Parser) Parse(ctx context.Context, ddl string) ([]*parser.DDLResult, error) {
	// Create Lexer and Parser
	input := antlr.NewInputStream(ddl)
	lexer := generated.NewPostgreSQLLexer(input)
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	antlrParser := generated.NewPostgreSQLParser(tokenStream)

	// Parse using visitor pattern
	tree := antlrParser.Root()
	result := p.visitor.VisitRoot(tree.(*generated.RootContext))

	// Handle both single result and multiple results
	switch r := result.(type) {
	case *parser.DDLResults:
		if len(r.Results) == 0 {
			return []*parser.DDLResult{{
				Type:      parser.DDLTypeUnknown,
				Statement: ddl,
			}}, nil
		}
		return r.Results, nil
	case *parser.DDLResult:
		return []*parser.DDLResult{r}, nil
	default:
		return []*parser.DDLResult{{
			Type:      parser.DDLTypeUnknown,
			Statement: ddl,
		}}, nil
	}
}

// SupportedTypes returns the DDL types this parser can handle.
func (p *Parser) SupportedTypes() []parser.DDLType {
	return []parser.DDLType{
		parser.DDLTypeCreateDatabase,
		parser.DDLTypeDropDatabase,
		parser.DDLTypeAlterDatabase,
		parser.DDLTypeCreateTable,
		parser.DDLTypeDropTable,
		parser.DDLTypeAlterTable,
		parser.DDLTypeRenameTable,
		parser.DDLTypeTruncate,
		parser.DDLTypeCreateIndex,
		parser.DDLTypeDropIndex,
		parser.DDLTypeCreateView,
		parser.DDLTypeDropView,
	}
}

// Ensure Parser implements DDLParser interface
var _ parser.DDLParser = (*Parser)(nil)

// ApplyDDL is not yet implemented for PostgreSQL.
func (p *Parser) ApplyDDL(_ context.Context, _ *event.TableInfo, _ string) (*parser.DDLResult, error) {
	return nil, fmt.Errorf("ApplyDDL not yet implemented for %s", p.Name())
}

// Name returns the parser name.
func (p *Parser) Name() string {
	return "postgres"
}
