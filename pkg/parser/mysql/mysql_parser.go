// Package mysql provides a MySQL DDL parser using ANTLR.
// This implementation follows the design document specification.
package mysql

import (
	"context"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/mysql/generated"
	"github.com/antlr4-go/antlr/v4"
)

// Parser is a MySQL DDL parser using ANTLR.
type Parser struct {
	visitor *DDLVisitor
}

// NewParser creates a new MySQL DDL parser.
func NewParser() *Parser {
	return &Parser{
		visitor: NewDDLVisitor(),
	}
}

// Parse parses one or more DDL statements and returns structured results.
func (p *Parser) Parse(ctx context.Context, ddl string) ([]*parser.DDLResult, error) {
	// 1. Create Lexer
	input := antlr.NewInputStream(ddl)
	lexer := generated.NewMySqlLexer(input)
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// 2. Create Parser
	antlrParser := generated.NewMySqlParser(tokenStream)

	// 3. Parse
	tree := antlrParser.Root()

	// 4. Traverse parse tree using Accept to dispatch to correct visitor method
	result := tree.Accept(p.visitor)
	if result == nil {
		return []*parser.DDLResult{{
			Type:      parser.DDLTypeUnknown,
			Statement: ddl,
		}}, nil
	}

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
