// Package sqlserver provides a SQL Server T-SQL DDL parser using ANTLR.
package sqlserver

import (
	"context"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/sqlserver/generated"
	"github.com/antlr4-go/antlr/v4"
)

// Parser is a SQL Server T-SQL DDL parser using ANTLR.
type Parser struct {
	visitor *DDLVisitor
}

// NewParser creates a new SQL Server DDL parser.
func NewParser() *Parser {
	return &Parser{
		visitor: NewDDLVisitor(),
	}
}

// Parse parses a DDL statement and returns structured result.
func (p *Parser) Parse(ctx context.Context, ddl string) (*parser.DDLResult, error) {
	// Create Lexer and Parser
	input := antlr.NewInputStream(ddl)
	lexer := generated.NewTSqlLexer(input)
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	antlrParser := generated.NewTSqlParser(tokenStream)

	// Parse using visitor pattern
	tree := antlrParser.Tsql_file()
	result := p.visitor.VisitTsql_file(tree.(*generated.Tsql_fileContext))

	if ddlResult, ok := result.(*parser.DDLResult); ok {
		return ddlResult, nil
	}

	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: ddl,
	}, nil
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
