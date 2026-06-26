// Package postgres provides a PostgreSQL DDL parser using ANTLR.
package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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

	// Store original DDL text in visitor for type extraction
	p.visitor.ddl = ddl

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

// ApplyDDL applies a DDL statement to a table structure and returns the resulting new table structure.
func (p *Parser) ApplyDDL(ctx context.Context, oldTable *event.TableInfo, ddl string) (*parser.DDLResult, error) {
	results, err := p.Parse(ctx, ddl)
	if err != nil {
		return nil, fmt.Errorf("parse DDL: %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no DDL result parsed")
	}

	result := results[0]

	switch result.Type {
	case parser.DDLTypeCreateTable:
		if result.TableChanges != nil && result.TableChanges.Table != nil {
			result.NewTableInfo = buildEventTableInfo(result.TableChanges.Table)
		}
	case parser.DDLTypeAlterTable:
		if oldTable == nil {
			return nil, fmt.Errorf("oldTable is required for ALTER TABLE")
		}
		newTable := oldTable.Clone()
		applyAlterChanges(newTable, result.TableChanges)
		result.NewTableInfo = newTable
	case parser.DDLTypeDropTable:
		result.NewTableInfo = nil
	default:
		// For non-table DDL, pass through oldTable if provided
		if oldTable != nil {
			result.NewTableInfo = oldTable.Clone()
		}
	}

	return result, nil
}

// buildEventTableInfo converts a parser.TableInfo to an event.TableInfo.
func buildEventTableInfo(ti *parser.TableInfo) *event.TableInfo {
	result := &event.TableInfo{
		Database: ti.Database,
		Table:    ti.Name,
	}
	for _, col := range ti.Columns {
		result.Columns = append(result.Columns, convertToEventColumn(col))
	}
	if len(ti.PrimaryKey) > 0 {
		result.PrimaryKeyColumns = ti.PrimaryKey
	}
	return result
}

// convertToEventColumn converts a parser.ColumnInfo to an event.ColumnInfo.
func convertToEventColumn(col parser.ColumnInfo) event.ColumnInfo {
	ec := event.ColumnInfo{
		Name:     col.Name,
		Nullable: col.Nullable,
	}
	ec.Type, ec.Length, ec.Scale = parseTypeString(col.Type)
	return ec
}

// parseTypeString parses a PostgreSQL type string like "VARCHAR(255)" or "NUMERIC(10,2)"
// into base type, length, and scale.
func parseTypeString(typeStr string) (baseType string, length, scale int) {
	if typeStr == "" {
		return "", 0, 0
	}

	idx := strings.Index(typeStr, "(")
	if idx == -1 {
		return strings.ToUpper(typeStr), 0, 0
	}

	baseType = strings.ToUpper(typeStr[:idx])
	params := strings.TrimSuffix(typeStr[idx+1:], ")")
	parts := strings.SplitN(params, ",", 2)

	if len(parts) >= 1 {
		length, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
	}
	if len(parts) >= 2 {
		scale, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
	}

	return baseType, length, scale
}

// applyAlterChanges applies ALTER TABLE changes to a cloned event.TableInfo.
func applyAlterChanges(table *event.TableInfo, changes *parser.TableChanges) {
	if changes == nil {
		return
	}

	// Apply ADD columns
	for _, addCol := range changes.AddedColumns {
		table.Columns = append(table.Columns, convertToEventColumn(addCol))
	}

	// Apply DROP columns
	for _, dropName := range changes.DroppedColumns {
		for i, col := range table.Columns {
			if col.Name == dropName {
				table.Columns = append(table.Columns[:i], table.Columns[i+1:]...)
				break
			}
		}
	}

	// Apply MODIFY/CHANGE columns
	for _, mod := range changes.ModifiedColumns {
		for i, col := range table.Columns {
			if col.Name == mod.Old.Name {
				table.Columns[i] = mergeEventColumn(col, mod.New)
				break
			}
		}
	}

	// Apply primary key changes
	if changes.PrimaryKeyChange != nil {
		table.PrimaryKeyColumns = changes.PrimaryKeyChange.NewColumns
	}
}

// mergeEventColumn applies partial changes from a parser.ColumnInfo to an existing event.ColumnInfo.
// Only non-zero fields in 'change' override the existing column.
func mergeEventColumn(existing event.ColumnInfo, change parser.ColumnInfo) event.ColumnInfo {
	result := existing

	if change.Name != "" {
		result.Name = change.Name
	}
	if change.Type != "" {
		result.Type, result.Length, result.Scale = parseTypeString(change.Type)
	}
	// Nullable is always applied (it's a bool with meaningful false value)
	result.Nullable = change.Nullable

	return result
}

// Name returns the parser name.
func (p *Parser) Name() string {
	return "postgres"
}

func init() {
	parser.Register("postgres", NewParser())
}
