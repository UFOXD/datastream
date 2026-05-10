package postgres

import (
	"strings"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/postgres/generated"
)

// DDLVisitor traverses the PostgreSQL parse tree and extracts DDL information.
type DDLVisitor struct {
	*generated.BasePostgreSQLParserVisitor
	ddl string
}

// NewDDLVisitor creates a new DDL visitor.
func NewDDLVisitor() *DDLVisitor {
	return &DDLVisitor{
		BasePostgreSQLParserVisitor: &generated.BasePostgreSQLParserVisitor{},
	}
}

// VisitRoot visits the root node.
func (v *DDLVisitor) VisitRoot(ctx *generated.RootContext) interface{} {
	v.ddl = ctx.GetText()
	results := &parser.DDLResults{}

	if stmtblock := ctx.Stmtblock(); stmtblock != nil {
		stmtResults := v.VisitStmtblock(stmtblock.(*generated.StmtblockContext))
		if sr, ok := stmtResults.(*parser.DDLResults); ok {
			results.Results = append(results.Results, sr.Results...)
		}
	}

	return results
}

// VisitStmtblock visits statement block.
func (v *DDLVisitor) VisitStmtblock(ctx *generated.StmtblockContext) interface{} {
	results := &parser.DDLResults{}

	if stmtmulti := ctx.Stmtmulti(); stmtmulti != nil {
		stmtResults := v.VisitStmtmulti(stmtmulti.(*generated.StmtmultiContext))
		if sr, ok := stmtResults.(*parser.DDLResults); ok {
			results.Results = append(results.Results, sr.Results...)
		}
	}

	return results
}

// VisitStmtmulti visits multiple statements.
func (v *DDLVisitor) VisitStmtmulti(ctx *generated.StmtmultiContext) interface{} {
	results := &parser.DDLResults{}

	for _, stmt := range ctx.AllStmt() {
		result := v.VisitStmt(stmt.(*generated.StmtContext))
		if ddlResult, ok := result.(*parser.DDLResult); ok && ddlResult != nil {
			results.Add(ddlResult)
		}
	}

	return results
}

// VisitStmt visits a statement node.
func (v *DDLVisitor) VisitStmt(ctx *generated.StmtContext) interface{} {
	// CREATE DATABASE
	if createDb := ctx.Createdbstmt(); createDb != nil {
		return v.VisitCreatedbstmt(createDb.(*generated.CreatedbstmtContext))
	}

	// DROP DATABASE
	if dropDb := ctx.Dropdbstmt(); dropDb != nil {
		return v.VisitDropdbstmt(dropDb.(*generated.DropdbstmtContext))
	}

	// ALTER DATABASE
	if alterDb := ctx.Alterdatabasestmt(); alterDb != nil {
		return v.VisitAlterdatabasestmt(alterDb.(*generated.AlterdatabasestmtContext))
	}

	// CREATE SCHEMA (PostgreSQL schema is like a namespace)
	if createSchema := ctx.Createschemastmt(); createSchema != nil {
		return v.VisitCreateschemastmt(createSchema.(*generated.CreateschemastmtContext))
	}

	// CREATE TABLE
	if createTable := ctx.Createstmt(); createTable != nil {
		return v.VisitCreatestmt(createTable.(*generated.CreatestmtContext))
	}

	// DROP TABLE/INDEX/VIEW
	if dropStmt := ctx.Dropstmt(); dropStmt != nil {
		return v.VisitDropstmt(dropStmt.(*generated.DropstmtContext))
	}

	// ALTER TABLE
	if alterTable := ctx.Altertablestmt(); alterTable != nil {
		return v.VisitAltertablestmt(alterTable.(*generated.AltertablestmtContext))
	}

	// TRUNCATE
	if truncate := ctx.Truncatestmt(); truncate != nil {
		return v.VisitTruncatestmt(truncate.(*generated.TruncatestmtContext))
	}

	return nil
}

// VisitCreatedbstmt handles CREATE DATABASE statement.
func (v *DDLVisitor) VisitCreatedbstmt(ctx *generated.CreatedbstmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateDatabase,
		Statement: v.ddl,
	}

	if name := ctx.Name(); name != nil {
		result.Database = extractNameText(name)
	}

	return result
}

// VisitDropdbstmt handles DROP DATABASE statement.
func (v *DDLVisitor) VisitDropdbstmt(ctx *generated.DropdbstmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropDatabase,
		Statement: v.ddl,
	}

	if name := ctx.Name(); name != nil {
		result.Database = extractNameText(name)
	}

	return result
}

// VisitAlterdatabasestmt handles ALTER DATABASE statement.
func (v *DDLVisitor) VisitAlterdatabasestmt(ctx *generated.AlterdatabasestmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeAlterDatabase,
		Statement: v.ddl,
	}

	if name := ctx.Name(0); name != nil {
		result.Database = extractNameText(name)
	}

	return result
}

// VisitCreateschemastmt handles CREATE SCHEMA statement.
func (v *DDLVisitor) VisitCreateschemastmt(ctx *generated.CreateschemastmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateDatabase, // Schema in PostgreSQL is like a namespace
		Statement: v.ddl,
	}

	// Use ANTLR context method to get schema name via Colid
	if colid := ctx.Colid(); colid != nil {
		text := colid.GetText()
		result.Database = strings.Trim(text, "\"")
	}

	return result
}

// VisitCreatestmt handles CREATE TABLE statement.
func (v *DDLVisitor) VisitCreatestmt(ctx *generated.CreatestmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateTable,
		Statement: v.ddl,
	}

	// PostgreSQL uses Qualified_name for table names
	if qualifiedName := ctx.Qualified_name(0); qualifiedName != nil {
		schema, table := extractQualifiedName(qualifiedName)
		result.Database = schema
		result.Table = table
	}

	result.TableChanges = &parser.TableChanges{
		Operation: parser.TableOpCreate,
		Table: &parser.TableInfo{
			Database: result.Database,
			Name:     result.Table,
		},
	}

	return result
}

// VisitDropstmt handles DROP statement.
func (v *DDLVisitor) VisitDropstmt(ctx *generated.DropstmtContext) interface{} {
	// Determine what type of object is being dropped using ANTLR context methods
	if objType := ctx.Object_type_any_name(); objType != nil {
		objTypeCtx := objType.(*generated.Object_type_any_nameContext)

		// Check for TABLE
		if objTypeCtx.TABLE() != nil {
			return v.handleDropTable(ctx)
		}

		// Check for INDEX
		if objTypeCtx.INDEX() != nil {
			return v.handleDropIndex(ctx)
		}

		// Check for VIEW
		if objTypeCtx.VIEW() != nil {
			return v.handleDropView(ctx)
		}
	}

	// Check for SCHEMA (uses different context)
	if nameList := ctx.Name_list(); nameList != nil {
		// Could be DROP SCHEMA - check text for SCHEMA keyword
		text := ctx.GetText()
		if strings.Contains(strings.ToUpper(text), "SCHEMA") {
			return v.handleDropSchema(ctx)
		}
	}

	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: v.ddl,
	}
}

func (v *DDLVisitor) handleDropTable(ctx *generated.DropstmtContext) *parser.DDLResult {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropTable,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get table names
	if anyNameList := ctx.Any_name_list_(); anyNameList != nil {
		anyNameListCtx := anyNameList.(*generated.Any_name_list_Context)
		if anyNames := anyNameListCtx.AllAny_name(); len(anyNames) > 0 {
			// Get first table name
			firstAnyName := anyNames[0].(*generated.Any_nameContext)
			name := extractAnyName(firstAnyName)
			if strings.Contains(name, ".") {
				p := strings.SplitN(name, ".", 2)
				result.Database = p[0]
				result.Table = p[1]
			} else {
				result.Table = name
			}
		}
	}

	return result
}

func (v *DDLVisitor) handleDropIndex(ctx *generated.DropstmtContext) *parser.DDLResult {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropIndex,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get index names
	if anyNameList := ctx.Any_name_list_(); anyNameList != nil {
		anyNameListCtx := anyNameList.(*generated.Any_name_list_Context)
		if anyNames := anyNameListCtx.AllAny_name(); len(anyNames) > 0 {
			// Get first index name
			firstAnyName := anyNames[0].(*generated.Any_nameContext)
			name := extractAnyName(firstAnyName)

			// Split schema.index format if present
			var indexName, dbName string
			if strings.Contains(name, ".") {
				parts := strings.SplitN(name, ".", 2)
				dbName = parts[0]
				indexName = parts[1]
				result.Database = dbName
			} else {
				indexName = name
			}

			result.IndexChanges = &parser.IndexChanges{
				IndexName:    indexName,
				Operation:    "drop",
				DatabaseName: dbName,
			}
		}
	}

	return result
}

func (v *DDLVisitor) handleDropView(ctx *generated.DropstmtContext) *parser.DDLResult {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropView,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get view names
	if anyNameList := ctx.Any_name_list_(); anyNameList != nil {
		anyNameListCtx := anyNameList.(*generated.Any_name_list_Context)
		if anyNames := anyNameListCtx.AllAny_name(); len(anyNames) > 0 {
			// Get first view name
			firstAnyName := anyNames[0].(*generated.Any_nameContext)
			name := extractAnyName(firstAnyName)
			if strings.Contains(name, ".") {
				p := strings.SplitN(name, ".", 2)
				result.Database = p[0]
				result.Table = p[1]
			} else {
				result.Table = name
			}
		}
	}

	return result
}

func (v *DDLVisitor) handleDropSchema(ctx *generated.DropstmtContext) *parser.DDLResult {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropDatabase,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get schema names
	if nameList := ctx.Name_list(); nameList != nil {
		// Name_list contains a list of names
		text := nameList.GetText()
		// Remove quotes and get first name
		parts := strings.Split(text, ",")
		if len(parts) > 0 {
			result.Database = strings.Trim(strings.TrimSpace(parts[0]), "\"")
		}
	}

	return result
}

// extractAnyName extracts the name from an Any_nameContext
func extractAnyName(ctx *generated.Any_nameContext) string {
	if ctx == nil {
		return ""
	}
	// Any_name contains Colid which has the actual identifier
	text := ctx.GetText()
	return strings.Trim(text, "\"")
}

// VisitAltertablestmt handles ALTER TABLE statement.
func (v *DDLVisitor) VisitAltertablestmt(ctx *generated.AltertablestmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeAlterTable,
		Statement: v.ddl,
		TableChanges: &parser.TableChanges{
			Operation: parser.TableOpAlter,
		},
	}

	// PostgreSQL uses Relation_expr for table names in ALTER
	if relationExpr := ctx.Relation_expr(); relationExpr != nil {
		schema, table := extractRelationExpr(relationExpr)
		result.Database = schema
		result.Table = table
		result.TableChanges.Table = &parser.TableInfo{
			Database: schema,
			Name:     table,
		}
	}

	// Process ALTER commands
	if cmds := ctx.Alter_table_cmds(); cmds != nil {
		v.processAlterCommands(cmds.(*generated.Alter_table_cmdsContext), result.TableChanges)
	}

	return result
}

func (v *DDLVisitor) processAlterCommands(ctx *generated.Alter_table_cmdsContext, changes *parser.TableChanges) {
	for _, cmd := range ctx.AllAlter_table_cmd() {
		v.processAlterCommand(cmd.(*generated.Alter_table_cmdContext), changes)
	}
}

func (v *DDLVisitor) processAlterCommand(ctx *generated.Alter_table_cmdContext, changes *parser.TableChanges) {
	// ADD COLUMN - use ANTLR context methods
	if ctx.ADD_P() != nil {
		// Get column name from ColumnDef or Colid
		if colDef := ctx.ColumnDef(); colDef != nil {
			// ColumnDef contains the column definition, extract name from first Colid
			colName := extractColumnDefName(colDef.GetText())
			if colName != "" {
				col := &parser.ColumnInfo{
					Name:     colName,
					Nullable: true,
				}
				changes.AddedColumns = append(changes.AddedColumns, *col)
			}
		} else if colId := ctx.Colid(0); colId != nil {
			// Direct column name
			colName := strings.Trim(colId.GetText(), "\"")
			col := &parser.ColumnInfo{
				Name:     colName,
				Nullable: true,
			}
			changes.AddedColumns = append(changes.AddedColumns, *col)
		}
	}

	// DROP COLUMN - use ANTLR context methods
	if ctx.DROP() != nil && ctx.COLUMN() != nil {
		if colId := ctx.Colid(0); colId != nil {
			colName := strings.Trim(colId.GetText(), "\"")
			changes.DroppedColumns = append(changes.DroppedColumns, colName)
		}
	}

	// ALTER COLUMN - use ANTLR context methods
	if ctx.ALTER() != nil && ctx.COLUMN() != nil {
		if colId := ctx.Colid(0); colId != nil {
			colName := strings.Trim(colId.GetText(), "\"")
			newCol := &parser.ColumnInfo{
				Name: colName,
			}
			changes.ModifiedColumns = append(changes.ModifiedColumns, parser.ColumnModification{
				Old: parser.ColumnInfo{Name: newCol.Name},
				New: *newCol,
			})
		}
	}
}

// extractColumnDefName extracts column name from column definition text
func extractColumnDefName(text string) string {
	text = strings.TrimSpace(text)
	text = strings.Trim(text, "\"")

	// Column name is typically the first word in the definition
	// But we need to handle cases like "id SERIAL PRIMARY KEY"
	parts := strings.Fields(text)
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// VisitTruncatestmt handles TRUNCATE statement.
func (v *DDLVisitor) VisitTruncatestmt(ctx *generated.TruncatestmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeTruncate,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get table names
	if relationExprList := ctx.Relation_expr_list(); relationExprList != nil {
		relationExprListCtx := relationExprList.(*generated.Relation_expr_listContext)
		if relationExpr := relationExprListCtx.Relation_expr(0); relationExpr != nil {
			schema, table := extractRelationExpr(relationExpr)
			result.Database = schema
			result.Table = table
		}
	}

	return result
}

// Helper functions

func extractNameText(name generated.INameContext) string {
	if name == nil {
		return ""
	}
	text := name.GetText()
	return strings.Trim(text, "\"")
}

func extractQualifiedName(qualifiedName generated.IQualified_nameContext) (schema, table string) {
	if qualifiedName == nil {
		return "", ""
	}

	text := qualifiedName.GetText()
	text = strings.ReplaceAll(text, "\"", "")

	// Check if there's a dot (schema.table format)
	if strings.Contains(text, ".") {
		parts := strings.SplitN(text, ".", 2)
		return parts[0], parts[1]
	}
	return "", text
}

func extractRelationExpr(relationExpr generated.IRelation_exprContext) (schema, table string) {
	if relationExpr == nil {
		return "", ""
	}

	text := relationExpr.GetText()
	text = strings.ReplaceAll(text, "\"", "")

	// Check if there's a dot (schema.table format)
	if strings.Contains(text, ".") {
		parts := strings.SplitN(text, ".", 2)
		return parts[0], parts[1]
	}
	return "", text
}

func extractColumnName(text, keyword string) string {
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, keyword)
	if idx == -1 {
		return ""
	}
	rest := text[idx+len(keyword):]
	rest = strings.TrimSpace(rest)

	// Skip type keywords like "ADD", "DROP", "ALTER"
	for _, skip := range []string{"ADD", "DROP", "ALTER", "COLUMN", "SET", "TYPE"} {
		if strings.HasPrefix(strings.ToUpper(rest), skip) {
			rest = strings.TrimSpace(rest[len(skip):])
		}
	}

	parts := strings.Fields(rest)
	if len(parts) > 0 {
		return strings.Trim(parts[0], "\"")
	}
	return ""
}
