package postgres

import (
	"strings"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/postgres/generated"
)

// DDLVisitor traverses the PostgreSQL parse tree and extracts DDL information.
type DDLVisitor struct {
	*generated.BasePostgreSQLParserVisitor
	ddl        string // original DDL text with proper spacing
	ddlRaw     string // ANTLR-concatenated text
}

// NewDDLVisitor creates a new DDL visitor.
func NewDDLVisitor() *DDLVisitor {
	return &DDLVisitor{
		BasePostgreSQLParserVisitor: &generated.BasePostgreSQLParserVisitor{},
	}
}

// VisitRoot visits the root node.
func (v *DDLVisitor) VisitRoot(ctx *generated.RootContext) interface{} {
	v.ddlRaw = ctx.GetText()
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

	// RENAME (ALTER TABLE ... RENAME COLUMN)
	if renameStmt := ctx.Renamestmt(); renameStmt != nil {
		return v.VisitRenamestmt(renameStmt.(*generated.RenamestmtContext))
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

	tableInfo := &parser.TableInfo{
		Database: result.Database,
		Name:     result.Table,
	}

	// Extract column definitions from Opttableelementlist
	if optTableElemList := ctx.Opttableelementlist(); optTableElemList != nil {
		v.extractTableColumns(optTableElemList.(*generated.OpttableelementlistContext), tableInfo)
	}

	result.TableChanges = &parser.TableChanges{
		Operation: parser.TableOpCreate,
		Table:     tableInfo,
	}

	return result
}

// extractTableColumns extracts column definitions from CREATE TABLE element list.
func (v *DDLVisitor) extractTableColumns(ctx *generated.OpttableelementlistContext, tableInfo *parser.TableInfo) {
	tableElemList := ctx.Tableelementlist()
	if tableElemList == nil {
		return
	}
	elemListCtx := tableElemList.(*generated.TableelementlistContext)
	for _, elem := range elemListCtx.AllTableelement() {
		elemCtx := elem.(*generated.TableelementContext)
		if colDef := elemCtx.ColumnDef(); colDef != nil {
			col := v.extractColumnFromDef(colDef.(*generated.ColumnDefContext))
			if col != nil {
				tableInfo.Columns = append(tableInfo.Columns, *col)
			}
		}
	}
}

// extractColumnFromDef extracts column info from a ColumnDef context.
// Uses the raw DDL text for reliable type extraction since ANTLR's GetText()
// concatenates tokens without spaces, losing type modifiers like (100).
func (v *DDLVisitor) extractColumnFromDef(ctx *generated.ColumnDefContext) *parser.ColumnInfo {
	col := &parser.ColumnInfo{
		Nullable: true, // PostgreSQL columns are nullable by default
	}

	// Get column name from ANTLR context
	if colId := ctx.Colid(); colId != nil {
		col.Name = strings.Trim(colId.GetText(), "\"")
	}

	// Get column type from raw DDL text by finding the column definition
	col.Type = v.extractColumnTypeFromDDL(col.Name)

	// Check column constraints for NOT NULL, DEFAULT, PRIMARY KEY
	if colQualList := ctx.Colquallist(); colQualList != nil {
		v.applyColumnConstraints(colQualList.(*generated.ColquallistContext), col)
	}

	return col
}

// extractColumnTypeFromDDL extracts the column type from the original DDL string
// by finding the column name and parsing the type that follows it.
func (v *DDLVisitor) extractColumnTypeFromDDL(colName string) string {
	// Find the column name in the DDL (case-insensitive)
	upper := strings.ToUpper(v.ddl)
	nameUpper := strings.ToUpper(colName)

	// Look for "colName " pattern (column name followed by space)
	searchStr := nameUpper + " "
	idx := strings.Index(upper, searchStr)
	if idx == -1 {
		return ""
	}

	// Extract text after the column name
	rest := v.ddl[idx+len(searchStr):]
	rest = strings.TrimSpace(rest)

	// Parse the type: read tokens until we hit a constraint keyword or end
	// Constraints start with: NOT, NULL, PRIMARY, UNIQUE, DEFAULT, CHECK, REFERENCES, CONSTRAINT
	constraintKeywords := []string{"NOT", "NULL", "PRIMARY", "UNIQUE", "DEFAULT", "CHECK", "REFERENCES", "CONSTRAINT", "COLLATE"}

	// Handle parenthesized type modifiers like VARCHAR(255) or NUMERIC(10,2)
	var typeStr strings.Builder
	parenDepth := 0
	i := 0

	for i < len(rest) {
		ch := rest[i]

		if ch == '(' {
			parenDepth++
			typeStr.WriteByte(ch)
			i++
			continue
		}
		if ch == ')' {
			parenDepth--
			typeStr.WriteByte(ch)
			i++
			continue
		}

		if parenDepth > 0 {
			typeStr.WriteByte(ch)
			i++
			continue
		}

		// At top level, check for space-separated tokens
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			// Check if next token is a constraint keyword
			remaining := strings.TrimSpace(rest[i:])
			remainingUpper := strings.ToUpper(remaining)
			isConstraint := false
			for _, kw := range constraintKeywords {
				if strings.HasPrefix(remainingUpper, kw) {
					isConstraint = true
					break
				}
			}
			if isConstraint {
				break
			}
			// Could be a compound type like "DOUBLE PRECISION" or "CHARACTER VARYING"
			// Check if the next word is part of the type
			nextWord := strings.Fields(remaining)
			if len(nextWord) > 0 {
				nextUpper := strings.ToUpper(nextWord[0])
				partOfType := false
				switch nextUpper {
				case "PRECISION", "VARYING", "WITHOUT", "WITH", "ZONE", "TIME":
					partOfType = true
				}
				if partOfType {
					typeStr.WriteByte(ch)
					i++
					continue
				}
			}
			break
		}

		// Stop at comma (column separator in CREATE TABLE)
		if ch == ',' {
			break
		}

		typeStr.WriteByte(ch)
		i++
	}

	return strings.TrimSpace(typeStr.String())
}

// applyColumnConstraints applies column constraints (NOT NULL, DEFAULT, PRIMARY KEY).
func (v *DDLVisitor) applyColumnConstraints(ctx *generated.ColquallistContext, col *parser.ColumnInfo) {
	for _, constraint := range ctx.AllColconstraint() {
		constraintCtx := constraint.(*generated.ColconstraintContext)
		if elem := constraintCtx.Colconstraintelem(); elem != nil {
			elemCtx := elem.(*generated.ColconstraintelemContext)
			// NOT NULL
			if elemCtx.NOT() != nil && elemCtx.NULL_P() != nil {
				col.Nullable = false
			}
			// PRIMARY KEY implies NOT NULL
			if elemCtx.PRIMARY() != nil && elemCtx.KEY() != nil {
				col.Nullable = false
			}
		}
	}
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
	// ADD COLUMN
	if ctx.ADD_P() != nil {
		if colDef := ctx.ColumnDef(); colDef != nil {
			col := v.extractColumnFromDef(colDef.(*generated.ColumnDefContext))
			if col != nil {
				changes.AddedColumns = append(changes.AddedColumns, *col)
			}
		}
		return
	}

	// ALTER COLUMN (SET/DROP NOT NULL, SET/DROP DEFAULT, TYPE)
	// Must check before DROP COLUMN since ALTER COLUMN ... DROP NOT NULL
	// has both DROP and Column_ non-nil.
	if ctx.ALTER() != nil && ctx.Column_() != nil {
		v.processAlterColumn(ctx, changes)
		return
	}

	// DROP COLUMN - COLUMN is under Column_() context, not a direct terminal
	// Only match when ALTER is NOT present (to avoid matching ALTER COLUMN ... DROP NOT NULL)
	if ctx.DROP() != nil && ctx.Column_() != nil && ctx.ALTER() == nil {
		if colId := ctx.Colid(0); colId != nil {
			colName := strings.Trim(colId.GetText(), "\"")
			changes.DroppedColumns = append(changes.DroppedColumns, colName)
		}
		return
	}
}

// processAlterColumn handles ALTER COLUMN sub-commands.
func (v *DDLVisitor) processAlterColumn(ctx *generated.Alter_table_cmdContext, changes *parser.TableChanges) {
	colId := ctx.Colid(0)
	if colId == nil {
		return
	}
	colName := strings.Trim(colId.GetText(), "\"")

	mod := parser.ColumnModification{
		Old: parser.ColumnInfo{Name: colName},
		New: parser.ColumnInfo{Name: colName},
	}

	// ALTER COLUMN TYPE
	if ctx.TYPE_P() != nil {
		mod.New.Type = v.extractAlterColumnTypeFromDDL(colName)
		changes.ModifiedColumns = append(changes.ModifiedColumns, mod)
		return
	}

	// ALTER COLUMN SET NOT NULL / DROP NOT NULL
	if ctx.SET() != nil && ctx.NOT() != nil && ctx.NULL_P() != nil {
		mod.New.Nullable = false
		changes.ModifiedColumns = append(changes.ModifiedColumns, mod)
		return
	}
	if ctx.DROP() != nil && ctx.NOT() != nil && ctx.NULL_P() != nil {
		mod.New.Nullable = true
		changes.ModifiedColumns = append(changes.ModifiedColumns, mod)
		return
	}

	// ALTER COLUMN SET DEFAULT / DROP DEFAULT
	if alterDefault := ctx.Alter_column_default(); alterDefault != nil {
		defaultCtx := alterDefault.(*generated.Alter_column_defaultContext)
		if defaultCtx.SET() != nil && defaultCtx.DEFAULT() != nil {
			// Extract default value expression
			if aExpr := defaultCtx.A_expr(); aExpr != nil {
				mod.New.DefaultValue = aExpr.GetText()
			}
		}
		// DROP DEFAULT: leave DefaultValue as nil/zero
		changes.ModifiedColumns = append(changes.ModifiedColumns, mod)
		return
	}

	// Fallback: record as modification with no field changes (name-only)
	changes.ModifiedColumns = append(changes.ModifiedColumns, mod)
}

// extractAlterColumnTypeFromDDL extracts the new type from ALTER COLUMN colname TYPE ...
func (v *DDLVisitor) extractAlterColumnTypeFromDDL(colName string) string {
	upper := strings.ToUpper(v.ddl)
	nameUpper := strings.ToUpper(colName)

	// Find "TYPE" after the column name
	searchStr := nameUpper + " TYPE "
	idx := strings.Index(upper, searchStr)
	if idx == -1 {
		return ""
	}

	rest := v.ddl[idx+len(searchStr):]
	rest = strings.TrimSpace(rest)

	// Parse the type (same logic as extractColumnTypeFromDDL)
	var typeStr strings.Builder
	parenDepth := 0
	for i := 0; i < len(rest); i++ {
		ch := rest[i]
		if ch == '(' {
			parenDepth++
			typeStr.WriteByte(ch)
			continue
		}
		if ch == ')' {
			parenDepth--
			typeStr.WriteByte(ch)
			continue
		}
		if parenDepth > 0 {
			typeStr.WriteByte(ch)
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == ';' {
			break
		}
		typeStr.WriteByte(ch)
	}

	return strings.TrimSpace(typeStr.String())
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

// VisitRenamestmt handles RENAME statements including ALTER TABLE RENAME COLUMN.
func (v *DDLVisitor) VisitRenamestmt(ctx *generated.RenamestmtContext) interface{} {
	// Only handle TABLE RENAME COLUMN
	if ctx.TABLE() == nil || ctx.Column_() == nil {
		return &parser.DDLResult{
			Type:      parser.DDLTypeUnknown,
			Statement: v.ddl,
		}
	}

	result := &parser.DDLResult{
		Type:      parser.DDLTypeAlterTable,
		Statement: v.ddl,
		TableChanges: &parser.TableChanges{
			Operation: parser.TableOpAlter,
		},
	}

	// Get table name from Relation_expr
	if relationExpr := ctx.Relation_expr(); relationExpr != nil {
		schema, table := extractRelationExpr(relationExpr)
		result.Database = schema
		result.Table = table
		result.TableChanges.Table = &parser.TableInfo{
			Database: schema,
			Name:     table,
		}
	}

	// Get old and new column names from AllName()
	names := ctx.AllName()
	if len(names) >= 2 {
		oldName := extractNameText(names[0])
		newName := extractNameText(names[1])
		result.TableChanges.ModifiedColumns = append(result.TableChanges.ModifiedColumns, parser.ColumnModification{
			Old: parser.ColumnInfo{Name: oldName},
			New: parser.ColumnInfo{Name: newName},
		})
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
