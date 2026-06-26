package sqlserver

import (
	"fmt"
	"strings"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/sqlserver/generated"
)

// DDLVisitor traverses the SQL Server T-SQL parse tree and extracts DDL information.
type DDLVisitor struct {
	*generated.BaseTSqlParserVisitor
	ddl string
}

// NewDDLVisitor creates a new DDL visitor.
func NewDDLVisitor() *DDLVisitor {
	return &DDLVisitor{
		BaseTSqlParserVisitor: &generated.BaseTSqlParserVisitor{},
	}
}

// VisitTsql_file visits the root node.
func (v *DDLVisitor) VisitTsql_file(ctx *generated.Tsql_fileContext) interface{} {
	v.ddl = ctx.GetText()
	results := &parser.DDLResults{}

	for _, batch := range ctx.AllBatch() {
		batchResults := v.VisitBatch(batch.(*generated.BatchContext))
		if br, ok := batchResults.(*parser.DDLResults); ok {
			results.Results = append(results.Results, br.Results...)
		}
	}

	return results
}

// VisitBatch visits a batch node.
func (v *DDLVisitor) VisitBatch(ctx *generated.BatchContext) interface{} {
	results := &parser.DDLResults{}

	for _, stmt := range ctx.AllSql_clauses() {
		result := v.VisitSql_clauses(stmt.(*generated.Sql_clausesContext))
		if ddlResult, ok := result.(*parser.DDLResult); ok && ddlResult != nil {
			results.Add(ddlResult)
		}
	}

	return results
}

// VisitSql_clauses visits SQL clauses node.
func (v *DDLVisitor) VisitSql_clauses(ctx *generated.Sql_clausesContext) interface{} {
	// Check DDL clause
	if ddlClause := ctx.Ddl_clause(); ddlClause != nil {
		return v.VisitDdl_clause(ddlClause.(*generated.Ddl_clauseContext))
	}
	return nil
}

// VisitDdl_clause visits DDL clause node.
func (v *DDLVisitor) VisitDdl_clause(ctx *generated.Ddl_clauseContext) interface{} {
	// CREATE TABLE
	if createTable := ctx.Create_table(); createTable != nil {
		return v.VisitCreate_table(createTable.(*generated.Create_tableContext))
	}

	// ALTER TABLE
	if alterTable := ctx.Alter_table(); alterTable != nil {
		return v.VisitAlter_table(alterTable.(*generated.Alter_tableContext))
	}

	// DROP TABLE
	if dropTable := ctx.Drop_table(); dropTable != nil {
		return v.VisitDrop_table(dropTable.(*generated.Drop_tableContext))
	}

	// DROP INDEX
	if dropIndex := ctx.Drop_index(); dropIndex != nil {
		return v.VisitDrop_index(dropIndex.(*generated.Drop_indexContext))
	}

	// DROP VIEW
	if dropView := ctx.Drop_view(); dropView != nil {
		return v.VisitDrop_view(dropView.(*generated.Drop_viewContext))
	}

	// TRUNCATE TABLE
	if truncateTable := ctx.Truncate_table(); truncateTable != nil {
		return v.VisitTruncate_table(truncateTable.(*generated.Truncate_tableContext))
	}

	// CREATE INDEX
	if createIndex := ctx.Create_index(); createIndex != nil {
		return v.VisitCreate_index(createIndex.(*generated.Create_indexContext))
	}

	// DROP DATABASE
	if dropDb := ctx.Drop_database(); dropDb != nil {
		return v.visitDropDatabase(dropDb.(*generated.Drop_databaseContext))
	}

	return nil
}

// VisitCreate_table handles CREATE TABLE statement.
func (v *DDLVisitor) VisitCreate_table(ctx *generated.Create_tableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateTable,
		Statement: v.ddl,
	}

	// Extract table name
	if tableName := ctx.Table_name(); tableName != nil {
		schema, table := extractTableRef(tableName)
		result.Database = schema
		result.Table = table
	}

	tableInfo := &parser.TableInfo{
		Database: result.Database,
		Name:     result.Table,
	}

	// Extract column definitions from Column_def_table_constraints
	if colDefs := ctx.Column_def_table_constraints(); colDefs != nil {
		for _, cdtc := range colDefs.AllColumn_def_table_constraint() {
			constraintCtx := cdtc.(*generated.Column_def_table_constraintContext)
			if colDef := constraintCtx.Column_definition(); colDef != nil {
				col := extractColumnInfoFromDef(colDef.(*generated.Column_definitionContext), v.ddl)
				if col != nil {
					tableInfo.Columns = append(tableInfo.Columns, *col)
				}
			}
		}
	}

	result.TableChanges = &parser.TableChanges{
		Operation: parser.TableOpCreate,
		Table:     tableInfo,
	}

	return result
}

// VisitAlter_table handles ALTER TABLE statement.
func (v *DDLVisitor) VisitAlter_table(ctx *generated.Alter_tableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeAlterTable,
		Statement: v.ddl,
		TableChanges: &parser.TableChanges{
			Operation: parser.TableOpAlter,
		},
	}

	// Extract table name using ANTLR context method
	if tableName := ctx.Table_name(0); tableName != nil {
		schema, table := extractTableRef(tableName)
		result.Database = schema
		result.Table = table
		result.TableChanges.Table = &parser.TableInfo{
			Database: schema,
			Name:     table,
		}
	}

	// ADD COLUMN - check if ADD keyword exists
	if ctx.ADD() != nil {
		if colDefs := ctx.Column_def_table_constraints(); colDefs != nil {
			for _, cdtc := range colDefs.AllColumn_def_table_constraint() {
				constraintCtx := cdtc.(*generated.Column_def_table_constraintContext)
				if colDef := constraintCtx.Column_definition(); colDef != nil {
					col := extractColumnInfoFromDef(colDef.(*generated.Column_definitionContext), v.ddl)
					if col != nil {
						result.TableChanges.AddedColumns = append(result.TableChanges.AddedColumns, *col)
					}
				}
			}
		} else if ctx.Column_definition() != nil {
			col := extractColumnInfoFromDef(ctx.Column_definition().(*generated.Column_definitionContext), v.ddl)
			if col != nil {
				result.TableChanges.AddedColumns = append(result.TableChanges.AddedColumns, *col)
			}
		}
	}

	// DROP COLUMN - check if DROP keyword exists with COLUMN keyword
	if ctx.DROP() != nil && ctx.COLUMN() != nil {
		if colId := ctx.Id_(0); colId != nil {
			colName := strings.Trim(colId.GetText(), "[]\"")
			result.TableChanges.DroppedColumns = append(result.TableChanges.DroppedColumns, colName)
		}
	}

	// ALTER COLUMN - check if ALTER keyword exists with COLUMN keyword
	// When ALTER TABLE ... ALTER COLUMN col TYPE, Column_definition() is set but ADD() is nil
	if ctx.ADD() == nil && ctx.DROP() == nil && ctx.Column_definition() != nil {
		colDef := ctx.Column_definition().(*generated.Column_definitionContext)
		col := extractColumnInfoFromDef(colDef, v.ddl)
		if col != nil {
			mod := parser.ColumnModification{
				Old: parser.ColumnInfo{Name: col.Name},
				New: *col,
			}
			result.TableChanges.ModifiedColumns = append(result.TableChanges.ModifiedColumns, mod)
		}
	}

	return result
}

// VisitDrop_table handles DROP TABLE statement.
func (v *DDLVisitor) VisitDrop_table(ctx *generated.Drop_tableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropTable,
		Statement: v.ddl,
	}

	// Extract table name
	if tableName := ctx.Table_name(0); tableName != nil {
		schema, table := extractTableRef(tableName)
		result.Database = schema
		result.Table = table
	}

	return result
}

// VisitDrop_index handles DROP INDEX statement.
func (v *DDLVisitor) VisitDrop_index(ctx *generated.Drop_indexContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropIndex,
		Statement: v.ddl,
	}

	// Use ANTLR context methods to get index name
	// Try modern syntax first: DROP INDEX IF EXISTS index_name ON table_name
	if dropIndex := ctx.Drop_relational_or_xml_or_spatial_index(0); dropIndex != nil {
		dropIndexCtx := dropIndex.(*generated.Drop_relational_or_xml_or_spatial_indexContext)

		// Get index name
		if indexName := dropIndexCtx.GetIndex_name(); indexName != nil {
			text := indexName.GetText()
			text = strings.Trim(text, "[]\"")
			result.IndexChanges = &parser.IndexChanges{
				IndexName: text,
				Operation: "drop",
			}
		}

		// Get table name from Full_table_name
		if fullTableName := dropIndexCtx.Full_table_name(); fullTableName != nil {
			tableText := fullTableName.GetText()
			tableText = strings.ReplaceAll(tableText, "[", "")
			tableText = strings.ReplaceAll(tableText, "]", "")
			if strings.Contains(tableText, ".") {
				parts := strings.Split(tableText, ".")
				if len(parts) >= 2 {
					result.Database = parts[0]
					result.Table = parts[len(parts)-1]
				} else {
					result.Table = tableText
				}
			} else {
				result.Table = tableText
			}
			if result.IndexChanges != nil {
				result.IndexChanges.DatabaseName = result.Database
				result.IndexChanges.TableName = result.Table
			}
		}
	} else if dropIndex := ctx.Drop_backward_compatible_index(0); dropIndex != nil {
		// Handle backward compatible syntax: DROP INDEX index_name ON table_name
		dropIndexCtx := dropIndex.(*generated.Drop_backward_compatible_indexContext)

		// Get index name from GetIndex_name
		if indexName := dropIndexCtx.GetIndex_name(); indexName != nil {
			text := indexName.GetText()
			text = strings.Trim(text, "[]\"")
			result.IndexChanges = &parser.IndexChanges{
				IndexName: text,
				Operation: "drop",
			}
		}

		// Get table name from GetTable_or_view_name
		if tableOrViewName := dropIndexCtx.GetTable_or_view_name(); tableOrViewName != nil {
			tableText := tableOrViewName.GetText()
			tableText = strings.ReplaceAll(tableText, "[", "")
			tableText = strings.ReplaceAll(tableText, "]", "")
			if strings.Contains(tableText, ".") {
				parts := strings.Split(tableText, ".")
				if len(parts) >= 2 {
					result.Database = parts[0]
					result.Table = parts[len(parts)-1]
				} else {
					result.Table = tableText
				}
			} else {
				result.Table = tableText
			}
			if result.IndexChanges != nil {
				result.IndexChanges.DatabaseName = result.Database
				result.IndexChanges.TableName = result.Table
			}
		}
	}

	return result
}

// VisitDrop_view handles DROP VIEW statement.
func (v *DDLVisitor) VisitDrop_view(ctx *generated.Drop_viewContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropView,
		Statement: v.ddl,
	}

	// Extract view name (SQL Server uses Simple_name for views)
	if viewName := ctx.Simple_name(0); viewName != nil {
		text := viewName.GetText()
		text = strings.ReplaceAll(text, "[", "")
		text = strings.ReplaceAll(text, "]", "")
		text = strings.ReplaceAll(text, "\"", "")

		if strings.Contains(text, ".") {
			parts := strings.Split(text, ".")
			if len(parts) >= 2 {
				result.Database = parts[0]
				result.Table = parts[1]
			} else {
				result.Table = text
			}
		} else {
			result.Table = text
		}
	}

	return result
}

// VisitTruncate_table handles TRUNCATE TABLE statement.
func (v *DDLVisitor) VisitTruncate_table(ctx *generated.Truncate_tableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeTruncate,
		Statement: v.ddl,
	}

	// Extract table name
	if tableName := ctx.Table_name(); tableName != nil {
		schema, table := extractTableRef(tableName)
		result.Database = schema
		result.Table = table
	}

	return result
}

// VisitCreate_index handles CREATE INDEX statement.
func (v *DDLVisitor) VisitCreate_index(ctx *generated.Create_indexContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateIndex,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get index name (first Id_ is the index name)
	if indexName := ctx.Id_(0); indexName != nil {
		text := indexName.GetText()
		text = strings.Trim(text, "[]\"")
		result.IndexChanges = &parser.IndexChanges{
			IndexName: text,
			Operation: "create",
		}
	}

	// Use ANTLR context method to get table name
	if tableName := ctx.Table_name(); tableName != nil {
		schema, table := extractTableRef(tableName)
		result.Database = schema
		result.Table = table
		if result.IndexChanges != nil {
			result.IndexChanges.DatabaseName = schema
			result.IndexChanges.TableName = table
		}
	}

	return result
}

// visitDropDatabase handles DROP DATABASE statement.
func (v *DDLVisitor) visitDropDatabase(ctx *generated.Drop_databaseContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropDatabase,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get database name (first Id_ is the database name)
	if dbName := ctx.Id_(0); dbName != nil {
		text := dbName.GetText()
		result.Database = strings.Trim(text, "[]\"")
	}

	return result
}

// Helper functions

func extractTableRef(tableName generated.ITable_nameContext) (schema, table string) {
	if tableName == nil {
		return "", ""
	}

	text := tableName.GetText()
	text = strings.ReplaceAll(text, "[", "")
	text = strings.ReplaceAll(text, "]", "")
	text = strings.ReplaceAll(text, "\"", "")

	// Check if there's a dot (schema.table or database.schema.table format)
	if strings.Contains(text, ".") {
		parts := strings.Split(text, ".")
		if len(parts) >= 2 {
			// Return last two parts (schema.table or database.table)
			if len(parts) >= 3 {
				return parts[1], parts[2]
			}
			return parts[0], parts[1]
		}
	}
	return "", text
}

func extractColName(text, keyword string) string {
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, keyword)
	if idx == -1 {
		return ""
	}
	rest := text[idx+len(keyword):]
	rest = strings.TrimSpace(rest)

	// Skip type keywords
	for _, skip := range []string{"COLUMN", "ADD", "DROP", "ALTER", "SET", "TYPE"} {
		if strings.HasPrefix(strings.ToUpper(rest), skip) {
			rest = strings.TrimSpace(rest[len(skip):])
		}
	}

	parts := strings.Fields(rest)
	if len(parts) > 0 {
		return strings.Trim(parts[0], "[]\"")
	}
	return ""
}

// extractColumnFromDef extracts column name from column definition text
func extractColumnFromDef(text string) string {
	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "[", "")
	text = strings.ReplaceAll(text, "]", "")

	// Column name is typically the first word in the definition
	parts := strings.Fields(text)
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// extractColumnInfoFromDef extracts column info from a Column_definitionContext.
func extractColumnInfoFromDef(ctx *generated.Column_definitionContext, ddl string) *parser.ColumnInfo {
	if ctx == nil {
		return nil
	}

	col := &parser.ColumnInfo{
		Nullable: true, // default nullable
	}

	// Extract column name
	if id := ctx.Id_(); id != nil {
		col.Name = strings.Trim(id.GetText(), "[]\"")
	}

	// Extract data type using labeled fields from grammar
	if dataType := ctx.Data_type(); dataType != nil {
		col.Type = extractDataTypeString(dataType.(*generated.Data_typeContext))
	}

	// If grammar didn't capture the type (e.g., NVARCHAR(255) where NVARCHAR is a keyword),
	// try extracting from the original DDL text.
	if col.Type == "" && col.Name != "" {
		col.Type = extractColumnTypeFromDDL(ddl, col.Name)
	}

	// Check for NULL/NOT NULL constraints and PRIMARY KEY in column_definition_element
	for _, elem := range ctx.AllColumn_definition_element() {
		elemCtx := elem.(*generated.Column_definition_elementContext)
		if colConstraint := elemCtx.Column_constraint(); colConstraint != nil {
			constraintCtx := colConstraint.(*generated.Column_constraintContext)
			// Check for NULL/NOT NULL
			if nullNotNull := constraintCtx.Null_notnull(); nullNotNull != nil {
				nnCtx := nullNotNull.(*generated.Null_notnullContext)
				if nnCtx.NOT() != nil {
					col.Nullable = false
				}
			}
			// PRIMARY KEY columns are NOT NULL by default
			if constraintCtx.PRIMARY() != nil && constraintCtx.KEY() != nil {
				col.Nullable = false
			}
		}
	}

	if col.Name == "" {
		return nil
	}
	return col
}

// extractDataTypeString constructs the full type string from a Data_typeContext.
// Grammar alternatives:
//   - scaled = (VARCHAR|NVARCHAR|...) '(' MAX ')'
//   - ext_type = id_ '(' scale = DECIMAL ',' prec = DECIMAL ')'
//   - ext_type = id_ '(' scale = DECIMAL ')'
//   - ext_type = id_ IDENTITY ...
//   - double_prec = DOUBLE PRECISION?
//   - unscaled_type = id_
func extractDataTypeString(ctx *generated.Data_typeContext) string {
	// VARCHAR(MAX), NVARCHAR(MAX), etc.
	if ctx.GetScaled() != nil {
		return strings.ToUpper(ctx.GetScaled().GetText()) + "(MAX)"
	}
	// DECIMAL(10,2), NUMERIC(10,2)
	if ctx.GetExt_type() != nil && ctx.GetPrec() != nil && ctx.GetScale() != nil {
		return strings.ToUpper(ctx.GetExt_type().GetText()) +
			"(" + ctx.GetScale().GetText() + "," + ctx.GetPrec().GetText() + ")"
	}
	// NVARCHAR(100), VARCHAR(50)
	if ctx.GetExt_type() != nil && ctx.GetScale() != nil {
		return strings.ToUpper(ctx.GetExt_type().GetText()) +
			"(" + ctx.GetScale().GetText() + ")"
	}
	// IDENTITY types
	if ctx.GetExt_type() != nil && ctx.IDENTITY() != nil {
		base := strings.ToUpper(ctx.GetExt_type().GetText()) + " IDENTITY"
		if ctx.GetSeed() != nil && ctx.GetInc() != nil {
			base += "(" + ctx.GetSeed().GetText() + "," + ctx.GetInc().GetText() + ")"
		}
		return base
	}
	// DOUBLE PRECISION
	if ctx.GetDouble_prec() != nil {
		return "DOUBLE PRECISION"
	}
	// Simple types: INT, BIGINT, etc.
	if ctx.GetUnscaled_type() != nil {
		return strings.ToUpper(ctx.GetUnscaled_type().GetText())
	}
	// Return empty to let the DDL-text fallback handle it
	// (grammar may not capture types like NVARCHAR(255) where NVARCHAR is a keyword)
	return ""
}

// extractColumnTypeFromDDL extracts the column type from the original DDL text.
// Used as fallback when the ANTLR grammar doesn't capture the type (e.g., NVARCHAR(255)).
func extractColumnTypeFromDDL(ddl, colName string) string {
	upperDDL := strings.ToUpper(ddl)
	upperCol := strings.ToUpper(colName)

	// Try [colName] first (bracket-quoted)
	search := "[" + upperCol + "]"
	idx := strings.Index(upperDDL, search)
	if idx == -1 {
		search = upperCol
		idx = strings.Index(upperDDL, search)
	}
	if idx == -1 {
		return ""
	}

	rest := ddl[idx+len(search):]
	rest = strings.TrimSpace(rest)

	// Track parenthesis depth to handle types like NVARCHAR(255) or DECIMAL(10,2)
	parenDepth := 0
	end := len(rest)
	for i, ch := range rest {
		switch ch {
		case '(':
			parenDepth++
		case ')':
			if parenDepth == 0 {
				end = i
				goto done
			}
			parenDepth--
		case ',':
			if parenDepth == 0 {
				end = i
				goto done
			}
		default:
			if parenDepth == 0 {
				upper := strings.ToUpper(rest[i:])
				for _, kw := range []string{"NOT NULL", "NULL", "PRIMARY", "DEFAULT", "IDENTITY", "CONSTRAINT", "REFERENCES"} {
					if strings.HasPrefix(upper, kw) {
						end = i
						goto done
					}
				}
			}
		}
	}
done:
	typeStr := strings.TrimSpace(rest[:end])
	if typeStr == "" {
		return ""
	}
	return strings.ToUpper(typeStr)
}

// parseTypeString parses a SQL Server type string like "NVARCHAR(255)" or "DECIMAL(10,2)"
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
		p := strings.TrimSpace(parts[0])
		if p == "MAX" {
			length = -1 // -1 indicates MAX
		} else {
			fmt.Sscanf(p, "%d", &length)
		}
	}
	if len(parts) >= 2 {
		fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &scale)
	}

	return baseType, length, scale
}
