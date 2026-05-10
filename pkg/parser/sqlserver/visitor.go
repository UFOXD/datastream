package sqlserver

import (
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
	for _, batch := range ctx.AllBatch() {
		result := v.VisitBatch(batch.(*generated.BatchContext))
		if result != nil {
			return result
		}
	}
	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: v.ddl,
	}
}

// VisitBatch visits a batch node.
func (v *DDLVisitor) VisitBatch(ctx *generated.BatchContext) interface{} {
	for _, stmt := range ctx.AllSql_clauses() {
		result := v.VisitSql_clauses(stmt.(*generated.Sql_clausesContext))
		if result != nil {
			return result
		}
	}
	return nil
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

	result.TableChanges = &parser.TableChanges{
		Operation: parser.TableOpCreate,
		Table: &parser.TableInfo{
			Database: result.Database,
			Name:     result.Table,
		},
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

	// Extract table name
	if tableName := ctx.Table_name(0); tableName != nil {
		schema, table := extractTableRef(tableName)
		result.Database = schema
		result.Table = table
		result.TableChanges.Table = &parser.TableInfo{
			Database: schema,
			Name:     table,
		}
	}

	// Process ALTER actions from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)

	// ADD COLUMN
	if strings.Contains(upperText, "ADD") {
		colName := extractColName(text, "ADD")
		if colName != "" {
			col := &parser.ColumnInfo{
				Name:     colName,
				Nullable: true,
			}
			result.TableChanges.AddedColumns = append(result.TableChanges.AddedColumns, *col)
		}
	}

	// DROP COLUMN
	if strings.Contains(upperText, "DROPCOLUMN") || strings.Contains(upperText, "DROP COLUMN") {
		colName := extractColName(text, "COLUMN")
		if colName != "" {
			result.TableChanges.DroppedColumns = append(result.TableChanges.DroppedColumns, colName)
		}
	}

	// ALTER COLUMN
	if strings.Contains(upperText, "ALTERCOLUMN") || strings.Contains(upperText, "ALTER COLUMN") {
		colName := extractColName(text, "COLUMN")
		if colName != "" {
			newCol := &parser.ColumnInfo{
				Name: colName,
			}
			result.TableChanges.ModifiedColumns = append(result.TableChanges.ModifiedColumns, parser.ColumnModification{
				Old: parser.ColumnInfo{Name: newCol.Name},
				New: *newCol,
			})
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

	// Extract index name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "INDEX")
	if idx != -1 {
		rest := text[idx+5:]
		rest = strings.TrimSpace(rest)

		// Handle IF EXISTS
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
			rest = strings.TrimSpace(rest[9:])
		}

		parts := strings.Fields(rest)
		if len(parts) > 0 {
			name := strings.Trim(parts[0], "[]\"")
			// Handle schema.index format
			if strings.Contains(name, ".") {
				p := strings.SplitN(name, ".", 2)
				name = p[1]
			}
			result.IndexChanges = &parser.IndexChanges{
				IndexName: name,
				Operation: "drop",
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

	// Extract index name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "INDEX")
	if idx != -1 {
		rest := text[idx+5:]
		rest = strings.TrimSpace(rest)

		// Skip UNIQUE keyword if present
		if strings.HasPrefix(strings.ToUpper(rest), "UNIQUE") {
			rest = strings.TrimSpace(rest[6:])
		}

		// Skip CLUSTERED/NONCLUSTERED keywords if present
		if strings.HasPrefix(strings.ToUpper(rest), "CLUSTERED") {
			rest = strings.TrimSpace(rest[9:])
		}
		if strings.HasPrefix(strings.ToUpper(rest), "NONCLUSTERED") {
			rest = strings.TrimSpace(rest[12:])
		}

		parts := strings.Fields(rest)
		if len(parts) > 0 {
			name := strings.Trim(parts[0], "[]\"")
			result.IndexChanges = &parser.IndexChanges{
				IndexName: name,
				Operation: "create",
			}
		}
	}

	// Extract table name from ON clause
	if onClause := strings.Index(upperText, " ON "); onClause != -1 {
		rest := text[onClause+4:]
		rest = strings.TrimSpace(rest)
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			tableRef := strings.Trim(parts[0], "[]\"")
			if strings.Contains(tableRef, ".") {
				p := strings.SplitN(tableRef, ".", 2)
				result.Database = p[0]
				result.Table = p[1]
			} else {
				result.Table = tableRef
			}
			if result.IndexChanges != nil {
				result.IndexChanges.DatabaseName = result.Database
				result.IndexChanges.TableName = result.Table
			}
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

	// Extract database name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "DATABASE")
	if idx != -1 {
		rest := text[idx+8:]
		rest = strings.TrimSpace(rest)

		// Handle IF EXISTS
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
			rest = strings.TrimSpace(rest[9:])
		}

		parts := strings.Fields(rest)
		if len(parts) > 0 {
			result.Database = strings.Trim(parts[0], "[]\"")
		}
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
