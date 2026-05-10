package oracle

import (
	"strings"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/oracle/generated"
)

// DDLVisitor traverses the Oracle PL/SQL parse tree and extracts DDL information.
type DDLVisitor struct {
	*generated.BasePlSqlParserVisitor
	ddl string
}

// NewDDLVisitor creates a new DDL visitor.
func NewDDLVisitor() *DDLVisitor {
	return &DDLVisitor{
		BasePlSqlParserVisitor: &generated.BasePlSqlParserVisitor{},
	}
}

// VisitSql_script visits the root node.
func (v *DDLVisitor) VisitSql_script(ctx *generated.Sql_scriptContext) interface{} {
	v.ddl = ctx.GetText()
	for _, stmt := range ctx.AllUnit_statement() {
		result := v.VisitUnit_statement(stmt.(*generated.Unit_statementContext))
		if result != nil {
			return result
		}
	}
	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: v.ddl,
	}
}

// VisitUnit_statement visits a unit statement node.
func (v *DDLVisitor) VisitUnit_statement(ctx *generated.Unit_statementContext) interface{} {
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

	return nil
}

// VisitCreate_table handles CREATE TABLE statement.
func (v *DDLVisitor) VisitCreate_table(ctx *generated.Create_tableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateTable,
		Statement: v.ddl,
	}

	// Extract table name
	if tableName := ctx.Tableview_name(); tableName != nil {
		schema, table := extractTableviewName(tableName)
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
	if tableName := ctx.Tableview_name(); tableName != nil {
		schema, table := extractTableviewName(tableName)
		result.Database = schema
		result.Table = table
		result.TableChanges.Table = &parser.TableInfo{
			Database: schema,
			Name:     table,
		}
	}

	// Process ALTER properties
	if properties := ctx.Alter_table_properties(); properties != nil {
		v.processAlterProperties(properties.(*generated.Alter_table_propertiesContext), result.TableChanges)
	}

	return result
}

func (v *DDLVisitor) processAlterProperties(ctx *generated.Alter_table_propertiesContext, changes *parser.TableChanges) {
	text := ctx.GetText()
	upperText := strings.ToUpper(text)

	// ADD COLUMN
	if strings.Contains(upperText, "ADD") {
		// Extract column name from text
		colName := extractColumnName(text, "ADD")
		if colName != "" {
			col := &parser.ColumnInfo{
				Name:     colName,
				Nullable: true,
			}
			changes.AddedColumns = append(changes.AddedColumns, *col)
		}
	}

	// DROP COLUMN
	if strings.Contains(upperText, "DROPCOLUMN") || strings.Contains(upperText, "DROP COLUMN") {
		colName := extractColumnName(text, "COLUMN")
		if colName != "" {
			changes.DroppedColumns = append(changes.DroppedColumns, colName)
		}
	}

	// MODIFY COLUMN
	if strings.Contains(upperText, "MODIFY") {
		colName := extractColumnName(text, "MODIFY")
		if colName != "" {
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

// VisitDrop_table handles DROP TABLE statement.
func (v *DDLVisitor) VisitDrop_table(ctx *generated.Drop_tableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropTable,
		Statement: v.ddl,
	}

	// Extract table name
	if tableName := ctx.Tableview_name(0); tableName != nil {
		schema, table := extractTableviewName(tableName)
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
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			name := strings.Trim(parts[0], "\"")
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

	// Extract view name
	if viewName := ctx.Tableview_name(); viewName != nil {
		schema, view := extractTableviewName(viewName)
		result.Database = schema
		result.Table = view
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
	if tableName := ctx.Tableview_name(); tableName != nil {
		schema, table := extractTableviewName(tableName)
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

		// Skip BITMAP keyword if present
		if strings.HasPrefix(strings.ToUpper(rest), "BITMAP") {
			rest = strings.TrimSpace(rest[6:])
		}

		parts := strings.Fields(rest)
		if len(parts) > 0 {
			name := strings.Trim(parts[0], "\"")
			result.IndexChanges = &parser.IndexChanges{
				IndexName: name,
				Operation: "create",
			}
		}
	}

	// Extract table name from table_index_clause
	if tableClause := ctx.Table_index_clause(); tableClause != nil {
		if tableName := tableClause.(*generated.Table_index_clauseContext).Tableview_name(); tableName != nil {
			schema, table := extractTableviewName(tableName)
			result.Database = schema
			result.Table = table
			if result.IndexChanges != nil {
				result.IndexChanges.DatabaseName = schema
				result.IndexChanges.TableName = table
			}
		}
	}

	return result
}

// Helper functions

func extractTableviewName(tableName generated.ITableview_nameContext) (schema, table string) {
	if tableName == nil {
		return "", ""
	}

	text := tableName.GetText()
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

	// Skip type keywords
	for _, skip := range []string{"COLUMN", "ADD", "DROP", "MODIFY", "SET", "TYPE"} {
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
