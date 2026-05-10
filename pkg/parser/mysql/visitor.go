package mysql

import (
	"strings"

	"github.com/UFOXD/datastream/pkg/parser"
	"github.com/UFOXD/datastream/pkg/parser/mysql/generated"
)

// DDLVisitor traverses the MySQL parse tree and extracts DDL information.
type DDLVisitor struct {
	*generated.BaseMySqlParserVisitor
	ddl string
}

// NewDDLVisitor creates a new DDL visitor.
func NewDDLVisitor() *DDLVisitor {
	return &DDLVisitor{
		BaseMySqlParserVisitor: &generated.BaseMySqlParserVisitor{},
	}
}

// VisitRoot visits the root node.
func (v *DDLVisitor) VisitRoot(ctx *generated.RootContext) interface{} {
	v.ddl = ctx.GetText()
	results := &parser.DDLResults{}

	if sqlStmts := ctx.SqlStatements(); sqlStmts != nil {
		stmtResults := v.VisitSqlStatements(sqlStmts.(*generated.SqlStatementsContext))
		if sr, ok := stmtResults.(*parser.DDLResults); ok {
			results.Results = append(results.Results, sr.Results...)
		}
	}

	return results
}

// VisitSqlStatements visits SQL statements.
func (v *DDLVisitor) VisitSqlStatements(ctx *generated.SqlStatementsContext) interface{} {
	results := &parser.DDLResults{}

	for _, stmt := range ctx.AllSqlStatement() {
		result := v.VisitSqlStatement(stmt.(*generated.SqlStatementContext))
		if ddlResult, ok := result.(*parser.DDLResult); ok && ddlResult != nil {
			results.Add(ddlResult)
		}
	}

	return results
}

// VisitSqlStatement visits a SQL statement node.
func (v *DDLVisitor) VisitSqlStatement(ctx *generated.SqlStatementContext) interface{} {
	// Check for DDL statement
	if ddl := ctx.DdlStatement(); ddl != nil {
		return v.visitDdlStatement(ddl.(*generated.DdlStatementContext))
	}
	return nil
}

// visitDdlStatement visits DDL statement.
func (v *DDLVisitor) visitDdlStatement(ctx *generated.DdlStatementContext) interface{} {
	// CREATE DATABASE
	if createDb := ctx.CreateDatabase(); createDb != nil {
		return v.VisitCreateDatabase(createDb.(*generated.CreateDatabaseContext))
	}

	// DROP DATABASE
	if dropDb := ctx.DropDatabase(); dropDb != nil {
		return v.VisitDropDatabase(dropDb.(*generated.DropDatabaseContext))
	}

	// ALTER DATABASE
	if alterDb := ctx.AlterDatabase(); alterDb != nil {
		return v.VisitAlterDatabase(alterDb.(*generated.AlterDatabaseContext))
	}

	// CREATE TABLE - use Accept to dispatch to correct visitor method
	if createTable := ctx.CreateTable(); createTable != nil {
		return createTable.Accept(v)
	}

	// DROP TABLE
	if dropTable := ctx.DropTable(); dropTable != nil {
		return v.VisitDropTable(dropTable.(*generated.DropTableContext))
	}

	// ALTER TABLE
	if alterTable := ctx.AlterTable(); alterTable != nil {
		return v.VisitAlterTable(alterTable.(*generated.AlterTableContext))
	}

	// CREATE INDEX
	if createIndex := ctx.CreateIndex(); createIndex != nil {
		return v.VisitCreateIndex(createIndex.(*generated.CreateIndexContext))
	}

	// DROP INDEX
	if dropIndex := ctx.DropIndex(); dropIndex != nil {
		return v.VisitDropIndex(dropIndex.(*generated.DropIndexContext))
	}

	// CREATE VIEW
	if createView := ctx.CreateView(); createView != nil {
		return v.VisitCreateView(createView.(*generated.CreateViewContext))
	}

	// DROP VIEW
	if dropView := ctx.DropView(); dropView != nil {
		return v.VisitDropView(dropView.(*generated.DropViewContext))
	}

	// TRUNCATE TABLE
	if truncateTable := ctx.TruncateTable(); truncateTable != nil {
		return v.VisitTruncateTable(truncateTable.(*generated.TruncateTableContext))
	}

	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: v.ddl,
	}
}

// VisitCreateDatabase handles CREATE DATABASE statement.
func (v *DDLVisitor) VisitCreateDatabase(ctx *generated.CreateDatabaseContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateDatabase,
		Statement: v.ddl,
	}

	if uid := ctx.Uid(); uid != nil {
		result.Database = v.extractUidText(uid)
	}

	return result
}

// VisitDropDatabase handles DROP DATABASE statement.
func (v *DDLVisitor) VisitDropDatabase(ctx *generated.DropDatabaseContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropDatabase,
		Statement: v.ddl,
	}

	if uid := ctx.Uid(); uid != nil {
		result.Database = v.extractUidText(uid)
	}

	return result
}

// VisitAlterDatabase handles ALTER DATABASE statement.
func (v *DDLVisitor) VisitAlterDatabase(ctx *generated.AlterDatabaseContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeAlterDatabase,
		Statement: v.ddl,
	}

	// AlterDatabaseContext is a base class, check for specific subtype
	// The database name is typically the first identifier after DATABASE/SCHEMA keyword
	text := ctx.GetText()
	upperText := strings.ToUpper(text)

	// Find DATABASE or SCHEMA keyword and get next identifier
	if idx := strings.Index(upperText, "DATABASE"); idx != -1 {
		rest := text[idx+8:]
		rest = strings.TrimSpace(rest)
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			result.Database = strings.Trim(parts[0], "`")
		}
	} else if idx := strings.Index(upperText, "SCHEMA"); idx != -1 {
		rest := text[idx+6:]
		rest = strings.TrimSpace(rest)
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			result.Database = strings.Trim(parts[0], "`")
		}
	}

	return result
}

// VisitColumnCreateTable handles CREATE TABLE with column definitions.
func (v *DDLVisitor) VisitColumnCreateTable(ctx *generated.ColumnCreateTableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateTable,
		Statement: v.ddl,
	}

	if tableName := ctx.TableName(); tableName != nil {
		db, table := v.extractTableName(tableName.(*generated.TableNameContext))
		result.Database = db
		result.Table = table
	}

	result.TableChanges = &parser.TableChanges{
		Operation: parser.TableOpCreate,
		Table: &parser.TableInfo{
			Database: result.Database,
			Name:     result.Table,
		},
	}

	if createDefinitions := ctx.CreateDefinitions(); createDefinitions != nil {
		v.extractColumns(createDefinitions.(*generated.CreateDefinitionsContext), result.TableChanges)
	}

	return result
}

// VisitCopyCreateTable handles CREATE TABLE ... LIKE.
func (v *DDLVisitor) VisitCopyCreateTable(ctx *generated.CopyCreateTableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateTable,
		Statement: v.ddl,
	}

	if tables := ctx.AllTableName(); len(tables) > 0 {
		db, table := v.extractTableName(tables[0].(*generated.TableNameContext))
		result.Database = db
		result.Table = table
	}

	return result
}

// VisitQueryCreateTable handles CREATE TABLE ... AS SELECT.
func (v *DDLVisitor) VisitQueryCreateTable(ctx *generated.QueryCreateTableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateTable,
		Statement: v.ddl,
	}

	if tableName := ctx.TableName(); tableName != nil {
		db, table := v.extractTableName(tableName.(*generated.TableNameContext))
		result.Database = db
		result.Table = table
	}

	return result
}

// extractColumns extracts column definitions.
func (v *DDLVisitor) extractColumns(ctx *generated.CreateDefinitionsContext, changes *parser.TableChanges) {
	for _, def := range ctx.AllCreateDefinition() {
		// Use Accept to dispatch to the correct visitor method
		if col, ok := def.Accept(v).(*parser.ColumnInfo); ok && col != nil && col.Name != "" {
			changes.Table.Columns = append(changes.Table.Columns, *col)
		}
	}
}

// VisitCreateDefinition handles a column definition.
func (v *DDLVisitor) VisitCreateDefinition(ctx *generated.CreateDefinitionContext) interface{} {
	col := &parser.ColumnInfo{
		Nullable: true,
	}

	// Check for column declaration
	text := ctx.GetText()

	// Parse column name and type from text
	parts := strings.Fields(text)
	if len(parts) >= 1 {
		col.Name = strings.Trim(parts[0], "`")
	}
	if len(parts) >= 2 {
		// Handle types with parameters like VARCHAR(255)
		col.Type = parts[1]
	}

	// Check for constraints
	upperText := strings.ToUpper(text)
	if strings.Contains(upperText, "NOTNULL") || strings.Contains(upperText, "PRIMARY") {
		col.Nullable = false
	}
	if strings.Contains(upperText, "AUTO_INCREMENT") {
		col.AutoIncrement = true
	}

	return col
}

// VisitDropTable handles DROP TABLE statement.
func (v *DDLVisitor) VisitDropTable(ctx *generated.DropTableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropTable,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get table names
	if tables := ctx.Tables(); tables != nil {
		if tableName := tables.TableName(0); tableName != nil {
			db, table := v.extractTableName(tableName.(*generated.TableNameContext))
			result.Database = db
			result.Table = table
		}
	}

	return result
}

// VisitAlterTable handles ALTER TABLE statement.
func (v *DDLVisitor) VisitAlterTable(ctx *generated.AlterTableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeAlterTable,
		Statement: v.ddl,
		TableChanges: &parser.TableChanges{
			Operation: parser.TableOpAlter,
		},
	}

	if tableName := ctx.TableName(); tableName != nil {
		db, table := v.extractTableName(tableName.(*generated.TableNameContext))
		result.Database = db
		result.Table = table
		result.TableChanges.Table = &parser.TableInfo{
			Database: db,
			Name:     table,
		}
	}

	// Process ALTER specifications using Accept to dispatch
	for _, spec := range ctx.AllAlterSpecification() {
		visitorResult := spec.Accept(v)
		if added, ok := visitorResult.(*alterResult); ok {
			if added.addedColumn != nil {
				result.TableChanges.AddedColumns = append(result.TableChanges.AddedColumns, *added.addedColumn)
			}
			if added.droppedColumn != "" {
				result.TableChanges.DroppedColumns = append(result.TableChanges.DroppedColumns, added.droppedColumn)
			}
			if added.modifiedColumn != nil {
				result.TableChanges.ModifiedColumns = append(result.TableChanges.ModifiedColumns, *added.modifiedColumn)
			}
		}
	}

	return result
}

// alterResult holds the result of processing an ALTER specification
type alterResult struct {
	addedColumn    *parser.ColumnInfo
	droppedColumn  string
	modifiedColumn *parser.ColumnModification
}

// VisitAlterByAddColumn handles ADD COLUMN specification (singular form).
func (v *DDLVisitor) VisitAlterByAddColumn(ctx *generated.AlterByAddColumnContext) interface{} {
	result := &alterResult{}

	if uids := ctx.AllUid(); len(uids) > 0 {
		col := &parser.ColumnInfo{
			Name:     v.extractUidText(uids[0]),
			Nullable: true,
		}
		if colDef := ctx.ColumnDefinition(); colDef != nil {
			col.Type = colDef.GetText()
		}
		result.addedColumn = col
	}

	return result
}

// VisitAlterByAddColumns handles ADD COLUMN specification (plural form).
func (v *DDLVisitor) VisitAlterByAddColumns(ctx *generated.AlterByAddColumnsContext) interface{} {
	result := &alterResult{}

	if uids := ctx.AllUid(); len(uids) > 0 {
		col := &parser.ColumnInfo{
			Name:     v.extractUidText(uids[0]),
			Nullable: true,
		}
		if colDefs := ctx.AllColumnDefinition(); len(colDefs) > 0 {
			col.Type = colDefs[0].GetText()
		}
		result.addedColumn = col
	}

	return result
}

// VisitAlterByDropColumn handles DROP COLUMN specification.
func (v *DDLVisitor) VisitAlterByDropColumn(ctx *generated.AlterByDropColumnContext) interface{} {
	result := &alterResult{}

	if uid := ctx.Uid(); uid != nil {
		result.droppedColumn = v.extractUidText(uid)
	}

	return result
}

// VisitAlterByModifyColumn handles MODIFY COLUMN specification.
func (v *DDLVisitor) VisitAlterByModifyColumn(ctx *generated.AlterByModifyColumnContext) interface{} {
	result := &alterResult{}

	if uids := ctx.AllUid(); len(uids) > 0 {
		newCol := &parser.ColumnInfo{
			Name:     v.extractUidText(uids[0]),
			Nullable: true,
		}
		if colDef := ctx.ColumnDefinition(); colDef != nil {
			newCol.Type = colDef.GetText()
		}
		result.modifiedColumn = &parser.ColumnModification{
			Old: parser.ColumnInfo{Name: newCol.Name},
			New: *newCol,
		}
	}

	return result
}

// VisitCreateIndex handles CREATE INDEX statement.
func (v *DDLVisitor) VisitCreateIndex(ctx *generated.CreateIndexContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateIndex,
		Statement: v.ddl,
	}

	if uid := ctx.Uid(); uid != nil {
		result.IndexChanges = &parser.IndexChanges{
			IndexName: v.extractUidText(uid),
			Operation: "create",
		}
	}

	if tableName := ctx.TableName(); tableName != nil {
		db, table := v.extractTableName(tableName.(*generated.TableNameContext))
		result.Database = db
		result.Table = table
		if result.IndexChanges != nil {
			result.IndexChanges.DatabaseName = db
			result.IndexChanges.TableName = table
		}
	}

	return result
}

// VisitDropIndex handles DROP INDEX statement.
func (v *DDLVisitor) VisitDropIndex(ctx *generated.DropIndexContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropIndex,
		Statement: v.ddl,
	}

	if uid := ctx.Uid(); uid != nil {
		result.IndexChanges = &parser.IndexChanges{
			IndexName: v.extractUidText(uid),
			Operation: "drop",
		}
	}

	if tableName := ctx.TableName(); tableName != nil {
		db, table := v.extractTableName(tableName.(*generated.TableNameContext))
		result.Database = db
		result.Table = table
		if result.IndexChanges != nil {
			result.IndexChanges.DatabaseName = db
			result.IndexChanges.TableName = table
		}
	}

	return result
}

// VisitCreateView handles CREATE VIEW statement.
func (v *DDLVisitor) VisitCreateView(ctx *generated.CreateViewContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeCreateView,
		Statement: v.ddl,
	}

	// Get view name from FullId
	if fullId := ctx.FullId(); fullId != nil {
		text := fullId.GetText()
		text = strings.ReplaceAll(text, "`", "")
		if strings.Contains(text, ".") {
			parts := strings.SplitN(text, ".", 2)
			result.Database = parts[0]
			result.Table = parts[1]
		} else {
			result.Table = text
		}
	}

	return result
}

// VisitDropView handles DROP VIEW statement.
func (v *DDLVisitor) VisitDropView(ctx *generated.DropViewContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeDropView,
		Statement: v.ddl,
	}

	// Use ANTLR context method to get view names via FullId
	if fullId := ctx.FullId(0); fullId != nil {
		text := fullId.GetText()
		text = strings.ReplaceAll(text, "`", "")
		if strings.Contains(text, ".") {
			parts := strings.SplitN(text, ".", 2)
			result.Database = parts[0]
			result.Table = parts[1]
		} else {
			result.Table = text
		}
	}

	return result
}

// VisitTruncateTable handles TRUNCATE TABLE statement.
func (v *DDLVisitor) VisitTruncateTable(ctx *generated.TruncateTableContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeTruncate,
		Statement: v.ddl,
	}

	if tableName := ctx.TableName(); tableName != nil {
		db, table := v.extractTableName(tableName.(*generated.TableNameContext))
		result.Database = db
		result.Table = table
	}

	return result
}

// extractUidText extracts text from UidContext.
func (v *DDLVisitor) extractUidText(uid generated.IUidContext) string {
	if uid == nil {
		return ""
	}
	text := uid.GetText()
	return strings.Trim(text, "`")
}

// extractTableName extracts database and table name.
func (v *DDLVisitor) extractTableName(ctx *generated.TableNameContext) (database, table string) {
	if ctx == nil {
		return "", ""
	}

	text := ctx.GetText()
	text = strings.ReplaceAll(text, "`", "")

	// Check if there's a dot (database.table format)
	if strings.Contains(text, ".") {
		parts := strings.SplitN(text, ".", 2)
		return parts[0], parts[1]
	}
	return "", text
}
