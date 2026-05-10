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
	if stmtblock := ctx.Stmtblock(); stmtblock != nil {
		return v.VisitStmtblock(stmtblock.(*generated.StmtblockContext))
	}
	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: v.ddl,
	}
}

// VisitStmtblock visits statement block.
func (v *DDLVisitor) VisitStmtblock(ctx *generated.StmtblockContext) interface{} {
	if stmtmulti := ctx.Stmtmulti(); stmtmulti != nil {
		return v.VisitStmtmulti(stmtmulti.(*generated.StmtmultiContext))
	}
	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: v.ddl,
	}
}

// VisitStmtmulti visits multiple statements.
func (v *DDLVisitor) VisitStmtmulti(ctx *generated.StmtmultiContext) interface{} {
	for _, stmt := range ctx.AllStmt() {
		result := v.VisitStmt(stmt.(*generated.StmtContext))
		if result != nil {
			return result
		}
	}
	return &parser.DDLResult{
		Type:      parser.DDLTypeUnknown,
		Statement: v.ddl,
	}
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

	// Extract schema name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "SCHEMA")
	if idx != -1 {
		rest := text[idx+6:]
		rest = strings.TrimSpace(rest)
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			result.Database = strings.Trim(parts[0], "\"")
		}
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
	text := ctx.GetText()
	upperText := strings.ToUpper(text)

	// Determine what type of object is being dropped from the text
	// PostgreSQL DROP statement structure: DROP <TYPE> <name>
	if strings.Contains(upperText, "DROPTABLE") || (ctx.DROP() != nil && strings.Contains(upperText, " TABLE ")) {
		return v.handleDropTable(ctx)
	} else if strings.Contains(upperText, "DROPINDEX") || (ctx.DROP() != nil && strings.Contains(upperText, " INDEX ")) {
		return v.handleDropIndex(ctx)
	} else if strings.Contains(upperText, "DROPVIEW") || (ctx.DROP() != nil && strings.Contains(upperText, " VIEW ")) {
		return v.handleDropView(ctx)
	} else if ctx.DROP() != nil && strings.Contains(upperText, " SCHEMA ") {
		return v.handleDropSchema(ctx)
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

	// Extract table name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "TABLE")
	if idx != -1 {
		rest := text[idx+5:]
		rest = strings.TrimSpace(rest)

		// Handle IF EXISTS
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
			rest = strings.TrimSpace(rest[9:])
		}

		parts := strings.Fields(rest)
		if len(parts) > 0 {
			name := strings.Trim(parts[0], "\"")
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
			name := strings.Trim(parts[0], "\"")
			result.IndexChanges = &parser.IndexChanges{
				IndexName: name,
				Operation: "drop",
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

	// Extract view name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "VIEW")
	if idx != -1 {
		rest := text[idx+4:]
		rest = strings.TrimSpace(rest)

		// Handle IF EXISTS
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
			rest = strings.TrimSpace(rest[9:])
		}

		parts := strings.Fields(rest)
		if len(parts) > 0 {
			name := strings.Trim(parts[0], "\"")
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

	// Extract schema name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "SCHEMA")
	if idx != -1 {
		rest := text[idx+6:]
		rest = strings.TrimSpace(rest)

		// Handle IF EXISTS
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
			rest = strings.TrimSpace(rest[9:])
		}

		parts := strings.Fields(rest)
		if len(parts) > 0 {
			result.Database = strings.Trim(parts[0], "\"")
		}
	}

	return result
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
	text := ctx.GetText()
	upperText := strings.ToUpper(text)

	// ADD COLUMN
	if strings.Contains(upperText, "ADD") && strings.Contains(upperText, "COLUMN") {
		// Extract column name from text
		colName := extractColumnName(text, "COLUMN")
		if colName != "" {
			col := &parser.ColumnInfo{
				Name:     colName,
				Nullable: true,
			}
			changes.AddedColumns = append(changes.AddedColumns, *col)
		}
	}

	// DROP COLUMN
	if strings.Contains(upperText, "DROP") && strings.Contains(upperText, "COLUMN") {
		colName := extractColumnName(text, "COLUMN")
		if colName != "" {
			changes.DroppedColumns = append(changes.DroppedColumns, colName)
		}
	}

	// ALTER COLUMN / MODIFY COLUMN
	if strings.Contains(upperText, "ALTER") && strings.Contains(upperText, "COLUMN") {
		colName := extractColumnName(text, "COLUMN")
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

// VisitTruncatestmt handles TRUNCATE statement.
func (v *DDLVisitor) VisitTruncatestmt(ctx *generated.TruncatestmtContext) interface{} {
	result := &parser.DDLResult{
		Type:      parser.DDLTypeTruncate,
		Statement: v.ddl,
	}

	// Extract table name from text
	text := ctx.GetText()
	upperText := strings.ToUpper(text)
	idx := strings.Index(upperText, "TABLE")
	if idx != -1 {
		rest := text[idx+5:]
		rest = strings.TrimSpace(rest)
		parts := strings.Fields(rest)
		if len(parts) > 0 {
			name := strings.Trim(parts[0], "\"")
			if strings.Contains(name, ".") {
				p := strings.SplitN(name, ".", 2)
				result.Database = p[0]
				result.Table = p[1]
			} else {
				result.Table = name
			}
		}
	} else {
		// TRUNCATE without TABLE keyword
		idx = strings.Index(upperText, "TRUNCATE")
		if idx != -1 {
			rest := text[idx+8:]
			rest = strings.TrimSpace(rest)
			parts := strings.Fields(rest)
			if len(parts) > 0 {
				name := strings.Trim(parts[0], "\"")
				if strings.Contains(name, ".") {
					p := strings.SplitN(name, ".", 2)
					result.Database = p[0]
					result.Table = p[1]
				} else {
					result.Table = name
				}
			}
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
