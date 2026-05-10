#!/bin/bash
# Generate ANTLR parsers for all supported databases
# Usage: ./scripts/generate-parsers.sh
#
# This script generates Go code from ANTLR grammar files for all supported databases.
# Grammar files are stored in pkg/parser/grammars/<database>/
# Base classes (for PostgreSQL and Oracle) are downloaded from grammars-v4 repository.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
GRAMMARS_DIR="$PROJECT_ROOT/pkg/parser/grammars"

echo "Generating ANTLR parsers..."

# Check if antlr is installed
if ! command -v antlr &> /dev/null; then
    echo "Error: antlr command not found. Please install ANTLR."
    echo "On macOS: brew install antlr"
    exit 1
fi

# Function to fix 'this' references in generated Go code
fix_this_references() {
    local file=$1
    # Replace 'this.' with appropriate receiver based on function context
    python3 << EOF
import re

with open('$file', 'r') as f:
    content = f.read()

lines = content.split('\n')
result = []
current_receiver = None
in_function = False
brace_count = 0

for line in lines:
    # Check for function start with receiver (l or p)
    match = re.match(r'func \(([lp]) \*\w+Lexer\)', line)
    if match:
        current_receiver = match.group(1)
        in_function = True
        brace_count = line.count('{') - line.count('}')
    elif match := re.match(r'func \(([lp]) \*\w+Parser\)', line):
        current_receiver = match.group(1)
        in_function = True
        brace_count = line.count('{') - line.count('}')
    elif re.match(r'func New\w+(Lexer|Parser)', line):
        current_receiver = 'this'  # local variable
        in_function = True
        brace_count = line.count('{') - line.count('}')

    if in_function:
        brace_count += line.count('{') - line.count('}')
        if brace_count <= 0:
            in_function = False
            current_receiver = None

    if current_receiver and current_receiver != 'this' and 'this.' in line:
        line = line.replace('this.', f'{current_receiver}.')

    result.append(line)

with open('$file', 'w') as f:
    f.write('\n'.join(result))
EOF
}

# Function to download base classes from grammars-v4
download_base_classes() {
    local tmp_dir=$(mktemp -d)
    echo "Downloading base classes from grammars-v4..."

    git clone --depth 1 --filter=blob:none --sparse https://github.com/antlr/grammars-v4.git "$tmp_dir" 2>/dev/null || true
    cd "$tmp_dir"
    git sparse-checkout set sql/postgresql/Go sql/plsql/Go 2>/dev/null || true

    POSTGRES_DIR="$PROJECT_ROOT/pkg/parser/postgres/generated"
    ORACLE_DIR="$PROJECT_ROOT/pkg/parser/oracle/generated"

    # Copy PostgreSQL base classes
    cp "$tmp_dir/sql/postgresql/Go/postgresql_lexer_base.go" "$POSTGRES_DIR/"
    cp "$tmp_dir/sql/postgresql/Go/postgresql_parser_base.go" "$POSTGRES_DIR/"
    cp "$tmp_dir/sql/postgresql/Go/string_stack.go" "$POSTGRES_DIR/"

    # Copy Oracle base classes
    cp "$tmp_dir/sql/plsql/Go/plsql_lexer_base.go" "$ORACLE_DIR/"
    cp "$tmp_dir/sql/plsql/Go/plsql_parser_base.go" "$ORACLE_DIR/"

    rm -rf "$tmp_dir"
    cd "$PROJECT_ROOT"
    echo "  Base classes downloaded."
}

# Generate MySQL parser
echo "Generating MySQL parser..."
MYSQL_DIR="$PROJECT_ROOT/pkg/parser/mysql/generated"
mkdir -p "$MYSQL_DIR"
antlr -Dlanguage=Go -visitor -package generated \
    -o "$MYSQL_DIR" \
    "$GRAMMARS_DIR/mysql/MySqlLexer.g4" \
    "$GRAMMARS_DIR/mysql/MySqlParser.g4"
echo "  MySQL parser generated."

# Generate PostgreSQL parser
echo "Generating PostgreSQL parser..."
POSTGRES_DIR="$PROJECT_ROOT/pkg/parser/postgres/generated"
mkdir -p "$POSTGRES_DIR"
antlr -Dlanguage=Go -visitor -package generated \
    -o "$POSTGRES_DIR" \
    "$GRAMMARS_DIR/postgres/PostgreSQLLexer.g4" \
    "$GRAMMARS_DIR/postgres/PostgreSQLParser.g4" 2>/dev/null || true

# Download and copy base classes
download_base_classes

# Fix package name in base classes
sed -i.bak 's/^package parser$/package generated/' "$POSTGRES_DIR/postgresql_lexer_base.go"
sed -i.bak 's/^package parser$/package generated/' "$POSTGRES_DIR/postgresql_parser_base.go"
sed -i.bak 's/^package parser$/package generated/' "$POSTGRES_DIR/string_stack.go"
rm -f "$POSTGRES_DIR"/*.bak

# Fix package name in Oracle base classes
sed -i.bak 's/^package parser$/package generated/' "$ORACLE_DIR/plsql_lexer_base.go"
sed -i.bak 's/^package parser$/package generated/' "$ORACLE_DIR/plsql_parser_base.go"
rm -f "$ORACLE_DIR"/*.bak

# Fix 'this' references
fix_this_references "$POSTGRES_DIR/postgresql_lexer.go"
fix_this_references "$POSTGRES_DIR/postgresql_parser.go"
echo "  PostgreSQL parser generated."

# Generate Oracle PL/SQL parser
echo "Generating Oracle PL/SQL parser..."
ORACLE_DIR="$PROJECT_ROOT/pkg/parser/oracle/generated"
mkdir -p "$ORACLE_DIR"
antlr -Dlanguage=Go -visitor -package generated \
    -o "$ORACLE_DIR" \
    "$GRAMMARS_DIR/oracle/PlSqlLexer.g4" \
    "$GRAMMARS_DIR/oracle/PlSqlParser.g4"

# Fix 'this' references
fix_this_references "$ORACLE_DIR/plsql_lexer.go"
fix_this_references "$ORACLE_DIR/plsql_parser.go"
echo "  Oracle PL/SQL parser generated."

# Generate SQL Server T-SQL parser
echo "Generating SQL Server T-SQL parser..."
SQLSERVER_DIR="$PROJECT_ROOT/pkg/parser/sqlserver/generated"
mkdir -p "$SQLSERVER_DIR"
antlr -Dlanguage=Go -visitor -package generated \
    -o "$SQLSERVER_DIR" \
    "$GRAMMARS_DIR/sqlserver/TSqlLexer.g4" \
    "$GRAMMARS_DIR/sqlserver/TSqlParser.g4"
echo "  SQL Server T-SQL parser generated."

echo ""
echo "All parsers generated successfully!"
echo ""
echo "Generated files:"
echo "  - MySQL:        $MYSQL_DIR"
echo "  - PostgreSQL:   $POSTGRES_DIR"
echo "  - Oracle:       $ORACLE_DIR"
echo "  - SQL Server:   $SQLSERVER_DIR"
