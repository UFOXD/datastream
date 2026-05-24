package lifecycle

import (
	"fmt"
	"strings"
)

type HashChunker struct {
	workers int
}

func NewHashChunker(workers int) *HashChunker {
	return &HashChunker{workers: workers}
}

func (c *HashChunker) Workers() int {
	return c.workers
}

func (c *HashChunker) BuildChunkSQL(schema, table string, pkCols []string, workerID int, dbType string) string {
	switch strings.ToLower(dbType) {
	case "mysql", "mariadb":
		return c.buildMySQL(schema, table, pkCols, workerID)
	case "postgres":
		return c.buildPostgres(schema, table, pkCols, workerID)
	case "oracle":
		return c.buildOracle(schema, table, pkCols, workerID)
	case "sqlserver":
		return c.buildSQLServer(schema, table, pkCols, workerID)
	default:
		return ""
	}
}

func (c *HashChunker) buildMySQL(schema, table string, pkCols []string, workerID int) string {
	quoted := make([]string, len(pkCols))
	for i, col := range pkCols {
		quoted[i] = "`" + col + "`"
	}
	concat := strings.Join(quoted, ",")
	return fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE MOD(CRC32(CONCAT(%s)), %d) = %d",
		schema, table, concat, c.workers, workerID)
}

func (c *HashChunker) buildPostgres(schema, table string, pkCols []string, workerID int) string {
	parts := make([]string, len(pkCols))
	for i, col := range pkCols {
		parts[i] = fmt.Sprintf(`"%s"::text`, col)
	}
	var hashExpr string
	if len(parts) == 1 {
		hashExpr = parts[0]
	} else {
		hashExpr = strings.Join(parts, " || ")
	}
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE MOD(hashtext(%s), %d) = %d`,
		schema, table, hashExpr, c.workers, workerID)
}

func (c *HashChunker) buildOracle(schema, table string, pkCols []string, workerID int) string {
	quoted := make([]string, len(pkCols))
	for i, col := range pkCols {
		quoted[i] = fmt.Sprintf(`"%s"`, col)
	}
	var hashExpr string
	if len(quoted) == 1 {
		hashExpr = quoted[0]
	} else {
		hashExpr = strings.Join(quoted, " || ")
	}
	return fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE MOD(ORA_HASH(%s), %d) = %d`,
		schema, table, hashExpr, c.workers, workerID)
}

func (c *HashChunker) buildSQLServer(schema, table string, pkCols []string, workerID int) string {
	quoted := make([]string, len(pkCols))
	for i, col := range pkCols {
		quoted[i] = "[" + col + "]"
	}
	checksum := strings.Join(quoted, ",")
	return fmt.Sprintf("SELECT * FROM [%s].[%s] WHERE ABS(CHECKSUM(%s)) %% %d = %d",
		schema, table, checksum, c.workers, workerID)
}
