package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashChunkerMySQL(t *testing.T) {
	c := NewHashChunker(4)
	sql := c.BuildChunkSQL("db1", "users", []string{"id"}, 0, "mysql")
	assert.Equal(t, "SELECT * FROM `db1`.`users` WHERE MOD(CRC32(CONCAT(`id`)), 4) = 0", sql)
}

func TestHashChunkerMySQLComposite(t *testing.T) {
	c := NewHashChunker(4)
	sql := c.BuildChunkSQL("db1", "orders", []string{"tenant_id", "order_id"}, 1, "mysql")
	assert.Equal(t, "SELECT * FROM `db1`.`orders` WHERE MOD(CRC32(CONCAT(`tenant_id`,`order_id`)), 4) = 1", sql)
}

func TestHashChunkerPostgres(t *testing.T) {
	c := NewHashChunker(4)
	sql := c.BuildChunkSQL("public", "users", []string{"id"}, 0, "postgres")
	assert.Equal(t, `SELECT * FROM "public"."users" WHERE MOD(hashtext("id"::text), 4) = 0`, sql)
}

func TestHashChunkerOracle(t *testing.T) {
	c := NewHashChunker(4)
	sql := c.BuildChunkSQL("HR", "EMP", []string{"ID"}, 0, "oracle")
	assert.Equal(t, `SELECT * FROM "HR"."EMP" WHERE MOD(ORA_HASH("ID"), 4) = 0`, sql)
}

func TestHashChunkerSQLServer(t *testing.T) {
	c := NewHashChunker(4)
	sql := c.BuildChunkSQL("dbo", "users", []string{"id"}, 0, "sqlserver")
	assert.Equal(t, `SELECT * FROM [dbo].[users] WHERE ABS(CHECKSUM([id])) % 4 = 0`, sql)
}

func TestHashChunkerWorkers(t *testing.T) {
	c := NewHashChunker(8)
	assert.Equal(t, 8, c.Workers())
}
