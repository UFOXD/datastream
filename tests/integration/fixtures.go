// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// TestConfig holds integration test configuration
type TestConfig struct {
	MySQLHost     string
	MySQLPort     string
	MySQLUser     string
	MySQLPassword string
	MySQLDatabase string

	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresDatabase string

	MongoDBHost     string
	MongoDBPort     string
	MongoDBUser     string
	MongoDBPassword string
	MongoDBDatabase string

	ElasticsearchHost string
	ElasticsearchPort string

	RedisHost string
	RedisPort string

	SQLServerHost     string
	SQLServerPort     string
	SQLServerUser     string
	SQLServerPassword string
	SQLServerDatabase string

	OracleHost        string
	OraclePort        string
	OracleUser        string
	OraclePassword    string
	OracleServiceName string

	KafkaBrokers string

	EtcdEndpoints string
}

// DefaultConfig returns default test configuration
func DefaultConfig() *TestConfig {
	return &TestConfig{
		MySQLHost:     getEnv("MYSQL_HOST", "localhost"),
		MySQLPort:     getEnv("MYSQL_PORT", "3306"),
		MySQLUser:     getEnv("MYSQL_USER", "datastream"),
		MySQLPassword: getEnv("MYSQL_PASSWORD", "datastream"),
		MySQLDatabase: getEnv("MYSQL_DATABASE", "datastream_test"),

		PostgresHost:     getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:     getEnv("POSTGRES_PORT", "5432"),
		PostgresUser:     getEnv("POSTGRES_USER", "datastream"),
		PostgresPassword: getEnv("POSTGRES_PASSWORD", "datastream"),
		PostgresDatabase: getEnv("POSTGRES_DATABASE", "datastream_test"),

		MongoDBHost:     getEnv("MONGODB_HOST", "localhost"),
		MongoDBPort:     getEnv("MONGODB_PORT", "27017"),
		MongoDBUser:     getEnv("MONGODB_USER", "datastream"),
		MongoDBPassword: getEnv("MONGODB_PASSWORD", "datastream"),
		MongoDBDatabase: getEnv("MONGODB_DATABASE", "datastream_test"),

		ElasticsearchHost: getEnv("ELASTICSEARCH_HOST", "localhost"),
		ElasticsearchPort: getEnv("ELASTICSEARCH_PORT", "9200"),

		RedisHost: getEnv("REDIS_HOST", "localhost"),
		RedisPort: getEnv("REDIS_PORT", "6379"),

		SQLServerHost:     getEnv("SQLSERVER_HOST", "localhost"),
		SQLServerPort:     getEnv("SQLSERVER_PORT", "1433"),
		SQLServerUser:     getEnv("SQLSERVER_USER", "sa"),
		SQLServerPassword: getEnv("SQLSERVER_PASSWORD", "Datastream123!"),
		SQLServerDatabase: getEnv("SQLSERVER_DATABASE", "datastream_test"),

		OracleHost:        getEnv("ORACLE_HOST", "localhost"),
		OraclePort:        getEnv("ORACLE_PORT", "1521"),
		OracleUser:        getEnv("ORACLE_USER", "system"),
		OraclePassword:    getEnv("ORACLE_PASSWORD", "oracle"),
		OracleServiceName: getEnv("ORACLE_SERVICE_NAME", "XE"),

		KafkaBrokers:  getEnv("KAFKA_BROKERS", "localhost:9093"),
		EtcdEndpoints: getEnv("ETCD_ENDPOINTS", "localhost:2379"),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// MySQLDSN returns MySQL connection string
func (c *TestConfig) MySQLDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		c.MySQLUser, c.MySQLPassword, c.MySQLHost, c.MySQLPort, c.MySQLDatabase)
}

// PostgresDSN returns PostgreSQL connection string
func (c *TestConfig) PostgresDSN() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.PostgresHost, c.PostgresPort, c.PostgresUser, c.PostgresPassword, c.PostgresDatabase)
}

// MongoDBURI returns MongoDB connection URI
func (c *TestConfig) MongoDBURI() string {
	return fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?authSource=admin",
		c.MongoDBUser, c.MongoDBPassword, c.MongoDBHost, c.MongoDBPort, c.MongoDBDatabase)
}

// ElasticsearchURL returns Elasticsearch URL
func (c *TestConfig) ElasticsearchURL() string {
	return fmt.Sprintf("http://%s:%s", c.ElasticsearchHost, c.ElasticsearchPort)
}

// RedisAddr returns Redis address
func (c *TestConfig) RedisAddr() string {
	return fmt.Sprintf("%s:%s", c.RedisHost, c.RedisPort)
}

// SQLServerDSN returns SQL Server connection string
func (c *TestConfig) SQLServerDSN() string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
		c.SQLServerUser, c.SQLServerPassword, c.SQLServerHost, c.SQLServerPort, c.SQLServerDatabase)
}

// OracleDSN returns Oracle connection string
func (c *TestConfig) OracleDSN() string {
	return fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		c.OracleUser, c.OraclePassword, c.OracleHost, c.OraclePort, c.OracleServiceName)
}

// TestFixture manages test fixtures and cleanup
type TestFixture struct {
	Config  *TestConfig
	t       *testing.T
	cleanup []func()
}

// NewTestFixture creates a new test fixture
func NewTestFixture(t *testing.T) *TestFixture {
	return &TestFixture{
		Config:  DefaultConfig(),
		t:       t,
		cleanup: make([]func(), 0),
	}
}

// Cleanup runs all cleanup functions
func (f *TestFixture) Cleanup() {
	for i := len(f.cleanup) - 1; i >= 0; i-- {
		f.cleanup[i]()
	}
}

// WaitForMySQL waits for MySQL to be ready
func (f *TestFixture) WaitForMySQL(ctx context.Context) error {
	return f.waitForDB(ctx, "mysql", f.Config.MySQLDSN())
}

// WaitForPostgres waits for PostgreSQL to be ready
func (f *TestFixture) WaitForPostgres(ctx context.Context) error {
	return f.waitForDB(ctx, "postgres", f.Config.PostgresDSN())
}

func (f *TestFixture) waitForDB(ctx context.Context, driver, dsn string) error {
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for %s", driver)
		case <-ticker.C:
			db, err := sql.Open(driver, dsn)
			if err != nil {
				continue
			}
			if err := db.Ping(); err != nil {
				db.Close()
				continue
			}
			db.Close()
			return nil
		}
	}
}

// SetupMySQLTable creates a test table in MySQL
func (f *TestFixture) SetupMySQLTable(tableName string, schema string) error {
	db, err := sql.Open("mysql", f.Config.MySQLDSN())
	if err != nil {
		return err
	}
	defer db.Close()

	// Drop table if exists
	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create table
	_, err = db.Exec(schema)
	if err != nil {
		return err
	}

	f.cleanup = append(f.cleanup, func() {
		db, _ := sql.Open("mysql", f.Config.MySQLDSN())
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		db.Close()
	})

	return nil
}

// SetupPostgresTable creates a test table in PostgreSQL
func (f *TestFixture) SetupPostgresTable(tableName string, schema string) error {
	db, err := sql.Open("postgres", f.Config.PostgresDSN())
	if err != nil {
		return err
	}
	defer db.Close()

	// Drop table if exists
	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Create table
	_, err = db.Exec(schema)
	if err != nil {
		return err
	}

	f.cleanup = append(f.cleanup, func() {
		db, _ := sql.Open("postgres", f.Config.PostgresDSN())
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		db.Close()
	})

	return nil
}

// InsertMySQL inserts a row into MySQL
func (f *TestFixture) InsertMySQL(table string, columns string, values string) error {
	db, err := sql.Open("mysql", f.Config.MySQLDSN())
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columns, values))
	return err
}

// InsertPostgres inserts a row into PostgreSQL
func (f *TestFixture) InsertPostgres(table string, columns string, values string) error {
	db, err := sql.Open("postgres", f.Config.PostgresDSN())
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columns, values))
	return err
}
