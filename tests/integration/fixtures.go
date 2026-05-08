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
