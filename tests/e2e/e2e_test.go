// +build e2e

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// E2EConfig holds e2e test configuration
type E2EConfig struct {
	APIAddr      string
	MySQLDSN     string
	PostgresDSN  string
	KafkaBrokers string
}

// DefaultE2EConfig returns default e2e configuration
func DefaultE2EConfig() *E2EConfig {
	return &E2EConfig{
		APIAddr:      getEnv("API_ADDR", "http://localhost:8300"),
		MySQLDSN:     getEnv("MYSQL_DSN", "datastream:datastream@tcp(localhost:3306)/datastream_test"),
		PostgresDSN:  getEnv("POSTGRES_DSN", "host=localhost port=5432 user=datastream password=datastream dbname=datastream_test sslmode=disable"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9093"),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// TestE2EHealthCheck tests the health endpoint
func TestE2EHealthCheck(t *testing.T) {
	cfg := DefaultE2EConfig()

	resp, err := http.Get(cfg.APIAddr + "/health")
	if err != nil {
		t.Fatalf("Failed to call health endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result["status"] != "healthy" {
		t.Fatalf("Expected status healthy, got %v", result["status"])
	}

	t.Log("Health check passed")
}

// TestE2ECreateAndListTasks tests task creation and listing
func TestE2ECreateAndListTasks(t *testing.T) {
	cfg := DefaultE2EConfig()

	// Create task
	taskID := fmt.Sprintf("e2e-task-%d", time.Now().UnixNano())
	createReq := map[string]interface{}{
		"id":   taskID,
		"name": "E2E Test Task",
		"config": map[string]interface{}{
			"sourceType": "mysql",
			"sinkType":   "kafka",
			"batchSize":  100,
		},
	}

	resp, err := http.Post(cfg.APIAddr+"/api/v1/tasks", "application/json", mustMarshal(createReq))
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Expected status 201, got %d", resp.StatusCode)
	}

	// List tasks
	resp, err = http.Get(cfg.APIAddr + "/api/v1/tasks")
	if err != nil {
		t.Fatalf("Failed to list tasks: %v", err)
	}
	defer resp.Body.Close()

	var listResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&listResult); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	tasks := listResult["tasks"].([]interface{})
	found := false
	for _, task := range tasks {
		taskMap := task.(map[string]interface{})
		if taskMap["id"] == taskID {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("Created task %s not found in list", taskID)
	}

	// Cleanup - delete task
	deleteURL := fmt.Sprintf("%s/api/v1/tasks/%s", cfg.APIAddr, taskID)
	req, _ := http.NewRequest(http.MethodDelete, deleteURL, nil)
	_, _ = http.DefaultClient.Do(req)

	t.Log("Create and list tasks test passed")
}

// TestE2EMySQLSourceIntegration tests MySQL source with real database
func TestE2EMySQLSourceIntegration(t *testing.T) {
	cfg := DefaultE2EConfig()

	// Connect to MySQL
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer db.Close()

	// Wait for MySQL to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for MySQL")
		default:
			if err := db.Ping(); err == nil {
				goto ready
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

ready:
	// Create test table
	tableName := fmt.Sprintf("e2e_test_%d", time.Now().UnixNano())
	createTable := fmt.Sprintf(`
		CREATE TABLE %s (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			value INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName)

	if _, err := db.Exec(createTable); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert test data
	for i := 1; i <= 10; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (name, value) VALUES (?, ?)", tableName), fmt.Sprintf("item-%d", i), i*10)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Verify data
	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 10 {
		t.Fatalf("Expected 10 rows, got %d", count)
	}

	t.Log("MySQL source integration test passed")
}

// TestE2EPostgresSourceIntegration tests PostgreSQL source with real database
func TestE2EPostgresSourceIntegration(t *testing.T) {
	cfg := DefaultE2EConfig()

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Wait for PostgreSQL to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for PostgreSQL")
		default:
			if err := db.Ping(); err == nil {
				goto ready
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

ready:
	// Create test table
	tableName := fmt.Sprintf("e2e_test_%d", time.Now().UnixNano())
	createTable := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			value INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, tableName)

	if _, err := db.Exec(createTable); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

	// Insert test data
	for i := 1; i <= 10; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (name, value) VALUES ($1, $2)", tableName), fmt.Sprintf("item-%d", i), i*10)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Verify data
	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 10 {
		t.Fatalf("Expected 10 rows, got %d", count)
	}

	t.Log("PostgreSQL source integration test passed")
}

// TestE2ETaskLifecycle tests the full task lifecycle
func TestE2ETaskLifecycle(t *testing.T) {
	cfg := DefaultE2EConfig()

	taskID := fmt.Sprintf("lifecycle-task-%d", time.Now().UnixNano())

	// Step 1: Create task
	createReq := map[string]interface{}{
		"id":   taskID,
		"name": "Lifecycle Test Task",
		"config": map[string]interface{}{
			"sourceType": "mysql",
			"sinkType":   "kafka",
		},
	}

	resp, err := http.Post(cfg.APIAddr+"/api/v1/tasks", "application/json", mustMarshal(createReq))
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	resp.Body.Close()

	// Step 2: Get task (should be stopped)
	resp, err = http.Get(fmt.Sprintf("%s/api/v1/tasks/%s", cfg.APIAddr, taskID))
	if err != nil {
		t.Fatalf("Failed to get task: %v", err)
	}

	var task map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&task)
	resp.Body.Close()

	t.Logf("Task initial status: %v", task["status"])

	// Step 3: Start task
	resp, err = http.Post(fmt.Sprintf("%s/api/v1/tasks/%s/start", cfg.APIAddr, taskID), "application/json", nil)
	if err != nil {
		t.Fatalf("Failed to start task: %v", err)
	}
	resp.Body.Close()

	// Step 4: Get position
	resp, err = http.Get(fmt.Sprintf("%s/api/v1/tasks/%s/position", cfg.APIAddr, taskID))
	if err != nil {
		t.Fatalf("Failed to get position: %v", err)
	}
	resp.Body.Close()

	// Step 5: Stop task
	resp, err = http.Post(fmt.Sprintf("%s/api/v1/tasks/%s/stop", cfg.APIAddr, taskID), "application/json", nil)
	if err != nil {
		t.Fatalf("Failed to stop task: %v", err)
	}
	resp.Body.Close()

	// Step 6: Delete task
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/api/v1/tasks/%s", cfg.APIAddr, taskID), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete task: %v", err)
	}
	resp.Body.Close()

	t.Log("Task lifecycle test passed")
}

// TestE2ENodesList tests listing nodes
func TestE2ENodesList(t *testing.T) {
	cfg := DefaultE2EConfig()

	resp, err := http.Get(cfg.APIAddr + "/api/v1/nodes")
	if err != nil {
		t.Fatalf("Failed to list nodes: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// Should have nodes array
	nodes, ok := result["nodes"].([]interface{})
	if !ok {
		t.Fatal("Expected nodes array in response")
	}

	t.Logf("Found %d nodes", len(nodes))
	t.Log("Nodes list test passed")
}

func mustMarshal(v interface{}) *mustMarshalReader {
	data, _ := json.Marshal(v)
	return &mustMarshalReader{data: data}
}

type mustMarshalReader struct {
	data []byte
}

func (r *mustMarshalReader) Read(p []byte) (n int, err error) {
	copy(p, r.data)
	return len(r.data), nil
}
