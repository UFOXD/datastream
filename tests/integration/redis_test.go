//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRedisSinkIntegration tests Redis sink connector
func TestRedisSinkIntegration(t *testing.T) {
	cfg := DefaultConfig()

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr(),
	})

	// Wait for Redis to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for Redis")
		default:
			if err := client.Ping(ctx).Err(); err == nil {
				goto ready
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

ready:
	// Test string operations
	key := "test:string:key"
	value := "test_value"

	err := client.Set(ctx, key, value, time.Minute).Err()
	if err != nil {
		t.Fatalf("Failed to set string: %v", err)
	}

	got, err := client.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("Failed to get string: %v", err)
	}

	if got != value {
		t.Fatalf("Expected %s, got %s", value, got)
	}

	// Test hash operations
	hashKey := "test:hash:key"
	fields := map[string]interface{}{
		"field1": "value1",
		"field2": 42,
	}

	err = client.HSet(ctx, hashKey, fields).Err()
	if err != nil {
		t.Fatalf("Failed to set hash: %v", err)
	}

	gotFields, err := client.HGetAll(ctx, hashKey).Result()
	if err != nil {
		t.Fatalf("Failed to get hash: %v", err)
	}

	if gotFields["field1"] != "value1" {
		t.Fatalf("Expected field1=value1, got %s", gotFields["field1"])
	}

	// Cleanup
	client.Del(ctx, key, hashKey)

	t.Log("Redis sink integration test passed")
}
