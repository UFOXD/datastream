// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdConnection(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fixture.Config.EtcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// Test put/get
	key := "/test/integration/connection"
	value := "test-value"

	_, err = cli.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	resp, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if len(resp.Kvs) == 0 {
		t.Fatal("Expected to find key in etcd")
	}

	if string(resp.Kvs[0].Value) != value {
		t.Fatalf("Expected value %s, got %s", value, string(resp.Kvs[0].Value))
	}

	// Cleanup
	_, _ = cli.Delete(ctx, key)

	t.Log("etcd connection successful")
}

func TestEtcdWatch(t *testing.T) {
	fixture := NewTestFixture(t)
	defer fixture.Cleanup()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fixture.Config.EtcdEndpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "/test/integration/watch"
	value := "watch-value"

	// Setup watch
	watchChan := cli.Watch(ctx, key)

	// Put value
	_, err = cli.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Wait for watch event
	select {
	case resp := <-watchChan:
		if resp.Err() != nil {
			t.Fatalf("Watch error: %v", resp.Err())
		}
		if len(resp.Events) == 0 {
			t.Fatal("Expected watch event")
		}
		if string(resp.Events[0].Kv.Value) != value {
			t.Fatalf("Expected value %s, got %s", value, string(resp.Events[0].Kv.Value))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for watch event")
	}

	// Cleanup
	_, _ = cli.Delete(ctx, key)

	t.Log("etcd watch successful")
}
