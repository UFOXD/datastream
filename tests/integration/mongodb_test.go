//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestMongoDBSourceIntegration tests MongoDB source connector
func TestMongoDBSourceIntegration(t *testing.T) {
	cfg := DefaultConfig()

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.MongoDBURI()))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	// Wait for MongoDB to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for MongoDB")
		default:
			if err := client.Ping(ctx, nil); err == nil {
				goto ready
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

ready:
	// Get collection
	db := client.Database(cfg.MongoDBDatabase)
	coll := db.Collection("test_integration")

	// Clean up
	coll.Drop(context.Background())

	// Insert test documents
	docs := []interface{}{
		bson.D{{Key: "name", Value: "doc1"}, {Key: "value", Value: 1}},
		bson.D{{Key: "name", Value: "doc2"}, {Key: "value", Value: 2}},
		bson.D{{Key: "name", Value: "doc3"}, {Key: "value", Value: 3}},
	}

	_, err = coll.InsertMany(context.Background(), docs)
	if err != nil {
		t.Fatalf("Failed to insert documents: %v", err)
	}

	// Verify documents
	count, err := coll.CountDocuments(context.Background(), bson.D{})
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	if count != 3 {
		t.Fatalf("Expected 3 documents, got %d", count)
	}

	// Clean up
	coll.Drop(context.Background())

	t.Log("MongoDB source integration test passed")
}

// TestMongoDBSinkIntegration tests MongoDB sink connector
func TestMongoDBSinkIntegration(t *testing.T) {
	cfg := DefaultConfig()

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.MongoDBURI()))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	// Wait for MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for MongoDB")
		default:
			if err := client.Ping(ctx, nil); err == nil {
				goto ready
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

ready:
	db := client.Database(cfg.MongoDBDatabase)
	coll := db.Collection("sink_test")

	// Clean up
	coll.Drop(context.Background())

	// Insert test document
	doc := bson.D{{Key: "name", Value: "sink_test"}, {Key: "value", Value: 100}}
	_, err = coll.InsertOne(context.Background(), doc)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// Verify
	var result bson.M
	err = coll.FindOne(context.Background(), bson.D{{Key: "name", Value: "sink_test"}}).Decode(&result)
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if result["value"].(int32) != 100 {
		t.Fatalf("Expected value 100, got %v", result["value"])
	}

	// Clean up
	coll.Drop(context.Background())

	t.Log("MongoDB sink integration test passed")
}
