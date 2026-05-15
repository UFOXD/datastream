package mongodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/sink"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// Connector implements the sink.Connector interface for MongoDB.
type Connector struct {
	config   *Config
	status   sink.Status
	position *event.Position
	client   *mongo.Client
	database *mongo.Database
	mu       sync.RWMutex
}

// New creates a new MongoDB sink connector.
func New() *Connector {
	return &Connector{
		status: sink.Status{
			State:     sink.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
}

// Name returns the connector name.
func (c *Connector) Name() string {
	return "mongodb"
}

// Initialize initializes the connector.
func (c *Connector) Initialize(ctx context.Context, config sink.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	c.config = cfg

	// Create MongoDB client
	clientOpts := options.Client().ApplyURI(cfg.ConnectionString())
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	c.client = client
	c.database = client.Database(cfg.Database)
	c.status.State = sink.StateReady
	c.status.Timestamp = time.Now().Format(time.RFC3339)

	log.Info("MongoDB sink initialized",
		zap.Strings("hosts", cfg.Hosts),
		zap.String("database", cfg.Database))
	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status.State == sink.StateWriting {
		return nil
	}

	if c.client == nil {
		return sink.ErrNotInitialized
	}

	c.status.State = sink.StateReady
	log.Info("MongoDB sink started")
	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status.State = sink.StateStopped

	if c.client != nil {
		c.client.Disconnect(ctx)
	}

	log.Info("MongoDB sink stopped")
	return nil
}

// Status returns the current status.
func (c *Connector) Status() sink.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Write writes events to MongoDB.
func (c *Connector) Write(ctx context.Context, events []*event.ChangeEvent) error {
	c.mu.Lock()
	c.status.State = sink.StateWriting
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.status.State = sink.StateReady
		c.mu.Unlock()
	}()

	// Group events by collection
	collectionEvents := make(map[string][]*event.ChangeEvent)
	for _, e := range events {
		coll := e.Table.Table
		collectionEvents[coll] = append(collectionEvents[coll], e)
	}

	// Process each collection
	for coll, collEvents := range collectionEvents {
		if err := c.writeToCollection(ctx, coll, collEvents); err != nil {
			c.mu.Lock()
			c.status.EventsFailed += int64(len(collEvents))
			c.mu.Unlock()
			return err
		}

		c.mu.Lock()
		c.status.EventsWritten += int64(len(collEvents))
		c.mu.Unlock()
	}

	// Update position to last event
	if len(events) > 0 {
		c.mu.Lock()
		c.position = &events[len(events)-1].Position
		c.mu.Unlock()
	}

	return nil
}

// writeToCollection writes events to a MongoDB collection.
func (c *Connector) writeToCollection(ctx context.Context, collection string, events []*event.ChangeEvent) error {
	coll := c.database.Collection(collection)

	// Build write models for bulk write
	writeModels := make([]mongo.WriteModel, 0, len(events))

	for _, e := range events {
		if e.IsDDL() {
			if err := c.handleDDL(e); err != nil {
				return err
			}
			continue
		}

		model, err := c.buildWriteModel(e)
		if err != nil {
			return err
		}

		if model != nil {
			writeModels = append(writeModels, model)
		}
	}

	if len(writeModels) == 0 {
		return nil
	}

	// Execute bulk write
	opts := options.BulkWrite().SetOrdered(c.config.Ordered)
	result, err := coll.BulkWrite(ctx, writeModels, opts)
	if err != nil {
		return fmt.Errorf("failed to write to collection %s: %w", collection, err)
	}

	log.Debug("wrote to MongoDB collection",
		zap.String("collection", collection),
		zap.Int64("inserted", result.InsertedCount),
		zap.Int64("updated", result.ModifiedCount),
		zap.Int64("upserted", result.UpsertedCount),
		zap.Int64("deleted", result.DeletedCount))

	return nil
}

// buildWriteModel builds a write model for an event.
func (c *Connector) buildWriteModel(e *event.ChangeEvent) (mongo.WriteModel, error) {
	switch e.Type {
	case event.EventTypeInsert:
		return c.buildInsertModel(e)
	case event.EventTypeUpdate:
		return c.buildUpdateModel(e)
	case event.EventTypeDelete:
		return c.buildDeleteModel(e)
	default:
		return nil, nil
	}
}

// buildInsertModel builds an insert model.
func (c *Connector) buildInsertModel(e *event.ChangeEvent) (mongo.WriteModel, error) {
	doc := c.buildDocument(e.After)

	switch c.config.WriteStrategy {
	case "insert":
		return mongo.NewInsertOneModel().SetDocument(doc), nil
	case "replace":
		id := c.getDocumentID(e)
		return mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": id}).
			SetReplacement(doc).
			SetUpsert(true), nil
	case "upsert":
		id := c.getDocumentID(e)
		return mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": id}).
			SetUpdate(bson.M{"$set": doc}).
			SetUpsert(true), nil
	default:
		return mongo.NewInsertOneModel().SetDocument(doc), nil
	}
}

// buildUpdateModel builds an update model.
func (c *Connector) buildUpdateModel(e *event.ChangeEvent) (mongo.WriteModel, error) {
	id := c.getDocumentID(e)
	doc := c.buildDocument(e.After)

	return mongo.NewUpdateOneModel().
		SetFilter(bson.M{"_id": id}).
		SetUpdate(bson.M{"$set": doc}), nil
}

// buildDeleteModel builds a delete model.
func (c *Connector) buildDeleteModel(e *event.ChangeEvent) (mongo.WriteModel, error) {
	id := c.getDocumentID(e)

	return mongo.NewDeleteOneModel().
		SetFilter(bson.M{"_id": id}), nil
}

// buildDocument builds a MongoDB document from RowData.
func (c *Connector) buildDocument(row event.RowData) bson.M {
	doc := make(bson.M)

	for name, field := range row.Fields {
		doc[name] = convertFieldToBSON(field.Value)
	}

	return doc
}

// getDocumentID gets the document ID from an event.
func (c *Connector) getDocumentID(e *event.ChangeEvent) interface{} {
	// Try to get _id from metadata
	if e.Metadata != nil {
		if idStr, ok := e.Metadata["_id"]; ok {
			// Try to parse as ObjectID
			if objID, err := primitive.ObjectIDFromHex(idStr); err == nil {
				return objID
			}
			return idStr
		}
	}

	// Try to get from row data
	if e.After.Fields != nil {
		if field, ok := e.After.GetField("_id"); ok {
			return convertFieldToBSON(field.Value)
		}
	}

	// Try to get from primary key columns
	if len(e.Table.PrimaryKeyColumns) > 0 {
		if field, ok := e.After.GetField(e.Table.PrimaryKeyColumns[0]); ok {
			return convertFieldToBSON(field.Value)
		}
	}

	// Generate a new ObjectID
	return primitive.NewObjectID()
}

// convertFieldToBSON converts a field value to BSON-compatible type.
func convertFieldToBSON(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case string, int, int32, int64, float32, float64, bool:
		return v
	case []byte:
		return primitive.Binary{Data: v}
	case time.Time:
		return primitive.NewDateTimeFromTime(v)
	case primitive.ObjectID:
		return v
	case primitive.DateTime:
		return v
	case primitive.Binary:
		return v
	default:
		return v
	}
}

// handleDDL handles DDL events.
func (c *Connector) handleDDL(e *event.ChangeEvent) error {
	switch c.config.DDLPolicy {
	case "ignore":
		return nil
	case "error":
		return sink.ErrDDLNotSupported
	}
	return nil
}

// Flush flushes any buffered data.
func (c *Connector) Flush(ctx context.Context) error {
	// MongoDB auto-flushes, no explicit flush needed
	log.Debug("MongoDB sink flushed")
	return nil
}

// GetPosition returns the last committed position.
func (c *Connector) GetPosition() *event.Position {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.position == nil {
		return nil
	}
	return c.position.Clone()
}

// SupportsDDL returns false (MongoDB handles schema differently).
func (c *Connector) SupportsDDL() bool {
	return false
}

// SupportsTransaction returns false (MongoDB uses bulk writes).
func (c *Connector) SupportsTransaction() bool {
	return false
}

func parseConfig(config sink.Config) (*Config, error) {
	cfg := DefaultConfig()

	// Copy connection settings
	if len(config.Connection.Host) > 0 {
		cfg.Hosts = []string{config.Connection.Host}
	}
	if config.Connection.Port > 0 {
		if len(cfg.Hosts) > 0 {
			cfg.Hosts[0] = fmt.Sprintf("%s:%d", config.Connection.Host, config.Connection.Port)
		}
	}
	cfg.User = config.Connection.User
	cfg.Password = config.Connection.Password
	cfg.Database = config.Connection.Database

	// Parse properties
	if v, ok := config.Properties["hosts"].([]interface{}); ok {
		cfg.Hosts = make([]string, 0, len(v))
		for _, host := range v {
			if h, ok := host.(string); ok {
				cfg.Hosts = append(cfg.Hosts, h)
			}
		}
	}
	if v, ok := config.Properties["replicaSet"].(string); ok {
		cfg.ReplicaSet = v
	}
	if v, ok := config.Properties["authSource"].(string); ok {
		cfg.AuthSource = v
	}
	if v, ok := config.Properties["writeStrategy"].(string); ok {
		cfg.WriteStrategy = v
	}
	if v, ok := config.Properties["writeConcern"].(string); ok {
		cfg.WriteConcern = v
	}
	if v, ok := config.Properties["ordered"].(bool); ok {
		cfg.Ordered = v
	}
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int(v)
	}
	if v, ok := config.Properties["sslMode"].(bool); ok {
		cfg.SSLMode = v
	}
	if v, ok := config.Properties["ddlPolicy"].(string); ok {
		cfg.DDLPolicy = v
	}

	cfg.Batch = config.Batch

	return cfg, nil
}

func init() {
	sink.Register("mongodb", &factory{})
}

type factory struct{}

func (f *factory) Create(config sink.Config) (sink.Connector, error) {
	return New(), nil
}
