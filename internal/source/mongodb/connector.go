package mongodb

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/UFOXD/datastream/internal/offset"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/pingcap/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// Connector implements the source.Connector interface for MongoDB.
type Connector struct {
	config      *Config
	status      source.Status
	position    *event.Position
	events      chan *event.ChangeEvent
	errors      chan error
	stopCh      chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex
	schemaCache map[string]*event.TableInfo

	// MongoDB client
	client *mongo.Client

	// Change stream
	changeStream *mongo.ChangeStream

	// Resume token
	resumeToken bson.Raw

	// Offset storage
	offsetStorage offset.Storage
	taskID        string

	// Sync scope
	syncScope *source.SyncScope
}

// New creates a new MongoDB source connector.
func New() *Connector {
	return &Connector{
		status: source.Status{
			State:     source.StateUninitialized,
			Timestamp: time.Now().Format(time.RFC3339),
		},
		events:      make(chan *event.ChangeEvent, 1000),
		errors:      make(chan error, 100),
		stopCh:      make(chan struct{}),
		schemaCache: make(map[string]*event.TableInfo),
	}
}

// Name returns the connector name.
func (c *Connector) Name() string {
	return "mongodb"
}

// Initialize initializes the connector.
func (c *Connector) Initialize(ctx context.Context, config source.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Parse MongoDB-specific config
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	c.config = cfg
	c.syncScope = config.SyncScope
	c.status.State = source.StateInitializing
	c.status.Timestamp = time.Now().Format(time.RFC3339)

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

	// Initialize offset storage
	if config.Offset.Backend != "" {
		offsetCfg := &offset.Config{
			Backend:       config.Offset.Backend,
			Path:          config.Offset.Path,
			FlushInterval: config.Offset.FlushInterval,
		}
		storage, err := offset.NewStorage(offsetCfg)
		if err != nil {
			client.Disconnect(ctx)
			return fmt.Errorf("failed to create offset storage: %w", err)
		}
		c.offsetStorage = storage
		c.taskID = config.Type

		// Load position from offset storage
		if pos, err := storage.Load(ctx, c.taskID); err != nil {
			log.Warn("failed to load position from offset storage",
				zap.String("taskId", c.taskID),
				zap.Error(err))
		} else if pos != nil && pos.Timestamp > 0 {
			c.position = pos
			// Resume token would be loaded here if stored
			log.Info("loaded position from offset storage",
				zap.Uint64("timestamp", pos.Timestamp))
		}
	}

	// Set resume token if configured
	if cfg.ResumeToken != "" {
		c.resumeToken = bson.Raw([]byte(cfg.ResumeToken))
	}

	c.status.State = source.StateStopped
	log.Info("MongoDB connector initialized",
		zap.Strings("hosts", cfg.Hosts),
		zap.String("database", cfg.Database))

	return nil
}

// Start starts the connector.
func (c *Connector) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.status.State == source.StateRunning {
		c.mu.Unlock()
		return source.ErrAlreadyRunning
	}
	c.status.State = source.StateRunning
	c.mu.Unlock()

	log.Info("starting MongoDB connector")

	// Build change stream pipeline
	pipeline := mongo.Pipeline{}

	// Build match stage for database/collection filtering
	if len(c.config.Databases) > 0 || len(c.config.Collections) > 0 {
		matchFilter := bson.D{}

		if len(c.config.Databases) > 0 {
			orFilters := bson.A{}
			for _, db := range c.config.Databases {
				orFilters = append(orFilters, bson.D{{"ns.db", db}})
			}
			matchFilter = append(matchFilter, bson.E{"$or", orFilters})
		}

		if len(matchFilter) > 0 {
			pipeline = append(pipeline, bson.D{{"$match", matchFilter}})
		}
	}

	// Set change stream options
	streamOpts := options.ChangeStream().
		SetFullDocument(options.FullDocument(c.config.FullDocument)).
		SetBatchSize(c.config.BatchSize).
		SetMaxAwaitTime(c.config.MaxAwaitTimeDuration())

	// Note: FullDocumentBeforeChange is only available in driver v2+
	// For v1, we rely on FullDocument setting only

	// Set resume token if available
	if c.resumeToken != nil {
		streamOpts.SetResumeAfter(c.resumeToken)
	}

	// Start change stream
	var stream *mongo.ChangeStream
	var err error

	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client == nil {
		c.mu.Lock()
		c.status.State = source.StateError
		c.status.Message = "client not initialized"
		c.mu.Unlock()
		return fmt.Errorf("client not initialized")
	}

	// Watch all databases (using client.Watch for cluster-wide)
	stream, err = client.Watch(ctx, pipeline, streamOpts)
	if err != nil {
		c.mu.Lock()
		c.status.State = source.StateError
		c.status.Message = err.Error()
		c.mu.Unlock()
		return fmt.Errorf("failed to start change stream: %w", err)
	}

	c.mu.Lock()
	c.changeStream = stream
	c.mu.Unlock()

	log.Info("started MongoDB change stream")

	// Start streaming in a goroutine
	c.wg.Add(1)
	go c.run(ctx)

	return nil
}

// Stop stops the connector.
func (c *Connector) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.status.State != source.StateRunning {
		c.mu.Unlock()
		return nil
	}
	c.status.State = source.StateStopped
	c.mu.Unlock()

	log.Info("stopping MongoDB connector")
	close(c.stopCh)

	// Close change stream
	c.mu.RLock()
	stream := c.changeStream
	c.mu.RUnlock()

	if stream != nil {
		stream.Close(ctx)
	}

	c.wg.Wait()

	// Disconnect MongoDB client
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()

	if client != nil {
		client.Disconnect(ctx)
	}

	// Save final position to offset storage
	if c.offsetStorage != nil && c.position != nil {
		if err := c.offsetStorage.Save(ctx, c.taskID, c.position); err != nil {
			log.Warn("failed to save final position to offset storage", zap.Error(err))
		}
	}

	// Close offset storage
	if c.offsetStorage != nil {
		c.offsetStorage.Close()
	}

	log.Info("MongoDB connector stopped")
	return nil
}

// Status returns the current status.
func (c *Connector) Status() source.Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// Events returns the events channel.
func (c *Connector) Events() <-chan *event.ChangeEvent {
	return c.events
}

// Errors returns the errors channel.
func (c *Connector) Errors() <-chan error {
	return c.errors
}

// GetPosition returns the current position.
func (c *Connector) GetPosition() *event.Position {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.position == nil {
		return nil
	}
	return c.position.Clone()
}

// SetPosition sets the starting position.
func (c *Connector) SetPosition(pos *event.Position) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.position = pos.Clone()

	// Save to offset storage
	if c.offsetStorage != nil {
		ctx := context.Background()
		if err := c.offsetStorage.Save(ctx, c.taskID, c.position); err != nil {
			log.Warn("failed to save position to offset storage", zap.Error(err))
		}
	}
	return nil
}

// GetSchema returns the schema for a collection.
func (c *Connector) GetSchema(database, table string) (*event.TableInfo, error) {
	c.mu.RLock()
	key := database + "." + table
	if schema, ok := c.schemaCache[key]; ok {
		c.mu.RUnlock()
		return schema.Clone(), nil
	}
	c.mu.RUnlock()

	// MongoDB is schemaless, return a basic table info
	info := &event.TableInfo{
		Database: database,
		Table:    table,
		Columns: []event.ColumnInfo{
			{Name: "_id", Type: "ObjectId", Nullable: false},
		},
		PrimaryKeyColumns: []string{"_id"},
	}

	// Cache it
	c.mu.Lock()
	c.schemaCache[key] = info
	c.mu.Unlock()

	return info, nil
}

// SyncScope returns the current sync scope.
func (c *Connector) SyncScope() *source.SyncScope {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.syncScope
}

// AddTables adds tables to sync (table-level only).
func (c *Connector) AddTables(ctx context.Context, tables []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
		return source.ErrInvalidSyncScope
	}
	for _, t := range tables {
		c.syncScope.Tables.Names = append(c.syncScope.Tables.Names, t)
	}
	return nil
}

// RemoveTables removes tables from sync (table-level only).
func (c *Connector) RemoveTables(ctx context.Context, tables []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
		return source.ErrInvalidSyncScope
	}
	remove := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		remove[t] = struct{}{}
	}
	names := c.syncScope.Tables.Names[:0]
	for _, n := range c.syncScope.Tables.Names {
		if _, ok := remove[n]; !ok {
			names = append(names, n)
		}
	}
	c.syncScope.Tables.Names = names
	return nil
}

// ListTables returns all tables being synced.
func (c *Connector) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.syncScope == nil || c.syncScope.Level != source.SyncLevelTable {
		return nil
	}
	result := make([]string, len(c.syncScope.Tables.Names))
	copy(result, c.syncScope.Tables.Names)
	return result
}

// shouldCapture checks if a namespace should be captured.
func (c *Connector) shouldCapture(database, collection string) bool {
	if len(c.config.Databases) == 0 && len(c.config.Collections) == 0 {
		return true
	}

	// Check databases
	for _, db := range c.config.Databases {
		if db == database || db == "*" {
			return true
		}
	}

	// Check collection patterns
	for db, pattern := range c.config.Collections {
		if db == database || db == "*" {
			if pattern == "*" || pattern == "" {
				return true
			}
			if matchPattern(pattern, collection) {
				return true
			}
		}
	}

	return false
}

// matchPattern performs simple pattern matching.
func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	return pattern == s
}

// run is the main event loop.
func (c *Connector) run(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Info("MongoDB connector context done")
			return
		case <-c.stopCh:
			log.Info("MongoDB connector stop signal received")
			return
		default:
			// Get change stream
			c.mu.RLock()
			stream := c.changeStream
			c.mu.RUnlock()

			if stream == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Check if stream has next
			if !stream.Next(ctx) {
				if err := stream.Err(); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Error("change stream error", zap.Error(err))
					c.sendError(err)
				}
				continue
			}

			// Get resume token
			c.mu.Lock()
			c.resumeToken = stream.ResumeToken()
			c.mu.Unlock()

			// Decode change event
			var changeDoc ChangeEventDocument
			if err := stream.Decode(&changeDoc); err != nil {
				log.Error("failed to decode change event", zap.Error(err))
				c.sendError(err)
				continue
			}

			// Convert to ChangeEvent
			evt, err := c.convertChangeEvent(&changeDoc)
			if err != nil {
				log.Error("failed to convert change event", zap.Error(err))
				c.sendError(err)
				continue
			}

			// Update position
			c.mu.Lock()
			c.position = &evt.Position
			c.mu.Unlock()

			// Send event
			select {
			case c.events <- evt:
			case <-time.After(time.Second * 5):
				log.Warn("event channel full, dropping event")
			}
		}
	}
}

// convertChangeEvent converts a MongoDB change document to a ChangeEvent.
func (c *Connector) convertChangeEvent(changeDoc *ChangeEventDocument) (*event.ChangeEvent, error) {
	// Build source info
	sourceInfo := event.SourceInfo{
		Connector: "mongodb",
		Database:  changeDoc.NS.DB,
	}

	// Determine event type
	var eventType event.EventType
	switch changeDoc.OperationType {
	case "insert":
		eventType = event.EventTypeInsert
	case "update", "replace":
		eventType = event.EventTypeUpdate
	case "delete":
		eventType = event.EventTypeDelete
	case "drop":
		eventType = event.EventTypeDDL
	case "dropDatabase":
		eventType = event.EventTypeDDL
	case "create":
		eventType = event.EventTypeDDL
	case "createIndexes":
		eventType = event.EventTypeDDL
	default:
		eventType = event.EventTypeInsert
	}

	// Build position
	position := event.Position{
		Timestamp:  uint64(changeDoc.ClusterTime.T),
		Order:      int(changeDoc.ClusterTime.I),
		CommitTime: time.Unix(int64(changeDoc.ClusterTime.T), 0),
	}

	// Build table info
	tableInfo := event.TableInfo{
		Database: changeDoc.NS.DB,
		Table:    changeDoc.NS.Coll,
		Columns: []event.ColumnInfo{
			{Name: "_id", Type: "ObjectId", Nullable: false},
		},
		PrimaryKeyColumns: []string{"_id"},
	}

	// Build row data
	var before, after event.RowData
	if changeDoc.FullDocumentBefore != nil {
		before = event.RowData{
			Fields: convertBSONToFields(changeDoc.FullDocumentBefore),
		}
	}
	if changeDoc.FullDocument != nil {
		after = event.RowData{
			Fields: convertBSONToFields(changeDoc.FullDocument),
		}
	}

	// Build event
	evt := &event.ChangeEvent{
		ID:        fmt.Sprintf("mongodb:%s:%d:%d", changeDoc.NS.DB, changeDoc.ClusterTime.T, changeDoc.ClusterTime.I),
		Source:    sourceInfo,
		Type:      eventType,
		Position:  position,
		Table:     tableInfo,
		Before:    before,
		After:     after,
		Timestamp: time.Unix(int64(changeDoc.ClusterTime.T), 0),
	}

	// Add document key to metadata
	if changeDoc.DocumentKey != nil {
		if evt.Metadata == nil {
			evt.Metadata = make(map[string]string)
		}
		// Extract _id from document key
		var key struct {
			ID interface{} `bson:"_id"`
		}
		if err := bson.Unmarshal(changeDoc.DocumentKey, &key); err == nil {
			evt.Metadata["_id"] = fmt.Sprintf("%v", key.ID)
		}
	}

	return evt, nil
}

// convertBSONToMap converts a BSON document to a map.
func convertBSONToMap(doc bson.Raw) map[string]interface{} {
	if doc == nil {
		return nil
	}

	var m map[string]interface{}
	if err := bson.Unmarshal(doc, &m); err != nil {
		return nil
	}
	return m
}

// convertBSONToFields converts a BSON document to event.Fields.
func convertBSONToFields(doc bson.Raw) map[string]event.Field {
	if doc == nil {
		return nil
	}

	m := make(map[string]event.Field)
	var rawMap map[string]interface{}
	if err := bson.Unmarshal(doc, &rawMap); err != nil {
		return nil
	}

	for k, v := range rawMap {
		m[k] = event.Field{
			Name:  k,
			Value: v,
			Type:  getFieldType(v),
			Null:  v == nil,
		}
	}
	return m
}

// getFieldType returns the type name for a value.
func getFieldType(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case string:
		return "string"
	case int, int32, int64:
		return "int"
	case float32, float64:
		return "double"
	case bool:
		return "bool"
	case primitive.Binary:
		return "binary"
	case primitive.ObjectID:
		return "objectId"
	case time.Time:
		return "date"
	case primitive.DateTime:
		return "date"
	default:
		return "object"
	}
}

// sendError sends an error to the errors channel.
func (c *Connector) sendError(err error) {
	select {
	case c.errors <- err:
	case <-time.After(time.Second * 5):
		log.Warn("failed to send error, channel full", zap.Error(err))
	}
}

func parseConfig(config source.Config) (*Config, error) {
	cfg := DefaultConfig()

	// Copy connection settings
	if len(config.Connection.Host) > 0 {
		cfg.Hosts = []string{config.Connection.Host}
	}
	if config.Connection.Port > 0 {
		// Update first host with port if needed
		if len(cfg.Hosts) > 0 {
			cfg.Hosts[0] = net.JoinHostPort(config.Connection.Host, strconv.Itoa(config.Connection.Port))
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
	if v, ok := config.Properties["authMechanism"].(string); ok {
		cfg.AuthMechanism = v
	}
	if v, ok := config.Properties["resumeToken"].(string); ok {
		cfg.ResumeToken = v
	}
	if v, ok := config.Properties["fullDocument"].(string); ok {
		cfg.FullDocument = v
	}
	if v, ok := config.Properties["fullDocumentBefore"].(string); ok {
		cfg.FullDocumentBefore = v
	}
	if v, ok := config.Properties["maxAwaitTime"].(float64); ok {
		cfg.MaxAwaitTime = int(v)
	}
	if v, ok := config.Properties["batchSize"].(float64); ok {
		cfg.BatchSize = int32(v)
	}
	if v, ok := config.Properties["sslMode"].(bool); ok {
		cfg.SSLMode = v
	}
	if v, ok := config.Properties["snapshotMode"].(string); ok {
		cfg.SnapshotMode = source.SnapshotMode(v)
	}

	// Copy databases/collections
	for _, tf := range config.Tables {
		cfg.Databases = append(cfg.Databases, tf.Database)
		for _, tbl := range tf.Tables {
			if cfg.Collections == nil {
				cfg.Collections = make(map[string]string)
			}
			cfg.Collections[tf.Database] = tbl
		}
	}

	// Copy offset settings
	cfg.OffsetBackend = config.Offset.Backend
	cfg.OffsetPath = config.Offset.Path
	cfg.OffsetFlushMs = config.Offset.FlushInterval

	return cfg, nil
}

func init() {
	source.Register("mongodb", &factory{})
}

type factory struct{}

func (f *factory) Create(config source.Config) (source.Connector, error) {
	conn := New()
	return conn, nil
}
