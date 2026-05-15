package mongodb

import (
	"testing"
	"time"

	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "default config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "missing hosts",
			config: &Config{
				Hosts: []string{},
			},
			wantErr: true,
		},
		{
			name: "valid config with all fields",
			config: &Config{
				Hosts:          []string{"localhost:27017"},
				User:           "test",
				Password:       "test",
				Database:       "testdb",
				ReplicaSet:     "rs0",
				MaxConnections: 10,
				FullDocument:   "updateLookup",
				MaxAwaitTime:   1000,
			},
			wantErr: false,
		},
		{
			name: "invalid fullDocument mode",
			config: &Config{
				Hosts:        []string{"localhost:27017"},
				FullDocument: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_ConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		contains []string
	}{
		{
			name: "basic connection string",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Database: "testdb",
			},
			contains: []string{"mongodb://localhost:27017/testdb"},
		},
		{
			name: "connection string with auth",
			config: &Config{
				Hosts:      []string{"localhost:27017"},
				User:       "testuser",
				Password:   "testpass",
				Database:   "testdb",
				AuthSource: "admin",
			},
			contains: []string{"mongodb://testuser:testpass@localhost:27017/testdb", "authSource=admin"},
		},
		{
			name: "connection string with replica set",
			config: &Config{
				Hosts:      []string{"host1:27017", "host2:27017"},
				ReplicaSet: "rs0",
				Database:   "testdb",
			},
			contains: []string{"mongodb://host1:27017,host2:27017/testdb", "replicaSet=rs0"},
		},
		{
			name: "connection string with SSL",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Database: "testdb",
				SSLMode:  true,
			},
			contains: []string{"tls=true"},
		},
		{
			name: "connection string with pool settings",
			config: &Config{
				Hosts:          []string{"localhost:27017"},
				Database:       "testdb",
				MaxConnections: 100,
				MaxIdle:        10,
			},
			contains: []string{"maxPoolSize=100", "minPoolSize=10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := tt.config.ConnectionString()
			for _, s := range tt.contains {
				assert.Contains(t, cs, s)
			}
		})
	}
}

func TestConfig_MaxAwaitTimeDuration(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected time.Duration
	}{
		{
			name:     "default",
			config:   &Config{MaxAwaitTime: 1000},
			expected: time.Second,
		},
		{
			name:     "custom value",
			config:   &Config{MaxAwaitTime: 5000},
			expected: 5 * time.Second,
		},
		{
			name:     "zero value",
			config:   &Config{MaxAwaitTime: 0},
			expected: time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.MaxAwaitTimeDuration())
		})
	}
}

func TestConnector_New(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn)
	assert.Equal(t, "mongodb", conn.Name())
	assert.Equal(t, source.StateUninitialized, conn.Status().State)
}

func TestConnector_Events(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn.Events())
}

func TestConnector_Errors(t *testing.T) {
	conn := New()
	assert.NotNil(t, conn.Errors())
}

func TestConnector_GetPosition(t *testing.T) {
	conn := New()

	// Initially nil
	pos := conn.GetPosition()
	assert.Nil(t, pos)

	// After setting
	conn.position = &event.Position{
		Timestamp:  12345,
		Order:      1,
		CommitTime: time.Now(),
	}
	pos = conn.GetPosition()
	assert.NotNil(t, pos)
	assert.Equal(t, uint64(12345), pos.Timestamp)
}

func TestConnector_SetPosition(t *testing.T) {
	conn := New()

	pos := &event.Position{
		Timestamp:  12345,
		Order:      1,
		CommitTime: time.Now(),
	}

	err := conn.SetPosition(pos)
	assert.NoError(t, err)

	// Verify position is cloned
	assert.Equal(t, uint64(12345), conn.position.Timestamp)
	pos.Timestamp = 99999
	assert.Equal(t, uint64(12345), conn.position.Timestamp)
}

func TestConnector_GetSchema(t *testing.T) {
	conn := New()

	schema, err := conn.GetSchema("testdb", "testcollection")
	assert.NoError(t, err)
	assert.NotNil(t, schema)
	assert.Equal(t, "testdb", schema.Database)
	assert.Equal(t, "testcollection", schema.Table)
	assert.Contains(t, schema.PrimaryKeyColumns, "_id")
}

func TestConnector_shouldCapture(t *testing.T) {
	tests := []struct {
		name        string
		databases   []string
		collections map[string]string
		db          string
		coll        string
		want        bool
	}{
		{
			name: "no filters - capture all",
			db:   "testdb",
			coll: "testcoll",
			want: true,
		},
		{
			name:      "database filter - match",
			databases: []string{"testdb"},
			db:        "testdb",
			coll:      "testcoll",
			want:      true,
		},
		{
			name:      "database filter - no match",
			databases: []string{"otherdb"},
			db:        "testdb",
			coll:      "testcoll",
			want:      false,
		},
		{
			name:        "collection filter - match",
			collections: map[string]string{"testdb": "testcoll"},
			db:          "testdb",
			coll:        "testcoll",
			want:        true,
		},
		{
			name:        "collection filter - wildcard",
			collections: map[string]string{"testdb": "*"},
			db:          "testdb",
			coll:        "anycoll",
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := New()
			conn.config = &Config{
				Databases:   tt.databases,
				Collections: tt.collections,
			}

			got := conn.shouldCapture(tt.db, tt.coll)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		want    bool
	}{
		{"*", "anything", true},
		{"test", "test", true},
		{"test", "other", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.s, func(t *testing.T) {
			assert.Equal(t, tt.want, matchPattern(tt.pattern, tt.s))
		})
	}
}

func TestChangeEventDocument_IsDDL(t *testing.T) {
	tests := []struct {
		opType string
		want   bool
	}{
		{"insert", false},
		{"update", false},
		{"delete", false},
		{"create", true},
		{"drop", true},
		{"createIndexes", true},
		{"dropIndexes", true},
	}

	for _, tt := range tests {
		t.Run(tt.opType, func(t *testing.T) {
			doc := &ChangeEventDocument{OperationType: tt.opType}
			assert.Equal(t, tt.want, doc.IsDDL())
		})
	}
}

func TestChangeEventDocument_IsData(t *testing.T) {
	tests := []struct {
		opType string
		want   bool
	}{
		{"insert", true},
		{"update", true},
		{"replace", true},
		{"delete", true},
		{"create", false},
		{"drop", false},
	}

	for _, tt := range tests {
		t.Run(tt.opType, func(t *testing.T) {
			doc := &ChangeEventDocument{OperationType: tt.opType}
			assert.Equal(t, tt.want, doc.IsData())
		})
	}
}

func TestChangeEventDocument_GetDocumentID(t *testing.T) {
	tests := []struct {
		name   string
		docKey bson.Raw
		want   interface{}
	}{
		{
			name:   "nil document key",
			docKey: nil,
			want:   nil,
		},
		{
			name: "valid document key with ObjectId",
			docKey: func() bson.Raw {
				doc, _ := bson.Marshal(bson.M{"_id": primitive.ObjectID{0x50, 0x7f, 0x1c, 0x86, 0x37, 0x2c, 0xa9, 0x46, 0x08, 0x9c, 0x59, 0x00}})
				return doc
			}(),
			want: "ObjectID(\"507f1c86372ca946089c5900\")",
		},
		{
			name: "valid document key with string ID",
			docKey: func() bson.Raw {
				doc, _ := bson.Marshal(bson.M{"_id": "string-id-123"})
				return doc
			}(),
			want: "string-id-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := &ChangeEventDocument{DocumentKey: tt.docKey}
			id := doc.GetDocumentID()
			if tt.want == nil {
				assert.Nil(t, id)
			} else {
				assert.NotNil(t, id)
			}
		})
	}
}

func TestChangeEventDocument_GetDatabase(t *testing.T) {
	doc := &ChangeEventDocument{
		NS: Namespace{DB: "testdb", Coll: "testcoll"},
	}
	assert.Equal(t, "testdb", doc.GetDatabase())
}

func TestChangeEventDocument_GetCollection(t *testing.T) {
	doc := &ChangeEventDocument{
		NS: Namespace{DB: "testdb", Coll: "testcoll"},
	}
	assert.Equal(t, "testcoll", doc.GetCollection())
}

func TestConvertBSONToMap(t *testing.T) {
	tests := []struct {
		name string
		doc  bson.Raw
		want map[string]interface{}
	}{
		{
			name: "nil document",
			doc:  nil,
			want: nil,
		},
		{
			name: "empty document",
			doc:  bson.Raw([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
			want: map[string]interface{}{},
		},
		{
			name: "document with fields",
			doc: func() bson.Raw {
				doc, _ := bson.Marshal(bson.M{"name": "test", "count": 42})
				return doc
			}(),
			want: map[string]interface{}{"name": "test", "count": int32(42)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertBSONToMap(tt.doc)
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  source.Config
		checks  func(t *testing.T, cfg *Config)
		wantErr bool
	}{
		{
			name: "basic config",
			config: source.Config{
				Connection: source.ConnectionConfig{
					Host:     "localhost",
					Port:     27017,
					User:     "testuser",
					Password: "testpass",
					Database: "testdb",
				},
			},
			checks: func(t *testing.T, cfg *Config) {
				assert.Contains(t, cfg.Hosts[0], "localhost:27017")
				assert.Equal(t, "testuser", cfg.User)
				assert.Equal(t, "testpass", cfg.Password)
				assert.Equal(t, "testdb", cfg.Database)
			},
			wantErr: false,
		},
		{
			name: "config with properties",
			config: source.Config{
				Connection: source.ConnectionConfig{
					Host:     "localhost",
					Port:     27017,
					Database: "testdb",
				},
				Properties: map[string]interface{}{
					"replicaSet":   "rs0",
					"fullDocument": "updateLookup",
					"maxAwaitTime": float64(5000),
					"sslMode":      true,
					"snapshotMode": "initial",
				},
			},
			checks: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "rs0", cfg.ReplicaSet)
				assert.Equal(t, "updateLookup", cfg.FullDocument)
				assert.Equal(t, 5000, cfg.MaxAwaitTime)
				assert.True(t, cfg.SSLMode)
				assert.Equal(t, source.SnapshotModeInitial, cfg.SnapshotMode)
			},
			wantErr: false,
		},
		{
			name: "config with hosts array",
			config: source.Config{
				Properties: map[string]interface{}{
					"hosts": []interface{}{"host1:27017", "host2:27017"},
				},
			},
			checks: func(t *testing.T, cfg *Config) {
				assert.Len(t, cfg.Hosts, 2)
				assert.Equal(t, "host1:27017", cfg.Hosts[0])
				assert.Equal(t, "host2:27017", cfg.Hosts[1])
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseConfig(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, cfg)
				tt.checks(t, cfg)
			}
		})
	}
}

func TestFactory_Create(t *testing.T) {
	f := &factory{}
	conn, err := f.Create(source.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, "mongodb", conn.Name())
}
