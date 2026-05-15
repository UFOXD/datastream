package mongodb

import (
	"go.mongodb.org/mongo-driver/bson"
)

// ChangeEventDocument represents a MongoDB change stream event document.
type ChangeEventDocument struct {
	// _id is the document identifier (resume token)
	ID bson.Raw `bson:"_id"`

	// OperationType is the type of operation (insert, update, delete, replace, etc.)
	OperationType string `bson:"operationType"`

	// FullDocument is the complete document (for insert, update, replace)
	FullDocument bson.Raw `bson:"fullDocument,omitempty"`

	// FullDocumentBefore is the document before the change (when configured)
	FullDocumentBefore bson.Raw `bson:"fullDocumentBeforeChange,omitempty"`

	// NS is the namespace (database.collection)
	NS Namespace `bson:"ns"`

	// DocumentKey contains the _id of the affected document
	DocumentKey bson.Raw `bson:"documentKey,omitempty"`

	// UpdateDescription describes the fields that were updated
	UpdateDescription *UpdateDescription `bson:"updateDescription,omitempty"`

	// ClusterTime is the timestamp from the oplog
	ClusterTime ClusterTime `bson:"clusterTime"`

	// TxnNumber is the transaction number (if in a transaction)
	TxnNumber *int64 `bson:"txnNumber,omitempty"`

	// LSID is the logical session ID (if in a session)
	LSID *bson.Raw `bson:"lsid,omitempty"`

	// WallTime is the server time when the change occurred
	WallTime *ClusterTime `bson:"wallTime,omitempty"`
}

// Namespace represents a MongoDB namespace (database.collection).
type Namespace struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

// ClusterTime represents a MongoDB cluster timestamp.
type ClusterTime struct {
	T uint32 `bson:"t"` // Unix timestamp
	I uint32 `bson:"i"` // Increment
}

// UpdateDescription describes which fields were updated in an update operation.
type UpdateDescription struct {
	// UpdatedFields contains the fields that were updated
	UpdatedFields bson.Raw `bson:"updatedFields"`

	// RemovedFields contains the names of fields that were removed
	RemovedFields []string `bson:"removedFields,omitempty"`

	// TruncatedArrays contains arrays that were truncated
	TruncatedArrays []TruncatedArray `bson:"truncatedArrays,omitempty"`
}

// TruncatedArray describes an array that was truncated.
type TruncatedArray struct {
	// Field is the field path to the array
	Field string `bson:"field"`

	// NewSize is the new size of the array
	NewSize int32 `bson:"newSize"`
}

// DDLChangeEvent represents a DDL change event (create, drop, etc.)
type DDLChangeEvent struct {
	ChangeEventDocument

	// CollectionUUID is the UUID of the collection (for some DDL events)
	CollectionUUID *string `bson:"collectionUUID,omitempty"`

	// OnDemandStorage indicates if the collection uses on-demand storage
	OnDemandStorage *OnDemandStorage `bson:"onDemandStorage,omitempty"`
}

// OnDemandStorage contains on-demand storage configuration.
type OnDemandStorage struct {
	// RecordedDataSize is the size of recorded data
	RecordedDataSize int64 `bson:"recordedDataSize"`
}

// ResumeToken represents a MongoDB resume token.
type ResumeToken struct {
	// Data is the raw resume token data
	Data string `json:"data"`

	// Timestamp is the timestamp component
	Timestamp ClusterTime `json:"timestamp"`

	// Version is the resume token version
	Version int `json:"version"`
}

// ChangeStreamOptions holds options for the change stream.
type ChangeStreamOptions struct {
	// ResumeAfter sets the resume token to start after
	ResumeAfter bson.Raw

	// StartAfter sets the resume token to start after (similar to ResumeAfter)
	StartAfter bson.Raw

	// StartAtOperationTime sets the timestamp to start at
	StartAtOperationTime *ClusterTime

	// FullDocument specifies how to return the full document
	FullDocument string

	// FullDocumentBefore specifies how to return the document before the change
	FullDocumentBefore string

	// ShowExpandedEvents includes expanded events
	ShowExpandedEvents bool

	// MaxAwaitTime is the maximum time to wait for new data
	MaxAwaitTimeMs int32

	// BatchSize is the number of documents to return per batch
	BatchSize int32

	// Comment is an arbitrary string to help trace the operation
	Comment string
}

// OperationType constants for MongoDB change events.
const (
	OperationTypeInsert                   = "insert"
	OperationTypeUpdate                   = "update"
	OperationTypeReplace                  = "replace"
	OperationTypeDelete                   = "delete"
	OperationTypeDrop                     = "drop"
	OperationTypeDropDatabase             = "dropDatabase"
	OperationTypeRename                   = "rename"
	OperationTypeCreate                   = "create"
	OperationTypeCreateIndexes            = "createIndexes"
	OperationTypeDropIndexes              = "dropIndexes"
	OperationTypeModify                   = "modify"
	OperationTypeShardCollection          = "shardCollection"
	OperationTypeRefineCollectionShardKey = "refineCollectionShardKey"
	OperationTypeReshardCollection        = "reshardCollection"
	OperationTypeInvalidate               = "invalidate"
)

// IsDDL returns true if the operation is a DDL operation.
func (e *ChangeEventDocument) IsDDL() bool {
	switch e.OperationType {
	case OperationTypeCreate, OperationTypeDrop, OperationTypeDropDatabase,
		OperationTypeRename, OperationTypeCreateIndexes, OperationTypeDropIndexes,
		OperationTypeModify, OperationTypeShardCollection,
		OperationTypeRefineCollectionShardKey, OperationTypeReshardCollection:
		return true
	default:
		return false
	}
}

// IsData returns true if the operation is a data operation.
func (e *ChangeEventDocument) IsData() bool {
	switch e.OperationType {
	case OperationTypeInsert, OperationTypeUpdate, OperationTypeReplace, OperationTypeDelete:
		return true
	default:
		return false
	}
}

// GetDocumentID extracts the document _id from the document key.
func (e *ChangeEventDocument) GetDocumentID() interface{} {
	if e.DocumentKey == nil {
		return nil
	}

	var key struct {
		ID interface{} `bson:"_id"`
	}
	if err := bson.Unmarshal(e.DocumentKey, &key); err != nil {
		return nil
	}
	return key.ID
}

// GetDatabase returns the database name.
func (e *ChangeEventDocument) GetDatabase() string {
	return e.NS.DB
}

// GetCollection returns the collection name.
func (e *ChangeEventDocument) GetCollection() string {
	return e.NS.Coll
}
