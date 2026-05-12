package source

import "github.com/pingcap/errors"

var (
	// ErrUnsupportedConnector is returned when an unsupported connector type is requested.
	ErrUnsupportedConnector = errors.Normalize("unsupported connector type", errors.RFCCodeText("DS:Source:Unsupported"))

	// ErrConnectionFailed is returned when connection to the source fails.
	ErrConnectionFailed = errors.Normalize("connection failed", errors.RFCCodeText("DS:Source:ConnectionFailed"))

	// ErrInvalidConfig is returned when the configuration is invalid.
	ErrInvalidConfig = errors.Normalize("invalid configuration", errors.RFCCodeText("DS:Source:InvalidConfig"))

	// ErrNotInitialized is returned when operations are called before initialization.
	ErrNotInitialized = errors.Normalize("connector not initialized", errors.RFCCodeText("DS:Source:NotInitialized"))

	// ErrAlreadyRunning is returned when Start is called on a running connector.
	ErrAlreadyRunning = errors.Normalize("connector already running", errors.RFCCodeText("DS:Source:AlreadyRunning"))

	// ErrSnapshotFailed is returned when a snapshot fails.
	ErrSnapshotFailed = errors.Normalize("snapshot failed", errors.RFCCodeText("DS:Source:SnapshotFailed"))

	// ErrBinlogFailed is returned when binlog streaming fails.
	ErrBinlogFailed = errors.Normalize("binlog streaming failed", errors.RFCCodeText("DS:Source:BinlogFailed"))

	// ErrSchemaNotFound is returned when a table schema is not found.
	ErrSchemaNotFound = errors.Normalize("schema not found", errors.RFCCodeText("DS:Source:SchemaNotFound"))

	// ErrInvalidSyncScope is returned when sync scope configuration is invalid.
	ErrInvalidSyncScope = errors.Normalize("invalid sync scope configuration", errors.RFCCodeText("DS:Source:InvalidSyncScope"))

	// ErrDatabaseNotFound is returned when a database is not found.
	ErrDatabaseNotFound = errors.Normalize("database not found", errors.RFCCodeText("DS:Source:DatabaseNotFound"))

	// ErrTableNotFound is returned when a table is not found.
	ErrTableNotFound = errors.Normalize("table not found", errors.RFCCodeText("DS:Source:TableNotFound"))

	// ErrTableAlreadyExists is returned when adding an existing table.
	ErrTableAlreadyExists = errors.Normalize("table already exists in sync scope", errors.RFCCodeText("DS:Source:TableAlreadyExists"))

	// ErrDiscoveryNotSupported is returned when discovery is not supported.
	ErrDiscoveryNotSupported = errors.Normalize("auto-discovery not supported for this connector", errors.RFCCodeText("DS:Source:DiscoveryNotSupported"))
)
