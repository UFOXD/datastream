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
)
