package sink

import "github.com/pingcap/errors"

var (
	// ErrUnsupportedConnector is returned when an unsupported connector type is requested.
	ErrUnsupportedConnector = errors.Normalize("unsupported sink type", errors.RFCCodeText("DS:Sink:Unsupported"))

	// ErrConnectionFailed is returned when connection to the sink fails.
	ErrConnectionFailed = errors.Normalize("connection failed", errors.RFCCodeText("DS:Sink:ConnectionFailed"))

	// ErrInvalidConfig is returned when the configuration is invalid.
	ErrInvalidConfig = errors.Normalize("invalid configuration", errors.RFCCodeText("DS:Sink:InvalidConfig"))

	// ErrNotInitialized is returned when operations are called before initialization.
	ErrNotInitialized = errors.Normalize("sink not initialized", errors.RFCCodeText("DS:Sink:NotInitialized"))

	// ErrWriteFailed is returned when writing to the sink fails.
	ErrWriteFailed = errors.Normalize("write failed", errors.RFCCodeText("DS:Sink:WriteFailed"))

	// ErrFlushFailed is returned when flushing fails.
	ErrFlushFailed = errors.Normalize("flush failed", errors.RFCCodeText("DS:Sink:FlushFailed"))

	// ErrUnsupportedOperation is returned when an unsupported operation is requested.
	ErrUnsupportedOperation = errors.Normalize("unsupported operation", errors.RFCCodeText("DS:Sink:Unsupported"))

	// ErrDDLNotSupported is returned when DDL is not supported.
	ErrDDLNotSupported = errors.Normalize("DDL not supported", errors.RFCCodeText("DS:Sink:DDLNotSupported"))

	// ErrTransactionNotSupported is returned when transaction is not supported.
	ErrTransactionNotSupported = errors.Normalize("transaction not supported", errors.RFCCodeText("DS:Sink:TxNotSupported"))

	// ErrBufferFull is returned when the write buffer is full.
	ErrBufferFull = errors.Normalize("buffer full", errors.RFCCodeText("DS:Sink:BufferFull"))
)
