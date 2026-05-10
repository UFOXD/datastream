package pipeline

import "github.com/pingcap/errors"

var (
	// ErrAlreadyRunning is returned when trying to start a running pipeline.
	ErrAlreadyRunning = errors.Normalize("pipeline already running", errors.RFCCodeText("DS:Pipeline:AlreadyRunning"))

	// ErrNotRunning is returned when operations are called on a non-running pipeline.
	ErrNotRunning = errors.Normalize("pipeline not running", errors.RFCCodeText("DS:Pipeline:NotRunning"))

	// ErrInvalidState is returned when an operation is invalid for the current state.
	ErrInvalidState = errors.Normalize("invalid pipeline state", errors.RFCCodeText("DS:Pipeline:InvalidState"))

	// ErrNoSource is returned when no source is configured.
	ErrNoSource = errors.Normalize("no source configured", errors.RFCCodeText("DS:Pipeline:NoSource"))

	// ErrNoSink is returned when no sinks are configured.
	ErrNoSink = errors.Normalize("no sinks configured", errors.RFCCodeText("DS:Pipeline:NoSink"))

	// ErrBufferFull is returned when the buffer is full.
	ErrBufferFull = errors.Normalize("buffer full", errors.RFCCodeText("DS:Pipeline:BufferFull"))

	// ErrDispatchFailed is returned when event dispatch fails.
	ErrDispatchFailed = errors.Normalize("dispatch failed", errors.RFCCodeText("DS:Pipeline:DispatchFailed"))

	// ErrWriteFailed is returned when writing to sink fails.
	ErrWriteFailed = errors.Normalize("write failed", errors.RFCCodeText("DS:Pipeline:WriteFailed"))

	// ErrCoordinatorFailed is returned when coordinator operation fails.
	ErrCoordinatorFailed = errors.Normalize("coordinator failed", errors.RFCCodeText("DS:Pipeline:CoordinatorFailed"))
)
