// Package errors defines all DataStream error types and codes.
package errors

import "github.com/pingcap/errors"

// === Common Errors ===
var (
	ErrUnknown           = errors.Normalize("unknown error", errors.RFCCodeText("DS:ErrUnknown"))
	ErrInvalidArgument   = errors.Normalize("invalid argument: %s", errors.RFCCodeText("DS:ErrInvalidArgument"))
	ErrInternal          = errors.Normalize("internal error: %s", errors.RFCCodeText("DS:ErrInternal"))
	ErrNotImplemented    = errors.Normalize("not implemented", errors.RFCCodeText("DS:ErrNotImplemented"))
	ErrConnectionFailed  = errors.Normalize("connection failed: %s", errors.RFCCodeText("DS:ErrConnectionFailed"))
	ErrSchemaChanged     = errors.Normalize("schema changed: %s", errors.RFCCodeText("DS:ErrSchemaChanged"))
)

// === Config Errors ===
var (
	ErrInvalidConfig  = errors.Normalize("invalid config: %s", errors.RFCCodeText("DS:ErrInvalidConfig"))
	ErrConfigNotFound = errors.Normalize("config not found: %s", errors.RFCCodeText("DS:ErrConfigNotFound"))
)

// === Task Errors ===
var (
	ErrTaskNotExists     = errors.Normalize("task not exists: %s", errors.RFCCodeText("DS:ErrTaskNotExists"))
	ErrTaskAlreadyExists = errors.Normalize("task already exists: %s", errors.RFCCodeText("DS:ErrTaskAlreadyExists"))
	ErrTaskRunning       = errors.Normalize("task is running: %s", errors.RFCCodeText("DS:ErrTaskRunning"))
	ErrTaskNotRunning    = errors.Normalize("task is not running: %s", errors.RFCCodeText("DS:ErrTaskNotRunning"))
	ErrTaskPaused        = errors.Normalize("task is paused: %s", errors.RFCCodeText("DS:ErrTaskPaused"))
	ErrTaskFailed        = errors.Normalize("task failed: %s", errors.RFCCodeText("DS:ErrTaskFailed"))
)

// === Source Errors ===
var (
	ErrSourceNotExists        = errors.Normalize("source not exists: %s", errors.RFCCodeText("DS:ErrSourceNotExists"))
	ErrSourceConnectionFailed = errors.Normalize("source connection failed: %s", errors.RFCCodeText("DS:ErrSourceConnectionFailed"))
	ErrSourceReadFailed       = errors.Normalize("source read failed: %s", errors.RFCCodeText("DS:ErrSourceReadFailed"))
	ErrSourcePositionLost     = errors.Normalize("source position lost", errors.RFCCodeText("DS:ErrSourcePositionLost"))
	ErrSourceGCTTLExceeded    = errors.Normalize("source GC TTL exceeded", errors.RFCCodeText("DS:ErrSourceGCTTLExceeded"))
	ErrSourceSnapshotFailed   = errors.Normalize("source snapshot failed: %s", errors.RFCCodeText("DS:ErrSourceSnapshotFailed"))
)

// === Sink Errors ===
var (
	ErrSinkNotExists          = errors.Normalize("sink not exists: %s", errors.RFCCodeText("DS:ErrSinkNotExists"))
	ErrSinkConnectionFailed   = errors.Normalize("sink connection failed: %s", errors.RFCCodeText("DS:ErrSinkConnectionFailed"))
	ErrSinkWriteFailed        = errors.Normalize("sink write failed: %s", errors.RFCCodeText("DS:ErrSinkWriteFailed"))
	ErrSinkDDLFailed          = errors.Normalize("sink ddl apply failed: %s", errors.RFCCodeText("DS:ErrSinkDDLFailed"))
	ErrSinkPositionSaveFailed = errors.Normalize("sink position save failed: %s", errors.RFCCodeText("DS:ErrSinkPositionSaveFailed"))
)

// === Schema Errors ===
var (
	ErrSchemaNotExists    = errors.Normalize("schema not exists: %s", errors.RFCCodeText("DS:ErrSchemaNotExists"))
	ErrSchemaIncompatible = errors.Normalize("schema incompatible: %s", errors.RFCCodeText("DS:ErrSchemaIncompatible"))
	ErrSchemaFetchFailed  = errors.Normalize("schema fetch failed: %s", errors.RFCCodeText("DS:ErrSchemaFetchFailed"))
)

// === Pipeline Errors ===
var (
	ErrPipelineStopped = errors.Normalize("pipeline stopped", errors.RFCCodeText("DS:ErrPipelineStopped"))
	ErrPipelineTimeout = errors.Normalize("pipeline timeout", errors.RFCCodeText("DS:ErrPipelineTimeout"))
	ErrFilterFailed    = errors.Normalize("filter failed: %s", errors.RFCCodeText("DS:ErrFilterFailed"))
	ErrTransformFailed = errors.Normalize("transform failed: %s", errors.RFCCodeText("DS:ErrTransformFailed"))
)

// === Coordinator Errors ===
var (
	ErrNotLeader              = errors.Normalize("%s is not leader", errors.RFCCodeText("DS:ErrNotLeader"))
	ErrLeaderElectionFailed   = errors.Normalize("leader election failed: %s", errors.RFCCodeText("DS:ErrLeaderElectionFailed"))
	ErrCoordinatorTimeout     = errors.Normalize("coordinator timeout", errors.RFCCodeText("DS:ErrCoordinatorTimeout"))
	ErrCoordinatorUnreachable = errors.Normalize("coordinator unreachable", errors.RFCCodeText("DS:ErrCoordinatorUnreachable"))
)

// === Table Errors ===
var (
	ErrTableNotExists     = errors.Normalize("table not exists: %s", errors.RFCCodeText("DS:ErrTableNotExists"))
	ErrTableAlreadyExists = errors.Normalize("table already exists: %s", errors.RFCCodeText("DS:ErrTableAlreadyExists"))
)
