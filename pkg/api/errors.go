package api

import "github.com/pingcap/errors"

var (
	// ErrInvalidRequest is returned when the request is invalid.
	ErrInvalidRequest = errors.Normalize("invalid request", errors.RFCCodeText("DS:API:InvalidRequest"))

	// ErrTaskNotFound is returned when a task is not found.
	ErrTaskNotFound = errors.Normalize("task not found", errors.RFCCodeText("DS:API:TaskNotFound"))

	// ErrInternalError is returned when an internal error occurs.
	ErrInternalError = errors.Normalize("internal error", errors.RFCCodeText("DS:API:InternalError"))

	// ErrServiceUnavailable is returned when a service is unavailable.
	ErrServiceUnavailable = errors.Normalize("service unavailable", errors.RFCCodeText("DS:API:ServiceUnavailable"))
)
