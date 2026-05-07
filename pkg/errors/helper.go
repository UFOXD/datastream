package errors

import (
	"context"
	"net/http"

	"github.com/pingcap/errors"
)

// WrapError wraps err with the given RFC error and formats args into the message.
// Returns nil if err is nil.
func WrapError(rfcError *errors.Error, err error, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return rfcError.Wrap(err).GenWithStackByArgs(args...)
}

// RFCCode extracts the RFC error code from err by walking the error chain.
// Returns ("", false) if no RFC code is found.
func RFCCode(err error) (errors.RFCErrorCode, bool) {
	type rfcCoder interface {
		RFCCode() errors.RFCErrorCode
	}
	for err != nil {
		if c, ok := err.(rfcCoder); ok {
			return c.RFCCode(), true
		}
		err = errors.Unwrap(err)
	}
	return "", false
}

// unretryableErrors lists errors that must never be retried.
var unretryableErrors = []*errors.Error{
	ErrInvalidArgument,
	ErrNotImplemented,
	ErrInvalidConfig,
	ErrConfigNotFound,
	ErrTaskNotExists,
	ErrTaskAlreadyExists,
	ErrSourceNotExists,
	ErrSinkNotExists,
	ErrSchemaNotExists,
	ErrSchemaIncompatible,
	ErrSourcePositionLost,
	ErrSourceGCTTLExceeded,
	ErrTableNotExists,
	ErrTableAlreadyExists,
}

// IsRetryableError reports whether err represents a transient condition that
// is safe to retry. context.Canceled and context.DeadlineExceeded are always
// considered non-retryable.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// context errors are never retryable
	cause := errors.Cause(err)
	if cause == context.Canceled || cause == context.DeadlineExceeded {
		return false
	}
	for _, e := range unretryableErrors {
		if e.Equal(err) {
			return false
		}
	}
	return true
}

// httpStatusCodeMapping maps RFC error codes to HTTP status codes.
var httpStatusCodeMapping = map[errors.RFCErrorCode]int{
	ErrUnknown.RFCCode():               http.StatusInternalServerError,
	ErrInvalidArgument.RFCCode():       http.StatusBadRequest,
	ErrInternal.RFCCode():              http.StatusInternalServerError,
	ErrNotImplemented.RFCCode():        http.StatusNotImplemented,
	ErrInvalidConfig.RFCCode():         http.StatusBadRequest,
	ErrConfigNotFound.RFCCode():        http.StatusNotFound,
	ErrTaskNotExists.RFCCode():         http.StatusNotFound,
	ErrTaskAlreadyExists.RFCCode():     http.StatusConflict,
	ErrTaskRunning.RFCCode():           http.StatusConflict,
	ErrTaskNotRunning.RFCCode():        http.StatusConflict,
	ErrTaskPaused.RFCCode():            http.StatusConflict,
	ErrTaskFailed.RFCCode():            http.StatusInternalServerError,
	ErrSourceNotExists.RFCCode():       http.StatusNotFound,
	ErrSinkNotExists.RFCCode():         http.StatusNotFound,
	ErrSchemaNotExists.RFCCode():       http.StatusNotFound,
	ErrSchemaIncompatible.RFCCode():    http.StatusUnprocessableEntity,
	ErrTableNotExists.RFCCode():        http.StatusNotFound,
	ErrTableAlreadyExists.RFCCode():    http.StatusConflict,
	ErrNotLeader.RFCCode():             http.StatusServiceUnavailable,
	ErrCoordinatorTimeout.RFCCode():    http.StatusServiceUnavailable,
	ErrCoordinatorUnreachable.RFCCode(): http.StatusServiceUnavailable,
	ErrPipelineStopped.RFCCode():       http.StatusServiceUnavailable,
	ErrPipelineTimeout.RFCCode():       http.StatusGatewayTimeout,
}

// HTTPStatusCode returns the HTTP status code that best represents err.
// Returns http.StatusOK for nil, http.StatusInternalServerError for unknown errors.
func HTTPStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}
	rfcCode, ok := RFCCode(err)
	if !ok {
		return http.StatusInternalServerError
	}
	if code, ok := httpStatusCodeMapping[rfcCode]; ok {
		return code
	}
	return http.StatusInternalServerError
}
