package errors

import (
	"context"
	stderrors "errors"
	"io"
	"net"
	"strings"

	"github.com/pingcap/errors"
)

// ErrorSeverity defines the severity level of an error.
type ErrorSeverity string

const (
	SeverityWarning      ErrorSeverity = "warning"      // non-critical, informational
	SeverityRecoverable  ErrorSeverity = "recoverable"  // can be retried automatically
	SeverityIntervention ErrorSeverity = "intervention"  // requires human intervention
	SeverityFatal        ErrorSeverity = "fatal"         // unrecoverable, must stop
)

// ErrorCategory defines the category of an error.
type ErrorCategory string

const (
	CategoryConnection ErrorCategory = "connection"  // connection lifecycle errors
	CategoryNetwork    ErrorCategory = "network"     // network transport errors
	CategoryPermission ErrorCategory = "permission"  // authentication/authorization errors
	CategorySchema     ErrorCategory = "schema"      // schema compatibility errors
	CategoryData       ErrorCategory = "data"        // data integrity errors
	CategoryConfig     ErrorCategory = "config"      // configuration errors
	CategoryInternal   ErrorCategory = "internal"    // internal/unknown errors
	CategoryResource   ErrorCategory = "resource"    // resource exhaustion errors
)

// DataStreamError is a classified error that carries severity and category
// metadata alongside the original pingcap/errors RFC error code.
type DataStreamError struct {
	Code     *errors.Error // RFC error code from pingcap/errors
	Severity ErrorSeverity
	Category ErrorCategory
	Message  string
	Cause    error
}

// Error implements the error interface.
func (e *DataStreamError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// Unwrap returns the underlying cause for use with errors.Is/As.
func (e *DataStreamError) Unwrap() error {
	return e.Cause
}

// IsRecoverable reports whether the error can be retried automatically.
func (e *DataStreamError) IsRecoverable() bool {
	return e.Severity == SeverityRecoverable
}

// IsFatal reports whether the error is unrecoverable and requires stopping.
func (e *DataStreamError) IsFatal() bool {
	return e.Severity == SeverityFatal
}

// NeedsIntervention reports whether the error requires human intervention.
func (e *DataStreamError) NeedsIntervention() bool {
	return e.Severity == SeverityIntervention
}

// NewDataStreamError creates a new DataStreamError with the given parameters.
func NewDataStreamError(code *errors.Error, severity ErrorSeverity, category ErrorCategory, message string) *DataStreamError {
	return &DataStreamError{
		Code:     code,
		Severity: severity,
		Category: category,
		Message:  message,
	}
}

// ClassifyError inspects a standard error and returns a DataStreamError with
// appropriate severity and category. This is used to classify errors from
// external libraries (database drivers, net, io) into the DataStream error
// taxonomy.
func ClassifyError(err error) *DataStreamError {
	if err == nil {
		return nil
	}

	// Check context errors first.
	if err == context.DeadlineExceeded || err == context.Canceled {
		return &DataStreamError{
			Code:     ErrConnectionFailed,
			Severity: SeverityRecoverable,
			Category: CategoryNetwork,
			Message:  err.Error(),
			Cause:    err,
		}
	}

	// Check io.EOF / io.ErrUnexpectedEOF — typically a broken connection.
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return &DataStreamError{
			Code:     ErrConnectionFailed,
			Severity: SeverityRecoverable,
			Category: CategoryConnection,
			Message:  err.Error(),
			Cause:    err,
		}
	}

	// Check net.OpError.
	var opErr *net.OpError
	if stderrors.As(err, &opErr) {
		return &DataStreamError{
			Code:     ErrConnectionFailed,
			Severity: SeverityRecoverable,
			Category: CategoryNetwork,
			Message:  err.Error(),
			Cause:    err,
		}
	}

	// Fallback: check error message strings for common patterns.
	msg := err.Error()
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "timeout") {
		return &DataStreamError{
			Code:     ErrConnectionFailed,
			Severity: SeverityRecoverable,
			Category: CategoryNetwork,
			Message:  msg,
			Cause:    err,
		}
	}

	// Default: unknown error → intervention required.
	return &DataStreamError{
		Code:     ErrUnknown,
		Severity: SeverityIntervention,
		Category: CategoryInternal,
		Message:  msg,
		Cause:    err,
	}
}
