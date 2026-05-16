package metrics

import "github.com/UFOXD/datastream/pkg/errors"

// ErrorType is the value of the error_type label on error counters.
type ErrorType string

const (
	ErrorTypeRetriable    ErrorType = "retriable"
	ErrorTypeNonRetriable ErrorType = "non_retriable"
)

// ClassifyError maps an error to its metric label value.
// Single source of truth is pkg/errors.IsRetryableError.
func ClassifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeRetriable
	}
	if errors.IsRetryableError(err) {
		return ErrorTypeRetriable
	}
	return ErrorTypeNonRetriable
}
