package errors

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
)

func TestDataStreamErrorFields(t *testing.T) {
	err := NewDataStreamError(
		ErrConnectionFailed,
		SeverityRecoverable,
		CategoryConnection,
		"connection refused to host:3306",
	)
	if err.Severity != SeverityRecoverable {
		t.Errorf("Severity = %v, want %v", err.Severity, SeverityRecoverable)
	}
	if err.Category != CategoryConnection {
		t.Errorf("Category = %v, want %v", err.Category, CategoryConnection)
	}
	if !err.IsRecoverable() {
		t.Error("expected IsRecoverable() = true")
	}
	if err.IsFatal() {
		t.Error("expected IsFatal() = false")
	}
	// Must implement error interface
	if err.Error() == "" {
		t.Error("Error() should not be empty")
	}
}

func TestDataStreamErrorSeverityHelpers(t *testing.T) {
	fatal := NewDataStreamError(ErrInternal, SeverityFatal, CategoryInternal, "panic")
	if !fatal.IsFatal() {
		t.Error("expected IsFatal() = true")
	}
	if fatal.IsRecoverable() {
		t.Error("expected IsRecoverable() = false for fatal")
	}

	intervention := NewDataStreamError(ErrSchemaChanged, SeverityIntervention, CategorySchema, "schema mismatch")
	if !intervention.NeedsIntervention() {
		t.Error("expected NeedsIntervention() = true")
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		severity ErrorSeverity
		category ErrorCategory
	}{
		{"context deadline", context.DeadlineExceeded, SeverityRecoverable, CategoryNetwork},
		{"context canceled", context.Canceled, SeverityRecoverable, CategoryNetwork},
		{"io EOF", io.EOF, SeverityRecoverable, CategoryConnection},
		{"net error", &net.OpError{Op: "dial", Err: io.EOF}, SeverityRecoverable, CategoryNetwork},
		{"unknown error", errors.New("unknown"), SeverityIntervention, CategoryInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsErr := ClassifyError(tt.err)
			if dsErr.Severity != tt.severity {
				t.Errorf("Severity = %v, want %v", dsErr.Severity, tt.severity)
			}
			if dsErr.Category != tt.category {
				t.Errorf("Category = %v, want %v", dsErr.Category, tt.category)
			}
		})
	}
}

func TestDataStreamErrorUnwrap(t *testing.T) {
	cause := errors.New("root cause")
	dsErr := &DataStreamError{
		Code:     ErrInternal,
		Severity: SeverityFatal,
		Category: CategoryInternal,
		Message:  "something broke",
		Cause:    cause,
	}
	if dsErr.Unwrap() != cause {
		t.Error("Unwrap() should return the cause")
	}
}

func TestDataStreamErrorMessageWithCause(t *testing.T) {
	cause := errors.New("root cause")
	dsErr := &DataStreamError{
		Code:     ErrInternal,
		Severity: SeverityFatal,
		Category: CategoryInternal,
		Message:  "something broke",
		Cause:    cause,
	}
	msg := dsErr.Error()
	if msg != "something broke: root cause" {
		t.Errorf("Error() = %q, want %q", msg, "something broke: root cause")
	}
}

func TestClassifyErrorStringMatching(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		severity ErrorSeverity
		category ErrorCategory
	}{
		{"connection refused", errors.New("connection refused"), SeverityRecoverable, CategoryNetwork},
		{"timeout string", errors.New("operation timeout"), SeverityRecoverable, CategoryNetwork},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsErr := ClassifyError(tt.err)
			if dsErr.Severity != tt.severity {
				t.Errorf("Severity = %v, want %v", dsErr.Severity, tt.severity)
			}
			if dsErr.Category != tt.category {
				t.Errorf("Category = %v, want %v", dsErr.Category, tt.category)
			}
		})
	}
}
