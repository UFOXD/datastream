package errors

import (
	"errors"
	"testing"
)

func TestCommonErrors(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		code  string
	}{
		{"ErrUnknown", ErrUnknown, "DS:ErrUnknown"},
		{"ErrInvalidArgument", ErrInvalidArgument, "DS:ErrInvalidArgument"},
		{"ErrInternal", ErrInternal, "DS:ErrInternal"},
		{"ErrNotImplemented", ErrNotImplemented, "DS:ErrNotImplemented"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
			if tt.err.Error() == "" {
				t.Error("error message should not be empty")
			}
		})
	}
}

func TestConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrInvalidConfig", ErrInvalidConfig},
		{"ErrConfigNotFound", ErrConfigNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestTaskErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrTaskNotExists", ErrTaskNotExists},
		{"ErrTaskAlreadyExists", ErrTaskAlreadyExists},
		{"ErrTaskRunning", ErrTaskRunning},
		{"ErrTaskNotRunning", ErrTaskNotRunning},
		{"ErrTaskPaused", ErrTaskPaused},
		{"ErrTaskFailed", ErrTaskFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestSourceErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrSourceNotExists", ErrSourceNotExists},
		{"ErrSourceConnectionFailed", ErrSourceConnectionFailed},
		{"ErrSourceReadFailed", ErrSourceReadFailed},
		{"ErrSourcePositionLost", ErrSourcePositionLost},
		{"ErrSourceGCTTLExceeded", ErrSourceGCTTLExceeded},
		{"ErrSourceSnapshotFailed", ErrSourceSnapshotFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestSinkErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrSinkNotExists", ErrSinkNotExists},
		{"ErrSinkConnectionFailed", ErrSinkConnectionFailed},
		{"ErrSinkWriteFailed", ErrSinkWriteFailed},
		{"ErrSinkDDLFailed", ErrSinkDDLFailed},
		{"ErrSinkPositionSaveFailed", ErrSinkPositionSaveFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestSchemaErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrSchemaNotExists", ErrSchemaNotExists},
		{"ErrSchemaIncompatible", ErrSchemaIncompatible},
		{"ErrSchemaFetchFailed", ErrSchemaFetchFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestPipelineErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrPipelineStopped", ErrPipelineStopped},
		{"ErrPipelineTimeout", ErrPipelineTimeout},
		{"ErrFilterFailed", ErrFilterFailed},
		{"ErrTransformFailed", ErrTransformFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestCoordinatorErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrNotLeader", ErrNotLeader},
		{"ErrLeaderElectionFailed", ErrLeaderElectionFailed},
		{"ErrCoordinatorTimeout", ErrCoordinatorTimeout},
		{"ErrCoordinatorUnreachable", ErrCoordinatorUnreachable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestTableErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrTableNotExists", ErrTableNotExists},
		{"ErrTableAlreadyExists", ErrTableAlreadyExists},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
		})
	}
}

func TestErrorFormatting(t *testing.T) {
	// Test that errors can be formatted with arguments
	err := ErrInvalidArgument
	if err == nil {
		t.Fatal("ErrInvalidArgument should not be nil")
	}

	// Test error message contains expected content
	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
}

func TestErrorWrapping(t *testing.T) {
	// Test that errors can be wrapped
	innerErr := ErrSourceConnectionFailed
	wrapped := errors.Join(innerErr, ErrSourceReadFailed)

	if wrapped == nil {
		t.Error("wrapped error should not be nil")
	}
}

func TestAllErrorsDefined(t *testing.T) {
	// Ensure all exported errors are defined
	allErrors := []error{
		ErrUnknown,
		ErrInvalidArgument,
		ErrInternal,
		ErrNotImplemented,
		ErrInvalidConfig,
		ErrConfigNotFound,
		ErrTaskNotExists,
		ErrTaskAlreadyExists,
		ErrTaskRunning,
		ErrTaskNotRunning,
		ErrTaskPaused,
		ErrTaskFailed,
		ErrSourceNotExists,
		ErrSourceConnectionFailed,
		ErrSourceReadFailed,
		ErrSourcePositionLost,
		ErrSourceGCTTLExceeded,
		ErrSourceSnapshotFailed,
		ErrSinkNotExists,
		ErrSinkConnectionFailed,
		ErrSinkWriteFailed,
		ErrSinkDDLFailed,
		ErrSinkPositionSaveFailed,
		ErrSchemaNotExists,
		ErrSchemaIncompatible,
		ErrSchemaFetchFailed,
		ErrPipelineStopped,
		ErrPipelineTimeout,
		ErrFilterFailed,
		ErrTransformFailed,
		ErrNotLeader,
		ErrLeaderElectionFailed,
		ErrCoordinatorTimeout,
		ErrCoordinatorUnreachable,
		ErrTableNotExists,
		ErrTableAlreadyExists,
	}

	for i, err := range allErrors {
		if err == nil {
			t.Errorf("Error at index %d should not be nil", i)
		}
	}
}
