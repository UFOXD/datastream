package metrics_test

import (
	"context"
	"errors"
	"testing"

	dserrors "github.com/UFOXD/datastream/pkg/errors"
	"github.com/UFOXD/datastream/pkg/metrics"
)

func TestClassifyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want metrics.ErrorType
	}{
		{"nil", nil, metrics.ErrorTypeRetriable},
		{"context_canceled", context.Canceled, metrics.ErrorTypeNonRetriable},
		{"deadline", context.DeadlineExceeded, metrics.ErrorTypeNonRetriable},
		{"random_io", errors.New("connection reset"), metrics.ErrorTypeRetriable},
		{"invalid_arg", dserrors.ErrInvalidArgument.GenWithStackByArgs("bad"), metrics.ErrorTypeNonRetriable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := metrics.ClassifyError(c.err)
			if got != c.want {
				t.Errorf("ClassifyError(%v) = %s, want %s", c.err, got, c.want)
			}
		})
	}
}
