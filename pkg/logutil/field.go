package logutil

import (
	"time"

	"go.uber.org/zap"
)

// StringField returns a zap string field.
func StringField(key, val string) zap.Field {
	return zap.String(key, val)
}

// IntField returns a zap int field.
func IntField(key string, val int) zap.Field {
	return zap.Int(key, val)
}

// Int64Field returns a zap int64 field.
func Int64Field(key string, val int64) zap.Field {
	return zap.Int64(key, val)
}

// Uint64Field returns a zap uint64 field.
func Uint64Field(key string, val uint64) zap.Field {
	return zap.Uint64(key, val)
}

// DurationField returns a zap duration field.
func DurationField(key string, val time.Duration) zap.Field {
	return zap.Duration(key, val)
}

// TimeField returns a zap time field.
func TimeField(key string, val time.Time) zap.Field {
	return zap.Time(key, val)
}

// ErrorField returns a zap error field.
func ErrorField(err error) zap.Field {
	return zap.Error(err)
}

// ObjectField returns a zap field for any value using reflection.
func ObjectField(key string, val interface{}) zap.Field {
	return zap.Reflect(key, val)
}

// ShortError returns a zap field containing only the error message,
// with no stack trace. Useful for expected/non-fatal errors.
func ShortError(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", err.Error())
}
