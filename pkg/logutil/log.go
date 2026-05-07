// Package logutil provides logging utilities for DataStream.
package logutil

import (
	"context"
	"fmt"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// LogConfig holds configuration for the logger.
type LogConfig struct {
	Level      string `toml:"level" json:"level"`
	File       string `toml:"file" json:"file"`
	MaxSize    int    `toml:"max-size" json:"max-size"`
	MaxDays    int    `toml:"max-days" json:"max-days"`
	MaxBackups int    `toml:"max-backups" json:"max-backups"`
}

// InitLogger initializes the global logger with the given configuration.
func InitLogger(cfg *LogConfig) error {
	if cfg == nil {
		return fmt.Errorf("logutil: LogConfig must not be nil")
	}

	level := cfg.Level
	if level == "" {
		level = "info"
	}

	logCfg := &log.Config{
		Level: level,
	}

	if cfg.File != "" {
		logCfg.File = log.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.MaxSize,
			MaxDays:    cfg.MaxDays,
			MaxBackups: cfg.MaxBackups,
		}
	}

	logger, props, err := log.InitLogger(logCfg)
	if err != nil {
		return fmt.Errorf("logutil: failed to initialize logger: %w", err)
	}

	log.ReplaceGlobals(logger, props)
	return nil
}

// L returns the global logger.
func L() *zap.Logger {
	return log.L()
}

// WithComponent returns a child logger with a "component" field attached.
func WithComponent(component string) *zap.Logger {
	return log.L().With(zap.String("component", component))
}

// ErrorFilterContextCanceled returns true when err is (or wraps) context.Canceled.
func ErrorFilterContextCanceled(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled {
		return true
	}
	// Walk the error chain
	type unwrapper interface {
		Unwrap() error
	}
	for e := err; e != nil; {
		if e == context.Canceled {
			return true
		}
		u, ok := e.(unwrapper)
		if !ok {
			break
		}
		e = u.Unwrap()
	}
	return false
}
