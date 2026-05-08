package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/UFOXD/datastream/pkg/app"
	"github.com/UFOXD/datastream/pkg/config"
	"github.com/UFOXD/datastream/pkg/logutil"
	"github.com/UFOXD/datastream/pkg/version"
)

var (
	// Version is set at build time
	Version = "unknown"
	// BuildTime is set at build time
	BuildTime = "unknown"
)

func init() {
	version.Version = Version
	version.BuildTime = BuildTime
}

func main() {
	// Parse flags
	configPath := flag.String("config", "configs/datastream.toml", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.GetInfo().String())
		os.Exit(0)
	}

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	if err := logutil.InitLogger(&cfg.Log); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	logger := logutil.L()
	logger.Info("DataStream starting",
		logutil.StringField("version", Version),
		logutil.StringField("build", BuildTime),
	)

	// Create application
	application, err := app.New(cfg)
	if err != nil {
		logger.Error("Failed to create application", logutil.ErrorField(err))
		os.Exit(1)
	}

	// Run application (blocks until shutdown)
	ctx := context.Background()
	if err := application.Run(ctx); err != nil {
		logger.Error("Application error", logutil.ErrorField(err))
		os.Exit(1)
	}

	logger.Info("DataStream stopped gracefully")
}
