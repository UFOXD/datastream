package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/your-org/datastream/pkg/config"
	"github.com/your-org/datastream/pkg/logutil"
	"github.com/your-org/datastream/pkg/version"
)

var (
	// Version is set at build time
	Version = "unknown"
	// BuildTime is set at build time
	BuildTime = "unknown"
)

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
		logutil.StringField("version", version.Version),
		logutil.StringField("build", version.BuildTime),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("Received shutdown signal", logutil.StringField("signal", sig.String()))
		cancel()
	}()

	// TODO: Start the application
	logger.Info("DataStream started")

	// Wait for shutdown
	<-ctx.Done()
	logger.Info("DataStream stopped")
}
