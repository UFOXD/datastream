// Package app provides the main application lifecycle management.
package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/UFOXD/datastream/internal/api"
	"github.com/UFOXD/datastream/pkg/config"
	"github.com/UFOXD/datastream/internal/coordinator"
	"github.com/UFOXD/datastream/internal/pipeline"
	"github.com/UFOXD/datastream/pkg/metrics"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Application represents the DataStream application.
type Application struct {
	config         *config.Config
	apiServer      *api.Server
	coordinator    pipeline.Coordinator
	taskManager    *pipeline.TaskManager
	statsCollector *metrics.StatsCollector

	// Shutdown state
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

// New creates a new Application instance.
func New(cfg *config.Config) (*Application, error) {
	app := &Application{
		config:     cfg,
		shutdownCh: make(chan struct{}),
	}

	// Initialize API server
	serverConfig := &api.ServerConfig{
		Addr:         cfg.Server.HTTPAddr,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}
	app.apiServer = api.NewServer(serverConfig)

	// Initialize coordinator
	nodeID := fmt.Sprintf("node-%d", time.Now().UnixNano())
	if cfg.Coordinator.Type == "etcd" || cfg.Coordinator.Backend == "etcd" {
		etcdConfig := &coordinator.EtcdConfig{
			Endpoints:   cfg.Coordinator.Etcd.Endpoints,
			DialTimeout: time.Duration(cfg.Coordinator.Etcd.DialTimeout) * time.Second,
			Username:    cfg.Coordinator.Etcd.Username,
			Password:    cfg.Coordinator.Etcd.Password,
			NodeID:      nodeID,
			TTL:         cfg.Coordinator.SessionTTL,
		}
		etcdCoord, err := coordinator.NewEtcdCoordinator(etcdConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd coordinator: %w", err)
		}
		app.coordinator = etcdCoord
	} else {
		app.coordinator = pipeline.NewMemoryCoordinator(nodeID)
	}

	// Initialize task manager
	app.taskManager = pipeline.NewTaskManager()
	app.taskManager.SetCluster(cfg.Cluster)

	// Connect components
	app.apiServer.SetTaskManager(app.taskManager)
	app.apiServer.SetCoordinator(app.coordinator)

	return app, nil
}

// Run starts the application and blocks until shutdown.
func (app *Application) Run(ctx context.Context) error {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start API server
	if err := app.apiServer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start API server: %w", err)
	}

	// Start metrics StatsCollector if enabled
	if app.config.Metrics.Enabled {
		app.statsCollector = metrics.NewStatsCollector(
			app.config.Cluster,
			app.config.Metrics.ScrapeInterval,
			app.config.Metrics.StatsTimeout,
		)
		app.taskManager.SetStatsCollector(app.statsCollector)
		go app.statsCollector.Run(ctx)
		log.Info("stats collector started",
			zap.String("cluster", app.config.Cluster),
			zap.Duration("interval", app.config.Metrics.ScrapeInterval),
		)
	} else {
		log.Info("metrics collection disabled")
	}

	log.Info("DataStream application started",
		zap.String("addr", app.config.Server.HTTPAddr),
	)

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for shutdown signal or context cancellation
	select {
	case sig := <-sigCh:
		log.Info("Received shutdown signal",
			zap.String("signal", sig.String()),
		)
		if sig == syscall.SIGHUP {
			// Reload config (future feature)
			log.Info("SIGHUP received, config reload not yet implemented")
			return nil
		}
	case <-ctx.Done():
		log.Info("Context cancelled, initiating shutdown")
	case <-app.shutdownCh:
		log.Info("Shutdown requested")
	}

	// Perform graceful shutdown
	return app.Shutdown()
}

// Shutdown performs graceful shutdown of all components.
func (app *Application) Shutdown() error {
	var err error
	app.shutdownOnce.Do(func() {
		err = app.doShutdown()
	})
	return err
}

func (app *Application) doShutdown() error {
	log.Info("Starting graceful shutdown")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown order (reverse of startup):
	// 1. Stop accepting new connections (API server)
	// 2. Stop all running tasks
	// 3. Close coordinator connection
	// 4. Flush any pending data

	// Step 1: Stop API server
	log.Info("Stopping API server")
	if err := app.apiServer.Stop(ctx); err != nil {
		log.Error("Failed to stop API server", zap.Error(err))
	} else {
		log.Info("API server stopped")
	}

	// Step 2: Stop all tasks
	log.Info("Stopping all tasks")
	if app.taskManager != nil {
		tasks := app.taskManager.List()
		for _, task := range tasks {
			if task.Status == pipeline.TaskStatusRunning {
				log.Info("Stopping task",
					zap.String("taskId", task.ID),
					zap.String("taskName", task.Name),
				)
				if err := app.taskManager.Stop(ctx, task.ID); err != nil {
					log.Error("Failed to stop task",
						zap.String("taskId", task.ID),
						zap.Error(err),
					)
				}
			}
		}
		log.Info("All tasks stopped")
	}

	// Step 3: Close coordinator
	log.Info("Closing coordinator connection")
	if closer, ok := app.coordinator.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			log.Error("Failed to close coordinator", zap.Error(err))
		} else {
			log.Info("Coordinator connection closed")
		}
	}

	// Step 4: Final cleanup
	log.Info("Shutdown complete")

	return nil
}

// RequestShutdown requests the application to shutdown.
func (app *Application) RequestShutdown() {
	select {
	case app.shutdownCh <- struct{}{}:
	default:
		// Shutdown already requested
	}
}

// GetTaskManager returns the task manager.
func (app *Application) GetTaskManager() *pipeline.TaskManager {
	return app.taskManager
}

// GetCoordinator returns the coordinator.
func (app *Application) GetCoordinator() pipeline.Coordinator {
	return app.coordinator
}

// GetAPIServer returns the API server.
func (app *Application) GetAPIServer() *api.Server {
	return app.apiServer
}
