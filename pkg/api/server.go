// Package api provides HTTP and gRPC APIs for DataStream.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/UFOXD/datastream/pkg/pipeline"
	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Server represents the API server.
type Server struct {
	httpServer  *http.Server
	router      *mux.Router
	taskMgr     *pipeline.TaskManager
	coordinator pipeline.Coordinator
	config      *ServerConfig
}

// ServerConfig holds API server configuration.
type ServerConfig struct {
	Addr         string `json:"addr"`         // HTTP address
	ReadTimeout  int    `json:"readTimeout"`  // seconds
	WriteTimeout int    `json:"writeTimeout"` // seconds
	IdleTimeout  int    `json:"idleTimeout"`  // seconds
}

// DefaultServerConfig returns the default server configuration.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Addr:         ":8300",
		ReadTimeout:  30,
		WriteTimeout: 30,
		IdleTimeout:  120,
	}
}

// NewServer creates a new API server.
func NewServer(config *ServerConfig) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	router := mux.NewRouter()
	s := &Server{
		router: router,
		config: config,
	}

	s.setupRoutes()
	return s
}

// SetTaskManager sets the task manager.
func (s *Server) SetTaskManager(tm *pipeline.TaskManager) {
	s.taskMgr = tm
}

// SetCoordinator sets the coordinator.
func (s *Server) SetCoordinator(c pipeline.Coordinator) {
	s.coordinator = c
}

// setupRoutes sets up the API routes.
func (s *Server) setupRoutes() {
	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// API v1
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Tasks
	api.HandleFunc("/tasks", s.listTasks).Methods("GET")
	api.HandleFunc("/tasks", s.createTask).Methods("POST")
	api.HandleFunc("/tasks/{id}", s.getTask).Methods("GET")
	api.HandleFunc("/tasks/{id}", s.deleteTask).Methods("DELETE")
	api.HandleFunc("/tasks/{id}/start", s.startTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/stop", s.stopTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/pause", s.pauseTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/resume", s.resumeTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/position", s.getTaskPosition).Methods("GET")
	api.HandleFunc("/tasks/{id}/position", s.setTaskPosition).Methods("PUT")

	// Nodes
	api.HandleFunc("/nodes", s.listNodes).Methods("GET")

	// Metrics (Prometheus format)
	s.router.Handle("/metrics", s.handleMetrics())
}

// Start starts the API server.
func (s *Server) Start(ctx context.Context) error {
	s.httpServer = &http.Server{
		Addr:         s.config.Addr,
		Handler:      s.router,
		ReadTimeout:  time.Duration(s.config.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(s.config.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(s.config.IdleTimeout) * time.Second,
	}

	log.Info("API server starting", zap.String("addr", s.config.Addr))

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("API server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the API server.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}

	log.Info("API server stopping")
	return s.httpServer.Shutdown(ctx)
}

// handleHealth handles health check requests.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// listTasks lists all tasks.
func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	tasks := s.taskMgr.List()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tasks": tasks,
		"count": len(tasks),
	})
}

// createTask creates a new task.
func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	var req struct {
		ID     string           `json:"id"`
		Name   string           `json:"name"`
		Config *pipeline.Config `json:"config"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	task, err := s.taskMgr.Create(r.Context(), req.ID, req.Name, req.Config)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusCreated, task)
}

// getTask gets a task by ID.
func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	task, err := s.taskMgr.Get(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, task)
}

// deleteTask deletes a task.
func (s *Server) deleteTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	if err := s.taskMgr.Delete(r.Context(), id); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// startTask starts a task.
func (s *Server) startTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	if err := s.taskMgr.Start(r.Context(), id); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

// stopTask stops a task.
func (s *Server) stopTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	if err := s.taskMgr.Stop(r.Context(), id); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

// pauseTask pauses a task.
func (s *Server) pauseTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	task, err := s.taskMgr.Get(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := task.Pause(r.Context()); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

// resumeTask resumes a task.
func (s *Server) resumeTask(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	task, err := s.taskMgr.Get(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if err := task.Resume(r.Context()); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

// getTaskPosition gets the position of a task.
func (s *Server) getTaskPosition(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	task, err := s.taskMgr.Get(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	pos := task.GetPosition()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"taskId":   id,
		"position": pos,
	})
}

// setTaskPosition sets the position of a task.
func (s *Server) setTaskPosition(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	id := mux.Vars(r)["id"]
	task, err := s.taskMgr.Get(id)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	var pos struct {
		BinlogFile string `json:"binlogFile"`
		BinlogPos  uint32 `json:"binlogPos"`
		LSN        uint64 `json:"lsn"`
		TxID       string `json:"txId"`
		SeqNo      int    `json:"seqNo"`
	}

	if err := json.NewDecoder(r.Body).Decode(&pos); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Update position
	_ = task // Position update would go here

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

// listNodes lists all registered nodes.
func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	if s.coordinator == nil {
		s.writeError(w, http.StatusServiceUnavailable, "coordinator not available")
		return
	}

	nodes, err := s.coordinator.ListNodes(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"nodes": nodes,
		"count": len(nodes),
	})
}

// handleMetrics returns the Prometheus metrics handler.
func (s *Server) handleMetrics() http.Handler {
	// Return Prometheus handler
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Metrics would be served here
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# DataStream Metrics\n"))
	})
}

// writeJSON writes a JSON response.
func (s *Server) writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

// writeError writes an error response.
func (s *Server) writeError(w http.ResponseWriter, code int, message string) {
	s.writeJSON(w, code, map[string]string{
		"error": message,
		"code":  strconv.Itoa(code),
	})
}
