// Package api provides HTTP and gRPC APIs for DataStream.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"time"

	"github.com/UFOXD/datastream/internal/pipeline"
	"github.com/UFOXD/datastream/internal/source"
	"github.com/UFOXD/datastream/pkg/event"
	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server represents the API server.
type Server struct {
	httpServer   *http.Server
	router       *mux.Router
	taskMgr      *pipeline.TaskManager
	coordinator  pipeline.Coordinator
	TableManager *source.TableManager
	config       *ServerConfig
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
	api.HandleFunc("/tasks/{id}", s.updateTask).Methods("PUT")
	api.HandleFunc("/tasks/{id}/start", s.startTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/stop", s.stopTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/pause", s.pauseTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/resume", s.resumeTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/restart", s.restartTask).Methods("POST")
	api.HandleFunc("/tasks/{id}/status", s.getTaskStatus).Methods("GET")
	api.HandleFunc("/tasks/{id}/progress", s.getTaskProgress).Methods("GET")
	api.HandleFunc("/tasks/{id}/position", s.getTaskPosition).Methods("GET")
	api.HandleFunc("/tasks/{id}/position", s.setTaskPosition).Methods("PUT")

	// Tables
	api.HandleFunc("/tables", s.listTables).Methods("GET")
	api.HandleFunc("/tables", s.addTables).Methods("POST")
	api.HandleFunc("/tables", s.removeTables).Methods("DELETE")
	api.HandleFunc("/tables/{db}/{table}", s.getTableState).Methods("GET")
	api.HandleFunc("/tables/{db}/{table}/pause", s.pauseTable).Methods("POST")
	api.HandleFunc("/tables/{db}/{table}/resume", s.resumeTable).Methods("POST")

	// Nodes
	api.HandleFunc("/nodes", s.listNodes).Methods("GET")
	api.HandleFunc("/nodes/{id}", s.deleteNode).Methods("DELETE")
	api.HandleFunc("/nodes/{id}/drain", s.drainNode).Methods("POST")

	// Cluster
	api.HandleFunc("/cluster/status", s.getClusterStatus).Methods("GET")
	api.HandleFunc("/cluster/leader", s.getClusterLeader).Methods("GET")
	api.HandleFunc("/cluster/rebalance", s.rebalanceCluster).Methods("POST")

	// Readiness probe
	s.router.HandleFunc("/ready", s.readyCheck).Methods("GET")

	// Diagnostics
	s.router.HandleFunc("/diagnose", s.diagnose).Methods("GET")

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

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
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

// updateTask updates a task's configuration.
func (s *Server) updateTask(w http.ResponseWriter, r *http.Request) {
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

	var req struct {
		Name   string          `json:"name"`
		Config *pipeline.Config `json:"config"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	task.Update(req.Name, req.Config)

	s.writeJSON(w, http.StatusOK, task)
}

// restartTask restarts a task.
func (s *Server) restartTask(w http.ResponseWriter, r *http.Request) {
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

	if err := task.Stop(r.Context()); err != nil {
		log.Warn("stop during restart returned error", zap.String("id", id), zap.Error(err))
	}

	if err := task.Start(r.Context()); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "restarted"})
}

// getTaskStatus returns detailed task status.
func (s *Server) getTaskStatus(w http.ResponseWriter, r *http.Request) {
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

	result := map[string]interface{}{
		"taskId":    id,
		"status":    task.GetStatus(),
		"createdAt": task.CreatedAt,
		"updatedAt": task.UpdatedAt,
	}

	if task.Pipeline != nil {
		pipelineStatus := task.Pipeline.Status()
		result["pipelineState"] = pipelineStatus.State
		result["statistics"] = pipelineStatus.Statistics
	}

	s.writeJSON(w, http.StatusOK, result)
}

// getTaskProgress returns sync progress for a task.
func (s *Server) getTaskProgress(w http.ResponseWriter, r *http.Request) {
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

	result := map[string]interface{}{
		"taskId":   id,
		"position": task.GetPosition(),
	}

	if task.Pipeline != nil {
		stats := task.Pipeline.Status().Statistics
		result["eventsRead"] = stats.EventsRead
		result["eventsWritten"] = stats.EventsWritten
		result["eventsFailed"] = stats.EventsFailed
		result["currentLag"] = stats.CurrentLag
	} else {
		result["eventsRead"] = int64(0)
		result["eventsWritten"] = int64(0)
		result["eventsFailed"] = int64(0)
		result["currentLag"] = int64(0)
	}

	s.writeJSON(w, http.StatusOK, result)
}

// readyCheck handles the readiness probe.
func (s *Server) readyCheck(w http.ResponseWriter, r *http.Request) {
	ready := s.taskMgr != nil
	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	s.writeJSON(w, status, map[string]bool{"ready": ready})
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
	task.SetPosition(&event.Position{
		BinlogFile: pos.BinlogFile,
		BinlogPos:  pos.BinlogPos,
		LSN:        pos.LSN,
		TxID:       pos.TxID,
		SeqNo:      pos.SeqNo,
	})

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

// deleteNode unregisters a node from the cluster.
func (s *Server) deleteNode(w http.ResponseWriter, r *http.Request) {
	if s.coordinator == nil {
		s.writeError(w, http.StatusServiceUnavailable, "coordinator not available")
		return
	}

	id := mux.Vars(r)["id"]
	if err := s.coordinator.UnregisterNode(r.Context(), id); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "unregistered"})
}

// drainNode stops all tasks on a node (single-node assumption: stops all tasks).
func (s *Server) drainNode(w http.ResponseWriter, r *http.Request) {
	if s.taskMgr == nil {
		s.writeError(w, http.StatusServiceUnavailable, "task manager not available")
		return
	}

	tasks := s.taskMgr.List()
	stopped := 0
	for _, t := range tasks {
		if t.GetStatus() == pipeline.TaskStatusRunning {
			if err := t.Stop(r.Context()); err != nil {
				log.Warn("failed to stop task during drain",
					zap.String("taskId", t.ID), zap.Error(err))
				continue
			}
			stopped++
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":       "drained",
		"tasksStopped": stopped,
	})
}

// getClusterStatus returns cluster overview including nodes and task counts.
func (s *Server) getClusterStatus(w http.ResponseWriter, r *http.Request) {
	if s.coordinator == nil {
		s.writeError(w, http.StatusServiceUnavailable, "coordinator not available")
		return
	}

	nodes, err := s.coordinator.ListNodes(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := map[string]interface{}{
		"nodes":     nodes,
		"nodeCount": len(nodes),
	}

	if s.taskMgr != nil {
		tasks := s.taskMgr.List()
		result["taskCount"] = len(tasks)
	}

	s.writeJSON(w, http.StatusOK, result)
}

// getClusterLeader returns the current cluster leader information.
func (s *Server) getClusterLeader(w http.ResponseWriter, r *http.Request) {
	if s.coordinator == nil {
		s.writeError(w, http.StatusServiceUnavailable, "coordinator not available")
		return
	}

	nodes, err := s.coordinator.ListNodes(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// In single-node mode, return the first registered node as the leader.
	if len(nodes) > 0 {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"leader": nodes[0],
		})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"leader": nil,
	})
}

// rebalanceCluster triggers task rebalancing across cluster nodes.
func (s *Server) rebalanceCluster(w http.ResponseWriter, r *http.Request) {
	if s.coordinator == nil {
		s.writeError(w, http.StatusServiceUnavailable, "coordinator not available")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"status":  "rebalanced",
		"message": "single-node mode, no rebalance needed",
	})
}

// diagnose returns diagnostic runtime information.
func (s *Server) diagnose(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	info := map[string]interface{}{
		"goVersion":  runtime.Version(),
		"goroutines": runtime.NumGoroutine(),
		"heapAlloc":  m.HeapAlloc,
		"heapSys":    m.HeapSys,
		"numGC":      m.NumGC,
	}
	if s.taskMgr != nil {
		info["tasks"] = len(s.taskMgr.List())
	}
	s.writeJSON(w, http.StatusOK, info)
}

// handleMetrics returns the Prometheus metrics handler.
func (s *Server) handleMetrics() http.Handler {
	return promhttp.Handler()
}

// apiResponse is the standard API response envelope.
// All API responses use this format: {"code": 0, "message": "success", "data": {...}}
// For errors: {"code": 404, "message": "task not found"}
type apiResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// writeJSON writes a JSON response wrapped in the standard envelope.
func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(apiResponse{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

// writeError writes an error response in the standard envelope format.
func (s *Server) writeError(w http.ResponseWriter, statusCode int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(apiResponse{
		Code:    statusCode,
		Message: msg,
	})
}
