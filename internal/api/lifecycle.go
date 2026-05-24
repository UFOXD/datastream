package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/UFOXD/datastream/internal/source"
	"github.com/gorilla/mux"
)

// getTaskDetail returns table lifecycle summary for a task.
func (s *Server) getTaskDetail(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	store := s.scheduler.Store()
	taskID := s.scheduler.TaskID()

	tables, err := store.List(context.Background(), taskID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list tables: %v", err))
		return
	}

	// Build summary counts
	summary := map[string]int{
		"total":         len(tables),
		"pending":       0,
		"snapshotting":  0,
		"catching_up":   0,
		"streaming":     0,
		"error":         0,
		"paused":        0,
	}

	for _, lc := range tables {
		state := lc.GetState()
		switch state {
		case source.TableStatePending:
			summary["pending"]++
		case source.TableStateSnapshotting:
			summary["snapshotting"]++
		case source.TableStateCatchingUp:
			summary["catching_up"]++
		case source.TableStateStreaming:
			summary["streaming"]++
		case source.TableStateError:
			summary["error"]++
		case source.TableStatePaused:
			summary["paused"]++
		}
	}

	globalMinPos := s.scheduler.GetGlobalMinPosition()

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tables":            tables,
		"summary":           summary,
		"globalMinPosition": globalMinPos,
	})
}

// getTableErrors returns all tables in error state.
func (s *Server) getTableErrors(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	errTables, err := s.scheduler.ListErrors()
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list error tables: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tables": errTables,
		"count":  len(errTables),
	})
}

// restartTables restarts specified tables and/or schemas.
func (s *Server) restartTables(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	var req struct {
		Tables  []string `json:"tables"`
		Schemas []string `json:"schemas"`
		Force   bool     `json:"force"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var restartedTables []string
	var errors []string

	// Restart individual tables
	for _, t := range req.Tables {
		db, table := parseDBTable(t)
		if db == "" || table == "" {
			errors = append(errors, fmt.Sprintf("invalid table format: %s", t))
			continue
		}
		tableID := source.TableID{Database: db, Table: table}
		if err := s.scheduler.RestartTable(tableID, nil, req.Force); err != nil {
			errors = append(errors, fmt.Sprintf("restart %s: %v", t, err))
		} else {
			restartedTables = append(restartedTables, t)
		}
	}

	// Restart schemas
	var restartedSchemas []string
	for _, schema := range req.Schemas {
		restarted, err := s.scheduler.RestartSchema(schema, nil, req.Force)
		if err != nil {
			errors = append(errors, fmt.Sprintf("restart schema %s: %v", schema, err))
		} else {
			restartedSchemas = append(restartedSchemas, schema)
			for _, tid := range restarted {
				restartedTables = append(restartedTables, tid.String())
			}
		}
	}

	result := map[string]interface{}{
		"restartedTables":  restartedTables,
		"restartedSchemas": restartedSchemas,
	}
	if len(errors) > 0 {
		result["errors"] = errors
	}

	s.writeJSON(w, http.StatusOK, result)
}

// getTableLifecycleState returns the lifecycle state of a specific table.
func (s *Server) getTableLifecycleState(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	store := s.scheduler.Store()
	taskID := s.scheduler.TaskID()

	tableID := source.TableID{Database: db, Table: table}
	lc, err := store.Get(context.Background(), taskID, tableID)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("table %s.%s not found: %v", db, table, err))
		return
	}

	s.writeJSON(w, http.StatusOK, lc)
}

// pauseTableLifecycle pauses a table's lifecycle.
func (s *Server) pauseTableLifecycle(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	tableID := source.TableID{Database: db, Table: table}
	if err := s.scheduler.PauseTable(tableID); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to pause table: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

// resumeTableLifecycle resumes a paused table's lifecycle.
func (s *Server) resumeTableLifecycle(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	tableID := source.TableID{Database: db, Table: table}
	if err := s.scheduler.ResumeTable(tableID); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to resume table: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

// retryTableLifecycle retries a table that is in error state.
func (s *Server) retryTableLifecycle(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	store := s.scheduler.Store()
	taskID := s.scheduler.TaskID()

	tableID := source.TableID{Database: db, Table: table}
	lc, err := store.Get(context.Background(), taskID, tableID)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("table %s.%s not found: %v", db, table, err))
		return
	}

	if lc.GetState() != source.TableStateError {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("table %s.%s is not in error state", db, table))
		return
	}

	// Restart the table from its current snapshot position
	if err := s.scheduler.RestartTable(tableID, lc.SnapshotPosition, true); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to retry table: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "retrying"})
}

// skipTableError skips the current error and transitions the table to the next logical state.
func (s *Server) skipTableError(w http.ResponseWriter, r *http.Request) {
	if s.scheduler == nil {
		s.writeError(w, http.StatusServiceUnavailable, "lifecycle scheduler not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	store := s.scheduler.Store()
	taskID := s.scheduler.TaskID()

	tableID := source.TableID{Database: db, Table: table}
	lc, err := store.Get(context.Background(), taskID, tableID)
	if err != nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("table %s.%s not found: %v", db, table, err))
		return
	}

	if lc.GetState() != source.TableStateError {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("table %s.%s is not in error state", db, table))
		return
	}

	// Determine next state based on previous state
	prevState := lc.PreviousState
	var nextState source.TableState
	switch prevState {
	case source.TableStatePending, source.TableStateSnapshotting:
		// Skip to catching_up
		nextState = source.TableStateCatchingUp
	case source.TableStateCatchingUp:
		// Skip to streaming
		nextState = source.TableStateStreaming
	default:
		// Default: skip to streaming
		nextState = source.TableStateStreaming
	}

	// Force the transition: clear error and set new state
	lc.LastError = ""
	lc.PreviousState = lc.State
	lc.State = nextState

	if err := store.Save(context.Background(), taskID, lc); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to save table state: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":   "skipped",
		"newState": string(nextState),
	})
}

// parseDBTable splits "db.table" into db and table components.
func parseDBTable(s string) (db, table string) {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return s[:i], s[i+1:]
		}
	}
	return "", ""
}
