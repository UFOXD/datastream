package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/UFOXD/datastream/internal/source"
	"github.com/gorilla/mux"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// addTables adds tables to the sync scope.
func (s *Server) addTables(w http.ResponseWriter, r *http.Request) {
	if s.TableManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "table manager not available")
		return
	}

	var req struct {
		Tables []string `json:"tables"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if len(req.Tables) == 0 {
		s.writeError(w, http.StatusBadRequest, "tables must not be empty")
		return
	}

	if err := s.TableManager.AddTables(r.Context(), req.Tables); err != nil {
		if errors.Is(err, source.ErrTableAlreadyExists) {
			s.writeError(w, http.StatusConflict, "table already exists")
			return
		}
		if errors.Is(err, source.ErrInvalidConfig) {
			s.writeError(w, http.StatusBadRequest, "invalid table name format")
			return
		}
		log.Error("failed to add tables", zap.Error(err))
		s.writeError(w, http.StatusInternalServerError, "failed to add tables")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "added",
		"tables": req.Tables,
	})
}

// removeTables removes tables from the sync scope.
func (s *Server) removeTables(w http.ResponseWriter, r *http.Request) {
	if s.TableManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "table manager not available")
		return
	}

	var req struct {
		Tables []string `json:"tables"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if len(req.Tables) == 0 {
		s.writeError(w, http.StatusBadRequest, "tables must not be empty")
		return
	}

	if err := s.TableManager.RemoveTables(r.Context(), req.Tables); err != nil {
		if errors.Is(err, source.ErrTableNotFound) {
			s.writeError(w, http.StatusNotFound, "table not found")
			return
		}
		if errors.Is(err, source.ErrInvalidConfig) {
			s.writeError(w, http.StatusBadRequest, "invalid table name format")
			return
		}
		log.Error("failed to remove tables", zap.Error(err))
		s.writeError(w, http.StatusInternalServerError, "failed to remove tables")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "removed",
		"tables": req.Tables,
	})
}

// listTables lists all tables in the sync scope.
func (s *Server) listTables(w http.ResponseWriter, r *http.Request) {
	if s.TableManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "table manager not available")
		return
	}

	tables := s.TableManager.ListTables()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"tables": tables,
		"count":  len(tables),
	})
}

// getTableState gets the sync state of a specific table.
func (s *Server) getTableState(w http.ResponseWriter, r *http.Request) {
	if s.TableManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "table manager not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	state, err := s.TableManager.GetTableState(db, table)
	if err != nil {
		s.writeError(w, http.StatusNotFound, err.Error())
		return
	}

	resp := map[string]interface{}{
		"database": state.TableID.Database,
		"table":    state.TableID.Table,
		"status":   state.Status,
		"added_at": state.AddedAt,
	}
	if !state.SyncStarted.IsZero() {
		resp["sync_started"] = state.SyncStarted
	}
	if !state.PausedAt.IsZero() {
		resp["paused_at"] = state.PausedAt
	}
	if state.Error != nil {
		resp["error"] = state.Error.Error()
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// pauseTable pauses syncing of a specific table.
func (s *Server) pauseTable(w http.ResponseWriter, r *http.Request) {
	if s.TableManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "table manager not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	if err := s.TableManager.PauseTable(r.Context(), db, table); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

// resumeTable resumes syncing of a paused table.
func (s *Server) resumeTable(w http.ResponseWriter, r *http.Request) {
	if s.TableManager == nil {
		s.writeError(w, http.StatusServiceUnavailable, "table manager not available")
		return
	}

	vars := mux.Vars(r)
	db := vars["db"]
	table := vars["table"]

	if err := s.TableManager.ResumeTable(r.Context(), db, table); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}
