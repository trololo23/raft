package httphandler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"raft/src/server"
)

type Server struct {
	server *server.Server
}

func NewServer(raft *server.Server) *Server {
	return &Server{
		server: raft,
	}
}

func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	if value, ok := server.Get(key); ok {
		w.Write([]byte(fmt.Sprintf("%v", value)))
	} else {
		http.Error(w, "Key not found", http.StatusNotFound)
	}
}

func (s *Server) CreateHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	success, err := s.server.ReplicateLogEntry("CREATE", req.Key, &req.Value, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to apply command, err: %v", err), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprintf("%v", success)))
}

func (s *Server) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	success, err := s.server.ReplicateLogEntry("DELETE", req.Key, nil, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to apply command, err: %v", err), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprintf("%v", success)))
}

func (s *Server) UpdateHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	success, err := s.server.ReplicateLogEntry("UPDATE", req.Key, &req.Value, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to apply command, err: %v", err), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprintf("%v", success)))
}

func (s *Server) CompareAndSwapHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key      string `json:"key"`
		OldValue string `json:"old_value"`
		NewValue string `json:"new_value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	success, err := s.server.ReplicateLogEntry("CAS", req.Key, &req.NewValue, &req.OldValue)
	if err != nil {
		http.Error(w, "Failed to apply command", http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprintf("%v", success)))
}