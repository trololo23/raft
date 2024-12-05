package httphandler

import (
	"fmt"
	"net/http"
	"raft/src/server"
	"sync"
)

var kv sync.Map

func Get(key any) (any, bool) {
	return kv.Load(key)
}

type Handler struct {
	server *server.Server
}

func NewHandler(nodeServer *server.Server) *Handler {
	return &Handler{
		server: nodeServer,
	}
}

func (s *Handler) GetHandler(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	if value, ok := Get(key); ok {
		w.Write([]byte(fmt.Sprintf("%v", value)))
	} else {
		http.Error(w, "Key not found", http.StatusNotFound)
	}
}
