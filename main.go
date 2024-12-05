package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"log"
	"raft/src/httphandler"
	"github.com/lmittmann/tint"
	"raft/src/server"
)

func initLogger() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelInfo,
		}),
	))
}

func parseFlags() (int64, int, int, []string) {
	var (
		id       int64
		raftPort int
		httpPort int
	)
	flag.Int64Var(&id, "id", 0, "Unique identifier for the replica set")
	flag.IntVar(&raftPort, "raft_port", 0, "Port number for the Raft consensus protocol")
	flag.IntVar(&httpPort, "http_port", 0, "Port number for the HTTP server to handle user requests")
	flag.Parse()
	peers := flag.Args()
	return id, raftPort, httpPort, peers
}

func setupHTTPHandlers(handler *httphandler.Server) {
	http.HandleFunc("/get/{key}", handler.GetHandler)
	http.HandleFunc("/create", handler.CreateHandler)
	http.HandleFunc("/delete", handler.DeleteHandler)
	http.HandleFunc("/update", handler.UpdateHandler)
	http.HandleFunc("/cas", handler.CompareAndSwapHandler)
}

func startServers(raftServer *server.Server, raftPort, httpPort int) {
	go raftServer.StartServer(fmt.Sprintf(":%d", raftPort))
	slog.Info("Up HTTP server on port", httpPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
		log.Fatalf("HTTP server failed to start: %v", err)
	}
}

func main() {
	initLogger()
	id, raftPort, httpPort, peers := parseFlags()
	slog.Info("Replica:", id, "Peers:", peers)

	raftServer := server.NewServer(id, peers)
	httpServer := httphandler.NewServer(raftServer)

	setupHTTPHandlers(httpServer)
	startServers(raftServer, raftPort, httpPort)
}