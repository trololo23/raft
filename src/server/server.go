package server

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"raft/src/proto/proto"
	"sync"
	"time"

	"google.golang.org/grpc"
)

//////////////////////// STRUCTS

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

type LogEntry struct {
	Key      string
	Value    *string
	OldValue *string
	Term     int64
	Command  string
}

///////////////////////// TIME

type TimeModule struct {
	Timer   *time.Timer
	Timeout time.Duration
}

func (module *TimeModule) Reset(callback func()) {
	module.Stop()
	module.Timer = time.AfterFunc(module.Timeout, callback)
}

func (module *TimeModule) Stop() {
	if module.Timer != nil {
		module.Timer.Stop()
	}
}

////////////////////////// SERVER

type Server struct {
	proto.UnimplementedNodeServer

	// Identification
	id       int64
	leaderID int64

	// Term and voting
	currentTerm  int64
	lastVotedFor int64

	// Log management
	log         []LogEntry
	lastApply   int64
	commitIndex int64
	nextIndex   map[string]int64

	// State management
	state int

	// Timing modules
	election   TimeModule
	heartbeats TimeModule

	// Networking
	grpcModule *grpc.Server
	GrpcPort   string
	peers      []string

	// Synchronization
	mu sync.Mutex
}

func NewServer(id int64, peers []string) *Server {
	server := &Server{
		// Identification
		id:       id,
		leaderID: -1,

		// Term and voting
		currentTerm:  0,
		lastVotedFor: -1,

		// Log management
		log: []LogEntry{
			{
				Term:    0,
				Command: "init",
			},
		},
		commitIndex: 0,
		lastApply:   0,
		nextIndex:   make(map[string]int64),

		// State management
		state: FOLLOWER,

		// Timing modules
		election: TimeModule{
			Timeout: time.Second * time.Duration(5+id*3),
		},
		heartbeats: TimeModule{
			Timeout: time.Second * 3,
		},

		// Networking
		peers: peers,
	}

	server.election.Reset(server.electionProcess)

	return server
}

func (serv *Server) StartServer(grpcPort string) {
	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("Connection failed %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterNodeServer(grpcServer, serv)
	serv.grpcModule = grpcServer
	serv.GrpcPort = grpcPort

	slog.Info("Grpc started on", "port", grpcPort, "ID", serv.id)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Grpc failed: %v", err)
	}
}

////////////////////////////////////// ELECTION

func (serv *Server) electionProcess() {
	serv.mu.Lock()
	defer serv.mu.Unlock()

	slog.Warn("Election timeout. Start election process", "ID", serv.id)

	votes := 1
	serv.election.Reset(serv.electionProcess)

	serv.currentTerm++
	serv.state = CANDIDATE
	serv.lastVotedFor = serv.id

	for _, peer := range serv.peers {
		go serv.requestVoteFromPeer(peer, &votes)
	}
}

func (serv *Server) requestVoteFromPeer(peer string, votes *int) {
	response, err := sendRequestVote(peer, &proto.VoteRequest{
		Term:         serv.currentTerm,
		CandidateID:  serv.id,
		LastLogIndex: int64(len(serv.log) - 1),
		LastLogTerm:  int64(serv.log[len(serv.log)-1].Term),
	})

	if err != nil {
		slog.Error("Failed to send vote request to", "peer", peer, "err", err, "ID", serv.id)
		return
	}

	if response.VoteGranted {
		serv.mu.Lock()
		defer serv.mu.Unlock()
		slog.Info("Get vote from", "peer", peer, "ID", serv.id)
		*votes++
		if *votes > len(serv.peers)/2 && serv.state == CANDIDATE {
			slog.Info("Current leader is", "ID", serv.id)
			serv.state = LEADER
			serv.leaderID = serv.id
			serv.heartbeats.Reset(serv.sendHeartbeats)
			serv.election.Stop()
		}
	}
}

func (serv *Server) RequestVote(ctx context.Context, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	slog.Info("Received RequestVote", "candidate", req.CandidateID, "request_term", req.Term, "current_term", serv.currentTerm, "ID", serv.id)

	if req.Term < serv.currentTerm {
		slog.Debug("Got RequestVote from old server", "ID", serv.id, "Requester", req.CandidateID)
		return &proto.VoteResponse{Term: serv.currentTerm, VoteGranted: false}, nil
	}

	serv.election.Reset(serv.electionProcess)

	if serv.lastVotedFor == -1 || req.Term > serv.currentTerm {
		serv.currentTerm = req.Term
		serv.lastVotedFor = req.CandidateID
		return &proto.VoteResponse{Term: serv.currentTerm, VoteGranted: true}, nil
	}

	return &proto.VoteResponse{Term: serv.currentTerm, VoteGranted: false}, nil
}

////////////////////////// HEARTBEATS

func (serv *Server) sendHeartbeats() {
	serv.mu.Lock()
	defer serv.mu.Unlock()

	for _, peer := range serv.peers {
		go serv.handlePeerHeartbeat(peer)
	}

	serv.heartbeats.Reset(serv.sendHeartbeats)
}

func (serv *Server) handlePeerHeartbeat(peer string) {
	slog.Debug("Send heartbeat to", "peer", peer)

	serv.mu.Lock()
	nextIndex, exists := serv.nextIndex[peer]
	if !exists {
		serv.nextIndex[peer] = 0
		nextIndex = 0
	}
	serv.mu.Unlock()

	for {
		req := serv.prepareAppendEntriesRequest(peer, nextIndex)

		resp, err := sendAppendEntries(peer, req)

		if err != nil {
			slog.Error("heartbeat from leader error", "error", err, "ID", serv.id, "node", peer)
			return
		}

		slog.Debug("Got heartbeat from", "peer", peer)

		if serv.processAppendEntriesResponse(peer, resp, nextIndex) {
			break
		}
	}
}

func (serv *Server) prepareAppendEntriesRequest(peer string, nextIndex int64) *proto.AppendEntriesRequest {
	serv.mu.Lock()
	defer serv.mu.Unlock()

	entries := serv.log[nextIndex:]
	entriesProto := make([]*proto.LogEntry, len(entries))
	for i, entry := range entries {
		entriesProto[i] = &proto.LogEntry{
			Term:     entry.Term,
			Command:  entry.Command,
			Key:      entry.Key,
			Value:    entry.Value,
			OldValue: entry.OldValue,
		}
	}

	var PrevLogTerm int64
	if nextIndex > 0 {
		PrevLogTerm = serv.log[nextIndex-1].Term
	}

	return &proto.AppendEntriesRequest{
		Term:         serv.currentTerm,
		LeaderID:     serv.id,
		LeaderCommit: serv.commitIndex,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  PrevLogTerm,
		Entries:      entriesProto,
	}
}

func (serv *Server) processAppendEntriesResponse(peer string, resp *proto.AppendEntriesResponse, nextIndex int64) bool {
	serv.mu.Lock()
	defer serv.mu.Unlock()

	if resp.Success {
		serv.nextIndex[peer] = nextIndex + int64(len(serv.log[nextIndex:]))
		return true
	}

	serv.nextIndex[peer] = nextIndex - 1
	slog.Info("Replica not in sync! Decrementing next index and retrying", "leader", serv.id, "node", peer)
	return false
}

func (serv *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	slog.Info("AppendEntries received", "node", serv.id, "leader", req.LeaderID)

	if req.Term < serv.currentTerm {
		return &proto.AppendEntriesResponse{Term: serv.currentTerm, Success: false}, nil
	}

	serv.currentTerm = req.Term
	serv.lastVotedFor = -1
	serv.state = FOLLOWER
	serv.leaderID = req.LeaderID
	serv.election.Reset(serv.electionProcess)

	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= int64(len(serv.log)) || serv.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			return &proto.AppendEntriesResponse{Term: serv.currentTerm, Success: false}, nil
		}
	}

	serv.appendEntries(req)

	// Apply entries
	if req.LeaderCommit > serv.commitIndex {
		serv.mu.Lock()
		defer serv.mu.Unlock()

		for i := serv.commitIndex; i <= req.LeaderCommit; i++ {
			if i == 0 {
				continue
			}
			entry := serv.log[i]
			slog.Info("Apply entry", "ID", serv.id, "entry", entry)
			ProcessWrite(entry.Command, entry.Key, entry.Value, entry.OldValue)
		}

		serv.commitIndex = req.LeaderCommit
		serv.lastApply = serv.commitIndex
	}

	return &proto.AppendEntriesResponse{Term: serv.currentTerm, Success: true}, nil
}

///////////////////////////////////////////// REQUESTS

func (s *Server) waitForMajorityAcknowledgment(ackCh chan bool) bool {
	ackCount := 1
	for i := 0; i < len(s.peers); i++ {
		if <-ackCh {
			ackCount++
		}
		if ackCount > len(s.peers)/2 {
			return true
		}
	}
	return false
}

func (s *Server) ReplicateLogEntry(command, key string, value, oldValue *string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateLeaderState(); err != nil {
		return false, err
	}

	entry := s.prepareLogEntry(command, key, value, oldValue)
	prevLogIndex, prevLogTerm := s.getLastLogInfo()

	slog.Info("Appended entry to master", "ID", s.id, "entry", entry)
	s.log = append(s.log, entry)

	ackCh := make(chan bool, len(s.peers))
	for _, peer := range s.peers {
		go func(peer string) {
			req := &proto.AppendEntriesRequest{
				Term:         s.currentTerm,
				LeaderID:     s.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries: []*proto.LogEntry{
					{
						Term:     entry.Term,
						Command:  entry.Command,
						Key:      entry.Key,
						Value:    entry.Value,
						OldValue: entry.OldValue,
					},
				},
				LeaderCommit: s.commitIndex,
			}

			resp, err := sendAppendEntries(peer, req)
			if err == nil && resp.Success {
				ackCh <- true
				s.mu.Lock()
				s.nextIndex[peer]++
				s.mu.Unlock()
			} else {
				if err != nil {
					slog.Error("Failed AppendEntryRequest", "ID", s.id, "error", err)
				}
				ackCh <- false
			}
		}(peer)
	}

	// Wait for majority of acknowledgments
	if s.waitForMajorityAcknowledgment(ackCh) {
		s.commitIndex = int64(len(s.log) - 1)
		slog.Info("Commiting entry", "ID", s.id, "CommitId", s.commitIndex, "entry", entry)

		if s.commitIndex > s.lastApply {
			for i := s.lastApply + 1; i <= s.commitIndex; i++ {
				entry := s.log[i]
				slog.Info("Apply entry", "ID", s.id, "entry", entry)
				ProcessWrite(entry.Command, entry.Key, entry.Value, entry.OldValue)
			}

			s.lastApply = s.commitIndex
		}

		return true, nil
	}

	return false, fmt.Errorf("Can't replicate %+v", entry)
}

func (s *Server) validateLeaderState() error {
	if s.state != LEADER {
		return fmt.Errorf("cannot handle write on replica. LeaderID: %v", s.leaderID)
	}
	return nil
}

func (s *Server) prepareLogEntry(command, key string, value, oldValue *string) LogEntry {
	return LogEntry{
		Term:     s.currentTerm,
		Command:  command,
		Key:      key,
		Value:    value,
		OldValue: oldValue,
	}
}

func (s *Server) getLastLogInfo() (prevLogIndex, prevLogTerm int64) {
	prevLogIndex = int64(len(s.log) - 1)
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	return prevLogIndex, prevLogTerm
}

func (serv *Server) GetLeaderID() int64 {
	serv.mu.Lock()
	defer serv.mu.Unlock()
	return serv.leaderID
}

func (serv *Server) GetLogEntries() *[]LogEntry {
	serv.mu.Lock()
	defer serv.mu.Unlock()
	return &serv.log
}

func (s *Server) appendEntries(req *proto.AppendEntriesRequest) {
	for i, entry := range req.Entries {
		logIndex := req.PrevLogIndex + int64(i) + 1

		if logIndex < int64(len(s.log)) {
			if s.log[logIndex].Term != req.Term {
				s.log = s.log[:logIndex]
			} else {
				continue
			}
		}

		s.log = append(s.log, LogEntry{
			Term:     entry.Term,
			Command:  entry.Command,
			Key:      entry.Key,
			Value:    entry.Value,
			OldValue: entry.OldValue,
		})
	}
}

//////////////////////////////////////////////// TEST NEEDS

func (serv *Server) Stop() {
	slog.Warn("STOP SERVER", "ID", serv.id)

	serv.grpcModule.GracefulStop()
	serv.election.Stop()
	serv.heartbeats.Stop()
}

/////////////////////////////////////////////////////////// KV

var kv sync.Map

func Get(key any) (any, bool) {
	return kv.Load(key)
}

func ProcessWrite(command, key string, value, oldValue *string) (bool, error) {
	if len(command) == 0 {
		slog.Error("Invalid command", "command", command)
		return false, fmt.Errorf("Empty command")
	}

	switch command {
	case "CREATE":
		if value == nil {
			slog.Error("Null value", "key", key)
			return false, fmt.Errorf("Null value")
		}
		if _, exists := kv.Load(key); exists {
			return false, fmt.Errorf("already exists")
		}
		kv.Store(key, *value)
	case "DELETE":
		_, existed := kv.LoadAndDelete(key)
		return existed, nil
	case "UPDATE":
		if value == nil {
			slog.Error("Null value", "key", key)
		}
		_, exists := kv.Load(key)
		if !exists {
			return false, nil
		}
		kv.Store(key, *value)
	case "CAS":
		if value == nil {
			slog.Error("Null new value", "key", key)
			return false, fmt.Errorf("Null new value")
		}
		if oldValue == nil {
			slog.Error("Null old value", "key", key)
			return false, fmt.Errorf("Null old value")
		}
		return kv.CompareAndSwap(key, *oldValue, *value), nil
	default:
		slog.Error("Invalid command: expected CREATE/DELETE/UPDATE/CAS", "command", command)
		return false, fmt.Errorf("Invalid command: expected CREATE/DELETE/UPDATE/CAS, got %v", command)
	}

	return true, nil
}
