package server

import (
	"context"
	"raft/src/proto/proto"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func sendRequestVote(address string, request *proto.VoteRequest) (*proto.VoteResponse, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := proto.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	return client.RequestVote(ctx, request)
}

func sendAppendEntries(address string, request *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := proto.NewNodeClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	return client.AppendEntries(ctx, request)
}
