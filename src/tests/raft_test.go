package raft_test

import (
	"fmt"
	"os"

	"raft/src/server"
	"testing"
	"time"

	"github.com/lmittmann/tint"
	"github.com/stretchr/testify/assert"

	"log/slog"
)

func initLogger() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelDebug,
		}),
	))
}

const (
	BeginTestRaftPort = 5050
)

func Filter(ss []string, test func(string) bool) (ret []string) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}

func NewTestCluster(count int) []*server.Server {
	result := make([]*server.Server, 0, count)

	peers := []string{}
	for id := 0; id < count; id++ {
		peers = append(peers, fmt.Sprintf("localhost:%d", BeginTestRaftPort+id))
	}

	for id := int64(0); id < int64(count); id++ {
		newServer := server.NewServer(
			id,
			Filter(peers, func(s string) bool {
				return s != fmt.Sprintf("localhost:%d", BeginTestRaftPort+id)
			}),
		)

		go newServer.StartServer(fmt.Sprintf(":%d", BeginTestRaftPort+id))
		result = append(result, newServer)
	}

	return result
}

func TestLeaderElection(t *testing.T)  {
	initLogger()

	cluster := NewTestCluster(5)

	assert.Equal(t, int64(-1), cluster[0].GetLeaderID())

	time.Sleep(10 * time.Second)
	assert.Equal(t, int64(0), cluster[0].GetLeaderID())

	cluster[0].Stop()

	time.Sleep(15 * time.Second)

	assert.Greater(t, cluster[1].GetLeaderID(), int64(0))
	assert.Less(t, cluster[1].GetLeaderID(), int64(5))
}

func TestLogReplication(t *testing.T) {
	initLogger()

	cluster := NewTestCluster(5)
	time.Sleep(10 * time.Second)

	value := "2"
	success, err := cluster[0].ReplicateLogEntry("CREATE", "1", &value, nil)
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	time.Sleep(5 * time.Second)

	for id := 1; id < 5; id++ {
		logEntries := cluster[id].GetLogEntries()
		len := len(*logEntries)
		assert.Equal(t, 2, len)
		assert.Equal(t, server.LogEntry{
			Term: 1,
			Key: "1",
			Value: &value,
			Command: "CREATE",
			OldValue: nil,
		}, (*logEntries)[len - 1])
	}
}

func TestLogSync(t *testing.T) {
	initLogger()

	cluster := NewTestCluster(5)
	time.Sleep(10 * time.Second)
	cluster[1].Stop()

	value := "2"
	success, err := cluster[0].ReplicateLogEntry("CREATE", "1", &value, nil)
	assert.NoError(t, err)
	assert.Equal(t, true, success)
	success, err = cluster[0].ReplicateLogEntry("CREATE", "2", &value, nil)
	assert.NoError(t, err)
	assert.Equal(t, true, success)
	success, err = cluster[0].ReplicateLogEntry("CREATE", "3", &value, nil)
	assert.NoError(t, err)
	assert.Equal(t, true, success)

	time.Sleep(5 * time.Second)

	for id := 2; id < 5; id++ {
		assert.Equal(t, 4, len(*cluster[id].GetLogEntries()))
	}

	go cluster[1].StartServer(cluster[1].GrpcPort)
	time.Sleep(10 * time.Second)
	assert.Equal(t, 4, len(*cluster[1].GetLogEntries()))
}

