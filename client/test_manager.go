package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"paxos/api"
	"paxos/constants"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TestCaseManager struct {
	servers map[int]*serverInfo
	lock    sync.Mutex
}

func NewTestCaseManager() *TestCaseManager {
	return &TestCaseManager{
		servers: make(map[int]*serverInfo),
	}
}

func (m *TestCaseManager) maxServerID() int {
	maxID := 0
	for id := range constants.ServerPorts {
		if id > maxID {
			maxID = id
		}
	}
	return maxID
}

func (m *TestCaseManager) Start(liveNodes []int) error {
	m.lock.Lock()
	if len(m.servers) != 0 {
		m.lock.Unlock()
		return fmt.Errorf("manager already started")
	}
	m.lock.Unlock()

	maxID := m.maxServerID()
	for i := 1; i <= maxID; i++ {
		os.RemoveAll(filepath.Join("data", fmt.Sprintf("server_%d", i)))
	}

	if err := ensureServerBinary(); err != nil {
		return err
	}

	seen := make(map[int]struct{})
	for _, id := range liveNodes {
		if id <= 0 || id > maxID {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		launchCmd := exec.Command("./server_bin", strconv.Itoa(id))
		if err := launchCmd.Start(); err != nil {
			return err
		}
		serverPid := launchCmd.Process.Pid
		m.lock.Lock()
		m.servers[id] = &serverInfo{pid: serverPid, cmd: launchCmd}
		m.lock.Unlock()
	}

	time.Sleep(2 * time.Second)

	return nil
}

func (m *TestCaseManager) Stop() {
	m.lock.Lock()
	defer m.lock.Unlock()

	maxID := m.maxServerID()

	for i := 1; i <= maxID; i++ {
		os.RemoveAll(filepath.Join("data", fmt.Sprintf("server_%d", i)))
	}

	for _, s := range m.servers {
		if s.conn != nil {
			s.conn.Close()
		}
		if s.cmd != nil && s.cmd.Process != nil {
			s.cmd.Process.Kill()
		}
	}
	m.servers = make(map[int]*serverInfo)
}

func (m *TestCaseManager) getServerClient(serverID int) (api.ClientServerTxnsClient, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	s, ok := m.servers[serverID]
	if !ok {
		return nil, fmt.Errorf("server %d not found", serverID)
	}

	if s.conn == nil {
		address := fmt.Sprintf("localhost:%s", constants.ServerPorts[serverID])
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		s.conn = conn
	}

	return api.NewClientServerTxnsClient(s.conn), nil
}

func (m *TestCaseManager) getPrintClient(serverID int) (api.PaxosPrintInfoClient, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	s, ok := m.servers[serverID]
	if !ok {
		return nil, fmt.Errorf("server %d not found", serverID)
	}

	if s.conn == nil {
		address := fmt.Sprintf("localhost:%s", constants.ServerPorts[serverID])
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		s.conn = conn
	}

	return api.NewPaxosPrintInfoClient(s.conn), nil
}

func (m *TestCaseManager) FailNode(id int) error {
	client, err := m.getServerClient(id)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()
	_, err = client.SimulateNodeFailure(ctx, &api.Blank{})
	return err
}

func (m *TestCaseManager) RecoverNode(id int) error {
	client, err := m.getServerClient(id)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()
	_, err = client.RecoverNode(ctx, &api.Blank{})
	return err
}

func (m *TestCaseManager) initConnections() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	for id, s := range m.servers {
		if s.conn != nil {
			continue
		}
		address := fmt.Sprintf("localhost:%s", constants.ServerPorts[id])
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		s.conn = conn
	}
	return nil
}
