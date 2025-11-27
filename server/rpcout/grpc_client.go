package rpcout

import (
	"context"
	"fmt"
	"sync"
	"time"

	"2PC/server/api"
	"2PC/server/paxos"
	"2PC/server/rpcconv"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	selfID  paxos.ServerID
	logger  *zap.Logger
	clients map[paxos.ServerID]api.PaxosReplicationClient
	txn     map[paxos.ServerID]api.ClientServerTxnsClient
	conns   map[paxos.ServerID]*grpc.ClientConn
	mu      sync.RWMutex
}

func NewGRPCTransport(self paxos.ServerID, peers map[paxos.ServerID]string, logger *zap.Logger) (*GRPCTransport, error) {
	t := &GRPCTransport{
		selfID:  self,
		logger:  logger,
		clients: make(map[paxos.ServerID]api.PaxosReplicationClient),
		txn:     make(map[paxos.ServerID]api.ClientServerTxnsClient),
		conns:   make(map[paxos.ServerID]*grpc.ClientConn),
	}

	for id, addr := range peers {
		if id == self {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}

		t.clients[id] = api.NewPaxosReplicationClient(conn)
		t.txn[id] = api.NewClientServerTxnsClient(conn)
		t.conns[id] = conn
	}

	return t, nil
}

func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, conn := range t.conns {
		conn.Close()
		delete(t.conns, id)
		delete(t.clients, id)
		delete(t.txn, id)
	}
}

func (t *GRPCTransport) SendAccept(ctx context.Context, req paxos.AcceptRequest) (paxos.AcceptResponse, error) {
	client, err := t.clientFor(req.To)
	if err != nil {
		return paxos.AcceptResponse{}, err
	}

	bVal, bServer := rpcconv.BallotToProto(req.Ballot)
	rec := &api.LogRecord{
		SeqNum:    int32(req.SeqNum),
		BallotVal: bVal,
		ServerId:  bServer,
		Sender:    int32(req.Tx.Sender),
		Receiver:  int32(req.Tx.Receiver),
		Amount:    int32(req.Tx.Amount),
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.Accept(ctx, rec)
	if err != nil {
		return paxos.AcceptResponse{}, err
	}

	return paxos.AcceptResponse{
		From:   req.To,
		To:     req.From,
		SeqNum: req.SeqNum,
		Ballot: req.Ballot,
		Ok:     resp.Success,
	}, nil
}

func (t *GRPCTransport) SendPrepare(ctx context.Context, req paxos.PrepareRequest) (paxos.PromiseResponse, error) {
	client, err := t.clientFor(req.To)
	if err != nil {
		return paxos.PromiseResponse{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.Prepare(ctx, &api.PrepareReq{
		BallotVal: int32(req.Ballot.Val),
		ServerId:  rpcconv.ServerIDToProto(req.Ballot.ServerID),
	})
	if err != nil {
		return paxos.PromiseResponse{}, err
	}

	promise := paxos.PromiseResponse{
		From:    req.To,
		To:      req.From,
		Ballot:  rpcconv.ProtoToBallot(resp.BallotVal, resp.ServerId),
		Success: resp.Success,
	}

	for _, rec := range resp.AcceptLog {
		promise.Accepted = append(promise.Accepted, rpcconv.ProtoToLogEntry(rec))
	}

	return promise, nil
}

func (t *GRPCTransport) clientFor(id paxos.ServerID) (api.PaxosReplicationClient, error) {
	t.mu.RLock()
	client, ok := t.clients[id]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no client for server %s", string(id))
	}
	return client, nil
}

func (t *GRPCTransport) txnClientFor(id paxos.ServerID) (api.ClientServerTxnsClient, error) {
	t.mu.RLock()
	client, ok := t.txn[id]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no txn client for server %s", string(id))
	}
	return client, nil
}

func (t *GRPCTransport) SendClientRequest(ctx context.Context, dest paxos.ServerID, msg *api.Message) (*api.Reply, error) {
	client, err := t.txnClientFor(dest)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	return client.Request(ctx, msg)
}

func (t *GRPCTransport) SendCommit(ctx context.Context, req paxos.CommitRequest) error {
	client, err := t.clientFor(req.To)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err = client.Commit(ctx, rpcconv.LogEntryToProto(req.Entry))
	return err
}

func (t *GRPCTransport) SendNewView(ctx context.Context, req paxos.NewViewRequest) error {
	client, err := t.clientFor(req.To)
	if err != nil {
		return err
	}

	records := make([]*api.LogRecord, 0, len(req.Entries))
	for _, entry := range req.Entries {
		records = append(records, rpcconv.LogEntryToProto(entry))
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err = client.NewView(ctx, &api.NewViewReq{
		BallotVal: int32(req.Ballot.Val),
		ServerId:  rpcconv.ServerIDToProto(req.Ballot.ServerID),
		AcceptLog: records,
	})
	return err
}
