package rpcin

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"2PC/constants"
	"2PC/server/api"
	"2PC/server/paxos"
	"2PC/server/rpcconv"
	"2PC/server/store"

	"go.uber.org/zap"
)

type ClientForwarder interface {
	SendClientRequest(ctx context.Context, dest paxos.ServerID, msg *api.Message) (*api.Reply, error)
}

type KVServer struct {
	api.UnimplementedKVServer

	store  *store.Store
	logger *zap.Logger
}

func New(store *store.Store, logger *zap.Logger) *KVServer {
	return &KVServer{
		store:  store,
		logger: logger,
	}
}

func (s *KVServer) ReadOnlyRequest(ctx context.Context, req *api.ReadOnlyPayload) (*api.ReadOnlyResponse, error) {
	value, err := s.store.Get(ctx, []byte(req.Key))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return &api.ReadOnlyResponse{
				Found: false,
			}, nil
		}

		return &api.ReadOnlyResponse{
			Error: err.Error(),
		}, nil
	}

	return &api.ReadOnlyResponse{
		Value: value,
		Found: true,
	}, nil
}

func (s *KVServer) IntraShardRWReq(ctx context.Context, req *api.IntraShardRWPayload) (*api.IntraShardRWResp, error) {
	if req.Delete {
		err := s.store.Delete(ctx, []byte(req.Key))
		if err != nil {
			if err == store.ErrKeyNotFound {
				return &api.IntraShardRWResp{
					Found: false,
				}, nil
			}

			return &api.IntraShardRWResp{
				Error: err.Error(),
			}, nil
		}

		return &api.IntraShardRWResp{
			Found: true,
		}, nil
	}

	err := s.store.Put(ctx, []byte(req.Key), req.Value)
	if err != nil {
		return &api.IntraShardRWResp{
			Error: err.Error(),
		}, nil
	}

	return &api.IntraShardRWResp{
		Found: true,
	}, nil
}

func (s *KVServer) InterShardRWReq(ctx context.Context, req *api.InterShardRWPayload) (*api.InterShardRWResp, error) {
	if req.Delete {
		err := s.store.Delete(ctx, []byte(req.Key))
		if err != nil {
			if err == store.ErrKeyNotFound {
				return &api.InterShardRWResp{
					Found: false,
				}, nil
			}

			return &api.InterShardRWResp{
				Error: err.Error(),
			}, nil
		}

		return &api.InterShardRWResp{
			Found: true,
		}, nil
	}

	err := s.store.Put(ctx, []byte(req.Key), req.Value)
	if err != nil {
		return &api.InterShardRWResp{
			Error: err.Error(),
		}, nil
	}

	return &api.InterShardRWResp{
		Found: true,
	}, nil
}

type PaxosServer struct {
	api.UnimplementedPaxosReplicationServer

	node   *paxos.Node
	self   paxos.ServerID
	store  *store.Store
	logger *zap.Logger
}

func NewPaxosServer(node *paxos.Node, self paxos.ServerID, st *store.Store, logger *zap.Logger) *PaxosServer {
	return &PaxosServer{
		node:   node,
		self:   self,
		store:  st,
		logger: logger,
	}
}

func (s *PaxosServer) Accept(ctx context.Context, in *api.LogRecord) (*api.AcceptResp, error) {
	req := paxos.AcceptRequest{
		From:   rpcconv.ProtoToServerID(in.ServerId),
		To:     s.self,
		SeqNum: paxos.SeqNum(in.SeqNum),
		Ballot: rpcconv.ProtoToBallot(in.BallotVal, in.ServerId),
		Tx: paxos.Transaction{
			Sender:   paxos.ClientID(in.Sender),
			Receiver: paxos.ClientID(in.Receiver),
			Amount:   int64(in.Amount),
		},
	}

	resp := s.node.HandleAccept(ctx, req)

	return &api.AcceptResp{
		Success: resp.Ok,
		NodeId:  rpcconv.ServerIDToProto(s.self),
		Record:  in,
	}, nil
}

func (s *PaxosServer) Commit(ctx context.Context, in *api.LogRecord) (*api.Blank, error) {
	entry := rpcconv.ProtoToLogEntry(in)
	entry.Committed = true
	s.node.ApplyCommit(entry)
	return &api.Blank{}, nil
}

func (s *PaxosServer) Prepare(ctx context.Context, in *api.PrepareReq) (*api.PromiseResp, error) {
	req := paxos.PrepareRequest{
		From:   rpcconv.ProtoToServerID(in.ServerId),
		To:     s.self,
		Ballot: rpcconv.ProtoToBallot(in.BallotVal, in.ServerId),
	}

	resp := s.node.HandlePrepare(ctx, req)

	promise := &api.PromiseResp{
		Success:   resp.Success,
		BallotVal: int32(resp.Ballot.Val),
		ServerId:  rpcconv.ServerIDToProto(resp.Ballot.ServerID),
	}

	for _, entry := range resp.Accepted {
		promise.AcceptLog = append(promise.AcceptLog, rpcconv.LogEntryToProto(entry))
	}

	return promise, nil
}

func (s *PaxosServer) NewView(ctx context.Context, in *api.NewViewReq) (*api.Blank, error) {
	entries := make([]paxos.LogEntry, 0, len(in.AcceptLog))
	for _, rec := range in.AcceptLog {
		entries = append(entries, rpcconv.ProtoToLogEntry(rec))
	}
	s.node.ApplyNewView(entries)
	return &api.Blank{}, nil
}

type ClientTxnServer struct {
	api.UnimplementedClientServerTxnsServer

	node      *paxos.Node
	self      paxos.ServerID
	forwarder ClientForwarder
	cache     *clientCache
	logger    *zap.Logger
}

func NewClientTxnServer(node *paxos.Node, self paxos.ServerID, forwarder ClientForwarder, logger *zap.Logger) *ClientTxnServer {
	return &ClientTxnServer{
		node:      node,
		self:      self,
		forwarder: forwarder,
		cache:     newClientCache(),
		logger:    logger,
	}
}

func (s *ClientTxnServer) Request(ctx context.Context, in *api.Message) (*api.Reply, error) {
	tx := rpcconv.ProtoToTransaction(in)

	if reply, ok := s.cache.check(in.ClientId, in.Timestamp); ok {
		return reply, nil
	}

	if !s.node.IsLeader() {
		leader := s.node.LeaderID()
		if leader != "" && leader != s.self && s.forwarder != nil {
			return s.forwarder.SendClientRequest(ctx, leader, in)
		}
		if err := s.node.EnsureLeader(ctx); err != nil {
			return s.failureReply(in), nil
		}
		if !s.node.IsLeader() {
			return s.failureReply(in), nil
		}
	}

	if _, err := s.node.Replicate(ctx, tx); err != nil {
		return s.failureReply(in), nil
	}

	reply := &api.Reply{
		BallotVal: int32(s.node.LastBallot().Val),
		ServerId:  rpcconv.ServerIDToProto(s.self),
		Timestamp: in.Timestamp,
		ClientId:  in.ClientId,
		Result:    true,
	}
	s.cache.store(in.ClientId, in.Timestamp, reply)

	return reply, nil
}

func (s *ClientTxnServer) failureReply(in *api.Message) *api.Reply {
	return &api.Reply{
		BallotVal: int32(s.node.LastBallot().Val),
		ServerId:  rpcconv.ServerIDToProto(s.self),
		Timestamp: in.Timestamp,
		ClientId:  in.ClientId,
		Result:    false,
	}
}

type clientCache struct {
	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	timestamp int64
	reply     *api.Reply
}

func newClientCache() *clientCache {
	return &clientCache{
		entries: make(map[string]cacheEntry),
	}
}

func (c *clientCache) check(clientID string, ts int64) (*api.Reply, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[clientID]
	if !ok {
		return nil, false
	}
	if ts < entry.timestamp {
		return entry.reply, true
	}
	if ts == entry.timestamp {
		return entry.reply, true
	}
	return nil, false
}

func (c *clientCache) store(clientID string, ts int64, reply *api.Reply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[clientID] = cacheEntry{
		timestamp: ts,
		reply:     reply,
	}
}

type PrintServer struct {
	api.UnimplementedPaxosPrintInfoServer

	node   *paxos.Node
	store  *store.Store
	logger *zap.Logger
}

func NewPrintServer(node *paxos.Node, st *store.Store, logger *zap.Logger) *PrintServer {
	return &PrintServer{
		node:   node,
		store:  st,
		logger: logger,
	}
}

func (s *PrintServer) PrintLog(ctx context.Context, _ *api.Blank) (*api.Logs, error) {
	entries := s.node.Entries()
	out := make([]*api.LogRecord, 0, len(entries))
	for _, entry := range entries {
		out = append(out, rpcconv.LogEntryToProto(entry))
	}
	return &api.Logs{Logs: out}, nil
}

func (s *PrintServer) PrintDB(ctx context.Context, _ *api.Blank) (*api.Vault, error) {
	return &api.Vault{Vault: map[string]int32{}}, nil
}

func (s *PrintServer) PrintStatus(ctx context.Context, info *api.RequestInfo) (*api.Status, error) {
	entries := s.node.Entries()
	idx := int(info.SeqNum) - 1
	if idx >= 0 && idx < len(entries) {
		entry := entries[idx]
		if entry.Committed {
			return &api.Status{Status: api.TxnState_TXN_STATE_COMMITTED}, nil
		}
		return &api.Status{Status: api.TxnState_TXN_STATE_ACCEPTED}, nil
	}
	return &api.Status{Status: api.TxnState_TXN_STATE_UNKNOWN}, nil
}

func (s *PrintServer) PrintView(ctx context.Context, _ *api.Blank) (*api.ViewLogs, error) {
	return &api.ViewLogs{}, nil
}

type TxnApplier struct {
	store  *store.Store
	logger *zap.Logger
	mu     sync.Mutex
	queue  map[paxos.SeqNum]paxos.LogEntry
	seq    paxos.SeqNum
}

func NewTxnApplier(st *store.Store, logger *zap.Logger) *TxnApplier {
	return &TxnApplier{
		store:  st,
		logger: logger,
		queue:  make(map[paxos.SeqNum]paxos.LogEntry),
	}
}

func (a *TxnApplier) Apply(entry paxos.LogEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.queue[entry.SeqNum] = entry
	for {
		next := a.seq + 1
		record, ok := a.queue[next]
		if !ok {
			break
		}
		a.applyOne(record)
		delete(a.queue, next)
		a.seq = next
	}
}

func (a *TxnApplier) applyOne(entry paxos.LogEntry) {
	ctx := context.Background()
	amount := entry.Tx.Amount
	if amount <= 0 {
		return
	}

	senderKey := accountKey(entry.Tx.Sender)
	receiverKey := accountKey(entry.Tx.Receiver)

	senderBal, err := a.balance(ctx, senderKey)
	if err != nil {
		a.logError("balance read failed", err, entry)
		return
	}

	if senderBal < amount {
		a.logWarn("insufficient balance", entry, senderBal, amount)
		return
	}

	receiverBal, err := a.balance(ctx, receiverKey)
	if err != nil {
		a.logError("balance read failed", err, entry)
		return
	}

	senderBal -= amount
	receiverBal += amount

	if err := a.persist(ctx, senderKey, senderBal); err != nil {
		a.logError("balance write failed", err, entry)
		return
	}

	if err := a.persist(ctx, receiverKey, receiverBal); err != nil {
		a.logError("balance write failed", err, entry)
		return
	}
}

func (a *TxnApplier) balance(ctx context.Context, key []byte) (int64, error) {
	val, err := a.store.Get(ctx, key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return constants.StartingBalance, nil
		}
		return 0, err
	}
	return strconv.ParseInt(string(val), 10, 64)
}

func (a *TxnApplier) persist(ctx context.Context, key []byte, bal int64) error {
	return a.store.Put(ctx, key, []byte(strconv.FormatInt(bal, 10)))
}

func accountKey(id paxos.ClientID) []byte {
	return []byte(fmt.Sprintf("acct:%d", id))
}

func (a *TxnApplier) logWarn(msg string, entry paxos.LogEntry, senderBal int64, amount int64) {
	if a.logger == nil {
		return
	}
	a.logger.Warn(msg,
		zap.Int64("seq", int64(entry.SeqNum)),
		zap.Int64("sender", int64(entry.Tx.Sender)),
		zap.Int64("receiver", int64(entry.Tx.Receiver)),
		zap.Int64("amount", amount),
		zap.Int64("balance", senderBal),
	)
}

func (a *TxnApplier) logError(msg string, err error, entry paxos.LogEntry) {
	if a.logger == nil {
		return
	}
	a.logger.Error(msg,
		zap.Error(err),
		zap.Int64("seq", int64(entry.SeqNum)),
		zap.Int64("sender", int64(entry.Tx.Sender)),
		zap.Int64("receiver", int64(entry.Tx.Receiver)),
		zap.Int64("amount", entry.Tx.Amount),
	)
}
