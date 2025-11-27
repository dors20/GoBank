package paxos

import (
	"context"
	"errors"
	"sort"
	"sync"

	"go.uber.org/zap"
)

type ServerID string

type SeqNum uint64

type ClientID int64

type Transaction struct {
	Sender   ClientID
	Receiver ClientID
	Amount   int64
}

type Ballot struct {
	Val      uint64
	ServerID ServerID
}

type AcceptRequest struct {
	From   ServerID
	To     ServerID
	SeqNum SeqNum
	Ballot Ballot
	Tx     Transaction
}

type AcceptResponse struct {
	From   ServerID
	To     ServerID
	SeqNum SeqNum
	Ballot Ballot
	Ok     bool
}

type PrepareRequest struct {
	From   ServerID
	To     ServerID
	Ballot Ballot
}

type PromiseResponse struct {
	From     ServerID
	To       ServerID
	Ballot   Ballot
	Accepted []LogEntry
	Success  bool
}

type LogEntry struct {
	SeqNum    SeqNum
	Ballot    Ballot
	Tx        Transaction
	Committed bool
}

type NewViewRequest struct {
	From    ServerID
	To      ServerID
	Ballot  Ballot
	Entries []LogEntry
}

type CommitRequest struct {
	From  ServerID
	To    ServerID
	Entry LogEntry
}

type PersistentState struct {
	LastBallot   Ballot
	PromisedBall Ballot
	CommitIndex  SeqNum
	LogEntries   []LogEntry
}

type Persistence interface {
	Load(ctx context.Context) (PersistentState, error)
	SaveBallots(ctx context.Context, last Ballot, promised Ballot) error
	SaveLogEntry(ctx context.Context, entry LogEntry) error
	SaveCommitIndex(ctx context.Context, idx SeqNum) error
}

type Transport interface {
	SendAccept(ctx context.Context, req AcceptRequest) (AcceptResponse, error)
	SendPrepare(ctx context.Context, req PrepareRequest) (PromiseResponse, error)
	SendNewView(ctx context.Context, req NewViewRequest) error
	SendCommit(ctx context.Context, req CommitRequest) error
}

type Config struct {
	Self        ServerID
	Peers       []ServerID
	Transport   Transport
	Logger      *zap.Logger
	OnCommit    func(LogEntry)
	Persistence Persistence
}

type Node struct {
	cfg Config

	mu           sync.Mutex
	log          map[SeqNum]LogEntry
	commitIndex  SeqNum
	nextSeq      SeqNum
	lastBallot   Ballot
	promisedBall Ballot
	isLeader     bool
	leaderID     ServerID
	logger       *zap.Logger
	onCommit     func(LogEntry)
	storage      Persistence
}

var ErrNoTransport = errors.New("no transport")

func NewNode(cfg Config) (*Node, error) {
	if cfg.Transport == nil {
		return nil, ErrNoTransport
	}

	node := &Node{
		cfg:      cfg,
		log:      make(map[SeqNum]LogEntry),
		leaderID: "",
		logger:   cfg.Logger,
		onCommit: cfg.OnCommit,
		storage:  cfg.Persistence,
		lastBallot: Ballot{
			Val:      1,
			ServerID: cfg.Self,
		},
		promisedBall: Ballot{
			Val:      1,
			ServerID: cfg.Self,
		},
	}

	if cfg.Persistence != nil {
		state, err := cfg.Persistence.Load(context.Background())
		if err != nil {
			return nil, err
		}

		if state.LastBallot != (Ballot{}) {
			node.lastBallot = maxBallot(node.lastBallot, state.LastBallot)
		}
		if state.PromisedBall != (Ballot{}) {
			node.promisedBall = maxBallot(node.promisedBall, state.PromisedBall)
		}

		for _, entry := range state.LogEntries {
			node.log[entry.SeqNum] = entry
			if entry.SeqNum > node.nextSeq {
				node.nextSeq = entry.SeqNum
			}
		}
		node.commitIndex = state.CommitIndex
	}

	return node, nil
}

func (n *Node) StartPrepare(ctx context.Context) (Ballot, error) {
	n.mu.Lock()
	n.lastBallot.Val++
	n.lastBallot.ServerID = n.cfg.Self
	n.promisedBall = n.lastBallot
	ballot := n.lastBallot
	n.mu.Unlock()
	n.persistBallots(ctx)

	req := PrepareRequest{
		From:   n.cfg.Self,
		Ballot: ballot,
	}

	totalNodes := len(n.cfg.Peers) + 1
	needed := majorityCount(totalNodes)
	okCount := 1
	promises := make([]PromiseResponse, 0, len(n.cfg.Peers))
	if okCount >= needed {
		n.onLeaderElected(ctx, ballot, promises)
		return ballot, nil
	}

	type respResult struct {
		ok      bool
		err     error
		promise PromiseResponse
	}

	resCh := make(chan respResult, len(n.cfg.Peers))

	send := func(to ServerID) {
		localReq := req
		localReq.To = to
		resp, err := n.cfg.Transport.SendPrepare(ctx, localReq)
		if err != nil {
			resCh <- respResult{ok: false, err: err}
			return
		}

		if !resp.Success {
			resCh <- respResult{ok: false, err: nil}
			return
		}

		resCh <- respResult{ok: true, promise: resp}
	}

	for _, peer := range n.cfg.Peers {
		p := peer
		go send(p)
	}

	for i := 0; i < len(n.cfg.Peers); i++ {
		select {
		case <-ctx.Done():
			return Ballot{}, ctx.Err()
		case r := <-resCh:
			if r.err != nil {
				continue
			}

			if r.ok {
				okCount++
				promises = append(promises, r.promise)
				if okCount >= needed {
					n.onLeaderElected(ctx, ballot, promises)
					return ballot, nil
				}
			}
		}
	}

	return Ballot{}, errors.New("failed to reach majority in prepare")
}

func (n *Node) onLeaderElected(ctx context.Context, ballot Ballot, promises []PromiseResponse) {
	entries := n.buildNewViewEntries(ballot, promises)
	if len(entries) > 0 {
		n.ApplyNewView(entries)
		n.broadcastNewView(ctx, ballot, entries)
	}

	n.mu.Lock()
	n.isLeader = true
	n.leaderID = n.cfg.Self
	n.mu.Unlock()
}

// buildNewViewEntries enforces Spec §1.3, p.4 by merging AcceptLogs from the promise quorum
// and inserting no-op commands for any missing sequence numbers before broadcasting NEW-VIEW.
func (n *Node) buildNewViewEntries(ballot Ballot, promises []PromiseResponse) []LogEntry {
	accepted := make(map[SeqNum]LogEntry)

	n.mu.Lock()
	for seq, entry := range n.log {
		accepted[seq] = entry
	}
	n.mu.Unlock()

	var maxSeq SeqNum
	for seq := range accepted {
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	for _, promise := range promises {
		for _, entry := range promise.Accepted {
			if entry.SeqNum > maxSeq {
				maxSeq = entry.SeqNum
			}
			existing, ok := accepted[entry.SeqNum]
			if !ok || compareBallot(entry.Ballot, existing.Ballot) > 0 {
				accepted[entry.SeqNum] = entry
			}
		}
	}

	if maxSeq == 0 {
		return nil
	}

	entries := make([]LogEntry, 0, maxSeq)
	for seq := SeqNum(1); seq <= maxSeq; seq++ {
		entry, ok := accepted[seq]
		if !ok {
			entry = LogEntry{
				SeqNum: seq,
				Ballot: ballot,
			}
		} else {
			entry.Ballot = ballot
		}
		entries = append(entries, entry)
	}
	return entries
}

func (n *Node) broadcastNewView(ctx context.Context, ballot Ballot, entries []LogEntry) {
	if len(entries) == 0 {
		return
	}

	for _, peer := range n.cfg.Peers {
		req := NewViewRequest{
			From:    n.cfg.Self,
			To:      peer,
			Ballot:  ballot,
			Entries: cloneEntries(entries),
		}

		go func(r NewViewRequest) {
			if err := n.cfg.Transport.SendNewView(ctx, r); err != nil && n.logger != nil {
				n.logger.Warn("new view send failed",
					zap.String("peer", string(r.To)),
					zap.Error(err))
			}
		}(req)
	}
}

func (n *Node) Replicate(ctx context.Context, tx Transaction) (SeqNum, error) {
	n.mu.Lock()
	n.nextSeq++
	nextSeq := n.nextSeq
	n.log[nextSeq] = LogEntry{
		SeqNum: nextSeq,
		Ballot: n.lastBallot,
		Tx:     tx,
	}
	ballot := n.lastBallot
	n.mu.Unlock()

	local := AcceptRequest{
		From:   n.cfg.Self,
		To:     n.cfg.Self,
		SeqNum: nextSeq,
		Ballot: ballot,
		Tx:     tx,
	}

	if resp := n.HandleAccept(ctx, local); !resp.Ok {
		return 0, errors.New("local accept rejected")
	}

	type respResult struct {
		ok  bool
		err error
	}

	totalNodes := len(n.cfg.Peers) + 1
	needed := majorityCount(totalNodes)
	okCount := 1
	if okCount >= needed {
		n.commit(ctx, nextSeq, ballot, tx)
		return nextSeq, nil
	}

	resCh := make(chan respResult, len(n.cfg.Peers))

	send := func(to ServerID) {
		localReq := AcceptRequest{
			From:   n.cfg.Self,
			To:     to,
			SeqNum: nextSeq,
			Ballot: ballot,
			Tx:     tx,
		}
		resp, err := n.cfg.Transport.SendAccept(ctx, localReq)
		if err != nil {
			resCh <- respResult{ok: false, err: err}
			return
		}

		if !resp.Ok {
			resCh <- respResult{ok: false, err: nil}
			return
		}

		resCh <- respResult{ok: true, err: nil}
	}

	for _, peer := range n.cfg.Peers {
		p := peer
		go send(p)
	}

	for i := 0; i < len(n.cfg.Peers); i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case r := <-resCh:
			if r.err != nil {
				continue
			}

			if r.ok {
				okCount++
				if okCount >= needed {
					n.commit(ctx, nextSeq, ballot, tx)
					return nextSeq, nil
				}
			}
		}
	}

	return 0, errors.New("failed to reach majority")
}

func (n *Node) HandleAccept(ctx context.Context, req AcceptRequest) AcceptResponse {
	n.mu.Lock()
	if compareBallot(req.Ballot, n.promisedBall) < 0 {
		resp := AcceptResponse{
			From:   n.cfg.Self,
			To:     req.From,
			SeqNum: req.SeqNum,
			Ballot: n.promisedBall,
			Ok:     false,
		}
		n.mu.Unlock()
		return resp
	}

	n.lastBallot = req.Ballot
	n.leaderID = req.From
	n.isLeader = n.leaderID == n.cfg.Self

	entry := LogEntry{
		SeqNum: req.SeqNum,
		Ballot: req.Ballot,
		Tx:     req.Tx,
	}
	existing, ok := n.log[req.SeqNum]
	if ok && existing.Committed {
		entry.Committed = true
	}
	n.log[req.SeqNum] = entry
	if req.SeqNum > n.nextSeq {
		n.nextSeq = req.SeqNum
	}

	resp := AcceptResponse{
		From:   n.cfg.Self,
		To:     req.From,
		SeqNum: req.SeqNum,
		Ballot: req.Ballot,
		Ok:     true,
	}
	n.mu.Unlock()

	n.persistLogEntry(ctx, entry)
	return resp
}

func (n *Node) HandlePrepare(ctx context.Context, req PrepareRequest) PromiseResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if compareBallot(req.Ballot, n.promisedBall) < 0 {
		return PromiseResponse{
			From:    n.cfg.Self,
			To:      req.From,
			Ballot:  n.promisedBall,
			Success: false,
		}
	}

	n.promisedBall = req.Ballot
	n.lastBallot = req.Ballot
	n.leaderID = req.From
	n.isLeader = n.leaderID == n.cfg.Self

	accepted := make([]LogEntry, 0, len(n.log))
	for _, entry := range n.log {
		accepted = append(accepted, entry)
	}
	sort.Slice(accepted, func(i, j int) bool {
		return accepted[i].SeqNum < accepted[j].SeqNum
	})

	return PromiseResponse{
		From:     n.cfg.Self,
		To:       req.From,
		Ballot:   req.Ballot,
		Accepted: accepted,
		Success:  true,
	}
}

func (n *Node) ApplyCommit(entry LogEntry) {
	committed := n.recordCommit(context.Background(), entry)
	n.handleCommit(context.Background(), committed, false)
}

func (n *Node) ApplyNewView(entries []LogEntry) {
	n.mu.Lock()
	toPersist := make([]LogEntry, 0, len(entries))
	toApply := make([]LogEntry, 0)
	for _, entry := range entries {
		existing, ok := n.log[entry.SeqNum]
		if entry.Committed && (!ok || !existing.Committed) {
			toApply = append(toApply, entry)
		}
		n.log[entry.SeqNum] = entry
		if entry.SeqNum > n.nextSeq {
			n.nextSeq = entry.SeqNum
		}
		toPersist = append(toPersist, entry)
	}
	prevCommit := n.commitIndex
	n.advanceCommitLocked()
	newCommit := n.commitIndex
	n.mu.Unlock()

	for _, entry := range toPersist {
		n.persistLogEntry(context.Background(), entry)
	}
	if newCommit != prevCommit {
		n.persistCommitIndex(context.Background(), newCommit)
	}

	for _, entry := range toApply {
		n.handleCommit(context.Background(), entry, false)
	}
}

func (n *Node) LastBallot() Ballot {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastBallot
}

func (n *Node) Committed() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()

	out := make([]LogEntry, 0, n.commitIndex)
	for seq := SeqNum(1); seq <= n.commitIndex; seq++ {
		if entry, ok := n.log[seq]; ok && entry.Committed {
			out = append(out, entry)
		} else {
			break
		}
	}

	return out
}

func (n *Node) Entry(seq SeqNum) (LogEntry, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	entry, ok := n.log[seq]
	return entry, ok
}

func (n *Node) Entries() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()

	keys := make([]SeqNum, 0, len(n.log))
	for seq := range n.log {
		keys = append(keys, seq)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	out := make([]LogEntry, 0, len(keys))
	for _, seq := range keys {
		out = append(out, n.log[seq])
	}
	return out
}

func (n *Node) commit(ctx context.Context, seq SeqNum, ballot Ballot, tx Transaction) {
	entry := LogEntry{
		SeqNum: seq,
		Ballot: ballot,
		Tx:     tx,
	}
	committed := n.recordCommit(ctx, entry)
	n.handleCommit(ctx, committed, true)
}

func (n *Node) advanceCommitLocked() {
	for {
		next := n.commitIndex + 1
		entry, ok := n.log[next]
		if !ok || !entry.Committed {
			break
		}
		n.commitIndex = next
	}
}

func cloneEntries(entries []LogEntry) []LogEntry {
	out := make([]LogEntry, len(entries))
	copy(out, entries)
	return out
}

func (n *Node) recordCommit(ctx context.Context, entry LogEntry) LogEntry {
	n.mu.Lock()
	entry.Committed = true
	n.log[entry.SeqNum] = entry
	if entry.SeqNum > n.nextSeq {
		n.nextSeq = entry.SeqNum
	}
	prevCommit := n.commitIndex
	n.advanceCommitLocked()
	newCommit := n.commitIndex
	n.mu.Unlock()

	n.persistLogEntry(ctx, entry)
	if newCommit != prevCommit {
		n.persistCommitIndex(ctx, newCommit)
	}
	return entry
}

func (n *Node) handleCommit(ctx context.Context, entry LogEntry, broadcast bool) {
	if n.onCommit != nil {
		n.onCommit(entry)
	}
	if !broadcast {
		return
	}
	n.broadcastCommit(ctx, entry)
}

func (n *Node) broadcastCommit(ctx context.Context, entry LogEntry) {
	if len(n.cfg.Peers) == 0 {
		return
	}
	for _, peer := range n.cfg.Peers {
		req := CommitRequest{
			From:  n.cfg.Self,
			To:    peer,
			Entry: entry,
		}
		go func(r CommitRequest) {
			sendCtx := ctx
			if sendCtx == nil || (sendCtx.Err() != nil) {
				sendCtx = context.Background()
			}
			if err := n.cfg.Transport.SendCommit(sendCtx, r); err != nil && n.logger != nil {
				n.logger.Warn("commit send failed",
					zap.String("peer", string(r.To)),
					zap.Error(err))
			}
		}(req)
	}
}

func compareBallot(a, b Ballot) int {
	if a.Val < b.Val {
		return -1
	}
	if a.Val > b.Val {
		return 1
	}
	if a.ServerID < b.ServerID {
		return -1
	}
	if a.ServerID > b.ServerID {
		return 1
	}
	return 0
}

func majorityCount(total int) int {
	return total/2 + 1
}

func maxBallot(a, b Ballot) Ballot {
	if compareBallot(a, b) >= 0 {
		return a
	}
	return b
}

type LocalTransport struct {
	mu     sync.RWMutex
	nodes  map[ServerID]*Node
	active map[ServerID]bool
}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		nodes:  make(map[ServerID]*Node),
		active: make(map[ServerID]bool),
	}
}

func (t *LocalTransport) Register(id ServerID, n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = n
	t.active[id] = true
}

func (t *LocalTransport) SetActive(id ServerID, active bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.nodes[id]; !ok {
		return
	}
	t.active[id] = active
}

func (t *LocalTransport) SendAccept(ctx context.Context, req AcceptRequest) (AcceptResponse, error) {
	t.mu.RLock()
	target := t.nodes[req.To]
	active := t.active[req.To]
	t.mu.RUnlock()
	if target == nil || !active {
		return AcceptResponse{}, errors.New("target inactive")
	}
	return target.HandleAccept(ctx, req), nil
}

func (t *LocalTransport) SendPrepare(ctx context.Context, req PrepareRequest) (PromiseResponse, error) {
	t.mu.RLock()
	target := t.nodes[req.To]
	active := t.active[req.To]
	t.mu.RUnlock()
	if target == nil || !active {
		return PromiseResponse{}, errors.New("target inactive")
	}
	return target.HandlePrepare(ctx, req), nil
}

func (t *LocalTransport) SendNewView(ctx context.Context, req NewViewRequest) error {
	t.mu.RLock()
	target := t.nodes[req.To]
	active := t.active[req.To]
	t.mu.RUnlock()
	if target == nil || !active {
		return errors.New("target inactive")
	}
	target.ApplyNewView(req.Entries)
	return nil
}

func (t *LocalTransport) SendCommit(ctx context.Context, req CommitRequest) error {
	t.mu.RLock()
	target := t.nodes[req.To]
	active := t.active[req.To]
	t.mu.RUnlock()
	if target == nil || !active {
		return errors.New("target inactive")
	}
	target.ApplyCommit(req.Entry)
	return nil
}

func (n *Node) EnsureLeader(ctx context.Context) error {
	n.mu.Lock()
	leader := n.isLeader
	n.mu.Unlock()
	if leader {
		return nil
	}
	_, err := n.StartPrepare(ctx)
	return err
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.isLeader
}

func (n *Node) LeaderID() ServerID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *Node) persistBallots(ctx context.Context) {
	if n.storage == nil {
		return
	}
	ctx = normalizeCtx(ctx)
	if err := n.storage.SaveBallots(ctx, n.lastBallot, n.promisedBall); err != nil && n.logger != nil {
		n.logger.Warn("persist ballots failed", zap.Error(err))
	}
}

func (n *Node) persistLogEntry(ctx context.Context, entry LogEntry) {
	if n.storage == nil {
		return
	}
	ctx = normalizeCtx(ctx)
	if err := n.storage.SaveLogEntry(ctx, entry); err != nil && n.logger != nil {
		n.logger.Warn("persist log entry failed",
			zap.Error(err),
			zap.Uint64("seq", uint64(entry.SeqNum)))
	}
}

func (n *Node) persistCommitIndex(ctx context.Context, idx SeqNum) {
	if n.storage == nil {
		return
	}
	ctx = normalizeCtx(ctx)
	if err := n.storage.SaveCommitIndex(ctx, idx); err != nil && n.logger != nil {
		n.logger.Warn("persist commit index failed",
			zap.Error(err),
			zap.Uint64("commit_index", uint64(idx)))
	}
}

func normalizeCtx(ctx context.Context) context.Context {
	if ctx == nil || ctx.Err() != nil {
		return context.Background()
	}
	return ctx
}
