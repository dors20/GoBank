package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"paxos/api"
	"paxos/constants"
	"paxos/logger"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Current Ballot of server
// should be in initialized to (1,serverID)
type BallotNumber struct {
	BallotVal int `json:"ballotval"`
	ServerID  int `json:"serverId"`
}

// Clien send m <sender, reciever, amt>
type ClientRequestTxn struct {
	Sender   string `json:"sender"`
	Reciever string `json:"reciever"`
	Amount   int    `json:"amount"`
}

// Structure of a single record that will be stored in this servers log
type LogRecord struct {
	SeqNum      int               `json:"seqNum"`
	Ballot      *BallotNumber     `json:"ballot"`
	Txn         *ClientRequestTxn `json:"txn"`
	IsCommitted bool              `json:"committed"`
	IsExecuted  bool              `json:"exececuted"`
	Phase       string            `json:"phase"`
	TxID        string            `json:"txId"`
}

type LogStore struct {
	lock     sync.Mutex
	records  map[int]*LogRecord
	logsPath string
}

// Just stores client balances and process newly committed transactions
type StateMachine struct {
	lock                  sync.Mutex
	vault                 map[string]int
	lastExecutedCommitNum int
	executionStatus       map[int]chan bool
	execPool              chan struct{}
	locks                 map[string]bool
	wal                   map[string]map[string]int
}

type Client struct {
	lastRequestTimeStamp int64
	lastResponse         *api.Reply
}

// maps unique client id to their request and last served timestamp
type ClientImpl struct {
	lock       sync.Mutex
	clientList map[string]*Client
}

type Peer struct {
	id   int
	conn *grpc.ClientConn
	api  api.PaxosReplicationClient
}

type PeerManager struct {
	lock  sync.Mutex
	peers map[int]*Peer
}

// The main server struct
type ServerImpl struct {
	lock            sync.Mutex
	id              int
	db              *badger.DB
	ballot          *BallotNumber
	seqNum          int
	leaderTimer     *time.Timer
	state           int
	clientManager   *ClientImpl
	peerManager     *PeerManager
	port            string
	leaderBallot    *BallotNumber
	leaderPulse     chan bool
	viewLog         []*api.NewViewReq
	tp              time.Duration
	lastPrepareRecv time.Time
	prepareChan     chan struct{}
	pendingPrepares map[string]*api.PrepareReq
	pendingMax      *api.PrepareReq
	isPromised      bool
	isRunning       bool
	switchLock      sync.Mutex
	logFetchLock    sync.Mutex
	logFetchRunning bool
	api.UnimplementedClientServerTxnsServer
	api.UnimplementedPaxosPrintInfoServer
	api.UnimplementedPaxosReplicationServer
}

/*
Check for AB lock sequence to avoid deadlock ( Like contentManagger and stateManager issue)

Questions to be answered:
		1. Do we route stale client requests to current leader? What if
		 the current leader didn't server that message ? Maybe re-route it to the old leader who served the message
		ANS: NO RIGHT that request might not have qourum
		2. when Leader asks followers to replicate can we send false in RPC for faster processing? Or if
			follower doesn't accept the log do we need to wait for the RPC to timeout(not ideal)
    	3. If a txn sender balance less than amt do we still commit transaction and only respond as failed?
		4. In normal flow if state transitions, from leader to anything else what happens to current reqs being processed
		5. Blind trust commit if accept fails check /fall25/cse535/cft-dors20/runs/Commit_before_Accept
TODO:
3) In client store connections of servers - Done
4) In server store connections for other nodes - Done
6) In client on failure needs to braodcast to all nodes
7) check all state transitions
8) Check if ballot val is updated properly
9) Waiting for T_p time before responding to new candidate
*/

var server *ServerImpl
var logStore *LogStore
var sm *StateMachine
var logs *zap.SugaredLogger

func dbPathForServer(id int) string {
	return filepath.Join("data", fmt.Sprintf("server_%d", id))
}

func shardOfAccount(account string) int {
	if account == "" {
		return -1
	}
	n, err := strconv.Atoi(account)
	if err != nil {
		return -1
	}
	if n >= 1 && n <= 3000 {
		return 0
	}
	if n >= 3001 && n <= 6000 {
		return 1
	}
	if n >= 6001 && n <= 9000 {
		return 2
	}
	return -1
}

func openDB(id int) *badger.DB {
	path := dbPathForServer(id)
	err := os.MkdirAll(path, 0755)
	if err != nil {
		logs.Fatalf("Failed to create DB directory %s: %v", path, err)
	}
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		logs.Fatalf("Failed to open DB at %s: %v", path, err)
	}
	return db
}

func loadStateFromDB() {
	if server == nil || server.db == nil {
		return
	}

	maxSeq := 0
	lastExecuted := 0

	err := server.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefixVault := []byte("vault:")
		prefixLog := []byte("log:")
		prefixWal := []byte("wal:")
		metaLastExecuted := []byte("meta:lastExecuted")

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			switch {
			case bytes.HasPrefix(key, prefixVault):
				account := string(key[len(prefixVault):])
				err := item.Value(func(v []byte) error {
					val, err := strconv.Atoi(string(v))
					if err != nil {
						return err
					}
					sm.vault[account] = val
					return nil
				})
				if err != nil {
					return err
				}
			case bytes.HasPrefix(key, prefixLog):
				err := item.Value(func(v []byte) error {
					var rec LogRecord
					if err := json.Unmarshal(v, &rec); err != nil {
						return err
					}
					r := rec
					logStore.records[r.SeqNum] = &r
					if r.SeqNum > maxSeq {
						maxSeq = r.SeqNum
					}
					if r.IsExecuted && r.SeqNum > lastExecuted {
						lastExecuted = r.SeqNum
					}
					return nil
				})
				if err != nil {
					return err
				}
			case bytes.HasPrefix(key, prefixWal):
				txID := string(key[len(prefixWal):])
				err := item.Value(func(v []byte) error {
					var entry map[string]int
					if err := json.Unmarshal(v, &entry); err != nil {
						return err
					}
					if sm.wal == nil {
						sm.wal = make(map[string]map[string]int)
					}
					sm.wal[txID] = entry
					return nil
				})
				if err != nil {
					return err
				}
			case bytes.Equal(key, metaLastExecuted):
				err := item.Value(func(v []byte) error {
					val, err := strconv.Atoi(string(v))
					if err != nil {
						return err
					}
					lastExecuted = val
					return nil
				})
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	if err != nil {
		logs.Warnf("Failed to load state from DB: %v", err)
	}

	sm.lastExecutedCommitNum = lastExecuted
	server.seqNum = maxSeq
}

func persistLogRecord(record *LogRecord) {
	if server == nil || server.db == nil || record == nil {
		return
	}

	data, err := json.Marshal(record)
	if err != nil {
		logs.Warnf("Failed to marshal log record for seq %d: %v", record.SeqNum, err)
		return
	}

	key := []byte(fmt.Sprintf("log:%d", record.SeqNum))
	err = server.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
	if err != nil {
		logs.Warnf("Failed to persist log record for seq %d: %v", record.SeqNum, err)
	}
}

func persistVaultEntry(account string, balance int) {
	if server == nil || server.db == nil {
		return
	}

	key := []byte("vault:" + account)
	value := []byte(strconv.Itoa(balance))

	err := server.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	if err != nil {
		logs.Warnf("Failed to persist vault entry for %s: %v", account, err)
	}
}

func persistLastExecuted(idx int) {
	if server == nil || server.db == nil {
		return
	}

	key := []byte("meta:lastExecuted")
	value := []byte(strconv.Itoa(idx))

	err := server.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	if err != nil {
		logs.Warnf("Failed to persist last executed index %d: %v", idx, err)
	}
}

func persistWAL(txID string, entry map[string]int) {
	if server == nil || server.db == nil || txID == "" || entry == nil {
		return
	}

	data, err := json.Marshal(entry)
	if err != nil {
		logs.Warnf("Failed to marshal WAL for tx %s: %v", txID, err)
		return
	}

	key := []byte("wal:" + txID)
	err = server.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
	if err != nil {
		logs.Warnf("Failed to persist WAL for tx %s: %v", txID, err)
	}
}

func deleteWAL(txID string) {
	if server == nil || server.db == nil || txID == "" {
		return
	}
	key := []byte("wal:" + txID)
	err := server.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if err != nil {
		logs.Warnf("Failed to delete WAL for tx %s: %v", txID, err)
	}
}

func startServer(id int, t time.Duration) {

	logs = logger.InitLogger(id, true)

	logs.Debug("Enter")
	defer logs.Debug("Exit")

	logStore = &LogStore{
		records:  make(map[int]*LogRecord),
		logsPath: fmt.Sprintf("logstore_%d.json", id),
	}

	cm := &ClientImpl{
		clientList: make(map[string]*Client),
	}

	sm = &StateMachine{
		vault:                 make(map[string]int),
		lastExecutedCommitNum: 0,
		executionStatus:       make(map[int]chan bool),
		execPool:              make(chan struct{}, 1),
		locks:                 make(map[string]bool),
		wal:                   make(map[string]map[string]int),
	}

	pm := &PeerManager{
		peers: make(map[int]*Peer),
	}
	s := constants.Follower

	db := openDB(id)

	server = &ServerImpl{
		id:              id,
		db:              db,
		ballot:          &BallotNumber{BallotVal: 1, ServerID: id},
		seqNum:          0,
		leaderTimer:     time.NewTimer(t),
		state:           s,
		clientManager:   cm,
		peerManager:     pm,
		leaderBallot:    &BallotNumber{BallotVal: 0, ServerID: 0},
		leaderPulse:     make(chan bool, 1),
		viewLog:         make([]*api.NewViewReq, 0),
		tp:              constants.PREPARE_TIMEOUT * time.Millisecond,
		prepareChan:     make(chan struct{}),
		pendingPrepares: make(map[string]*api.PrepareReq),
		isRunning:       true,
	}
	cluster := constants.ClusterOf(id)
	clusterLeader := cluster*constants.MAX_NODES + 1
	if id == clusterLeader {
		server.state = constants.Leader
		server.ballot.BallotVal = 1
		server.ballot.ServerID = id
		server.leaderBallot.BallotVal = 1
		server.leaderBallot.ServerID = id
	} else {
		server.leaderBallot.BallotVal = 1
		server.leaderBallot.ServerID = clusterLeader
	}
	server.port = constants.ServerPorts[id]
	server.peerManager.initPeerConnections(server.id) // Note - not doing as a singleton on becoming first time leader for simplicity
	loadStateFromDB()
	sm.startExec()
	go server.monitorLeader()
	go server.startHeartbeat()
	go server.logCatchupLoop()
	logs.Infof("Server Initialized successfully with serverId: %d and timer duration: %d", id, t)
}

func main() {

	if len(os.Args) < 2 {
		log.Fatalf("Server ID must be provided as a command-line argument")
	}
	serverIdString := os.Args[1]
	serverId, err := strconv.Atoi(serverIdString)
	if err != nil {
		log.Fatalf("Invalid Server ID: %s", serverIdString)
	}

	leaderTimeout := constants.LEADER_TIMEOUT_SECONDS * time.Millisecond
	startServer(serverId, leaderTimeout)
	logs.Infof("Server-%d is up and running. Waiting for requests on port %s", serverId, server.port)

	// Starting grpc server
	addr := fmt.Sprintf(":%s", server.port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logs.Fatalf("Failed to listen on port %s: %v", server.port, err)
	}

	grpcServer := grpc.NewServer()
	api.RegisterClientServerTxnsServer(grpcServer, server)
	// TODO MAYBE DEDICATED SERVER FOR PRINT
	api.RegisterPaxosPrintInfoServer(grpcServer, server)
	api.RegisterPaxosReplicationServer(grpcServer, server)
	logs.Infof("gRPC server listening at %s", server.port)
	err = grpcServer.Serve(lis)
	if err != nil {
		logs.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// func stateTransition(from int, to int) {

// 	// TODO
// }

// func isValidStateTransition(from int, to int) {
// 	//TODO
// }

/*
	NOTE
	txn in a , b , c
	lets say concensus for a take a while b,c committed but not executed
	we need to wait for a to commit
	Thats why implementing a channel that gets a pulse everytime we commit a txn, then we check if the prev reqs are committed, otherwise we wait for next pulse
*/

func (s *ServerImpl) Request(ctx context.Context, in *api.Message) (*api.Reply, error) {

	logs.Debug("Entry")
	defer logs.Debug("Exit")
	if !s.isServerRunning() {
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "SERVER_DOWN"}, s.downErr()
	}

	logs.Infof("Received transaction from %s to %s for amount %d", in.Sender, in.Receiver, in.Amount)

	// TODO leader forward
	// *NOTE*
	// We'll allow self transfer, its a redundant log but the system should handle it like a normal txn
	// Exact once semantics before the leader check other wise on failed req, all nodes will flood Leader
	// Changed exactly once semantics from lastReply to lastReq because of one observed duplicated exec
	// Scenario is if client sends a req and the req takes too long to process because of network delay and the client retries, then both the reqs can be executed
	s.clientManager.lock.Lock()
	client, ok := s.clientManager.clientList[in.ClientId]
	if !ok {
		client = &Client{}
		s.clientManager.clientList[in.ClientId] = client
		logs.Infof("First request from client %s. Creating client cache", in.ClientId)
	}

	if in.GetTimestamp() <= client.lastRequestTimeStamp && client.lastResponse != nil {
		logs.Infof("Recieved duplicate request from client-%s", in.ClientId)
		resp := client.lastResponse
		s.clientManager.lock.Unlock()
		return resp, nil
	}
	client.lastRequestTimeStamp = in.GetTimestamp()
	s.clientManager.lock.Unlock()

	s.lock.Lock()
	if s.state != constants.Leader {
		leaderID := s.leaderBallot.ServerID
		// TODO forwarding to current leader
		s.lock.Unlock()
		// Before first election or after recieving prepare from higher ballot
		if leaderID == 0 || leaderID == s.id {
			logs.Warnf("Cannot process this transaction, not current cluster leader and leader is unknown.")
			return &api.Reply{Result: false, ServerId: int32(s.id), Error: "NOT_LEADER"}, nil
		}

		logs.Infof("Forwarding request to perceived leader: %d", leaderID)
		s.peerManager.lock.Lock()
		leaderPeer, ok := s.peerManager.peers[leaderID]
		s.peerManager.lock.Unlock()

		if !ok {
			logs.Warnf("No connection found for leader %d", leaderID)
			return &api.Reply{Result: false, ServerId: int32(s.id), Error: "NOT_LEADER"}, nil
		}

		client := api.NewClientServerTxnsClient(leaderPeer.conn)
		forwardCtx, cancel := context.WithTimeout(context.Background(), constants.FORWARD_TIMEOUT*time.Millisecond)
		defer cancel()
		return client.Request(forwardCtx, in)
	}
	senderShard := shardOfAccount(in.Sender)
	receiverShard := shardOfAccount(in.Receiver)
	serverCluster := constants.ClusterOf(s.id)
	if senderShard != -1 && receiverShard != -1 && senderShard != receiverShard && senderShard == serverCluster {
		s.lock.Unlock()
		return s.handleCrossShardRequest(ctx, in, senderShard, receiverShard)
	}
	s.seqNum++
	currSeqNum := s.seqNum
	txID := fmt.Sprintf("%s:%s:%d:%d", in.Sender, in.Receiver, in.Amount, in.Timestamp)
	record := &LogRecord{
		SeqNum: currSeqNum,
		Ballot: &BallotNumber{BallotVal: int(s.ballot.BallotVal), ServerID: s.id},
		Txn: &ClientRequestTxn{
			Sender:   in.Sender,
			Reciever: in.Receiver,
			Amount:   int(in.Amount),
		},
		IsCommitted: false,
		Phase:       "N",
		TxID:        txID,
	}
	s.lock.Unlock()

	logStore.append(record)
	execChannel := sm.registerSignal(currSeqNum)
	quorum := int((constants.MAX_NODES / 2) + 1)

	ok = false
	attempts := 0
	for !ok {
		s.lock.Lock()
		isLeader := s.state == constants.Leader
		if !isLeader {
			s.lock.Unlock()
			return &api.Reply{Result: false, ServerId: int32(s.id), Error: "NOT_LEADER"}, nil
		}
		if !s.isServerRunning() {
			s.lock.Unlock()
			return &api.Reply{Result: false, ServerId: int32(s.id), Error: "SERVER_DOWN"}, s.downErr()
		}
		if s.ballot.BallotVal < s.leaderBallot.BallotVal || (s.ballot.BallotVal == s.leaderBallot.BallotVal && s.id < s.leaderBallot.ServerID) {
			s.state = constants.Follower
			s.lock.Unlock()
			return &api.Reply{Result: false, ServerId: int32(s.id), Error: "NOT_LEADER"}, s.downErr()
		}
		s.lock.Unlock()
		ok = s.peerManager.broadcastAccept(quorum, record, in.GetTimestamp())
		attempts++
		if !ok && attempts >= 3 {
			return &api.Reply{Result: false, ServerId: int32(s.id), Error: "INSUFFICIENT_QUORUM"}, nil
		}
	}

	logStore.markCommitted(currSeqNum)
	sm.applyTxn()
	s.peerManager.broadcastCommit(record)

	logs.Infof("Waiting for seqNum %d to be executed", currSeqNum)
	res := <-execChannel
	logs.Infof("Execution completed for seqNum %d, txn success: %t ", currSeqNum, res)

	s.lock.Lock()
	reply := &api.Reply{
		BallotVal: int32(s.ballot.BallotVal),
		ServerId:  int32(s.id),
		Timestamp: in.GetTimestamp(),
		ClientId:  in.GetClientId(),
		Result:    res,
	}
	s.lock.Unlock()

	s.clientManager.lock.Lock()
	defer s.clientManager.lock.Unlock()
	client, ok = s.clientManager.clientList[in.ClientId]
	if ok && client.lastRequestTimeStamp == in.GetTimestamp() {
		client.lastResponse = reply
	}
	return reply, nil

}

func (s *ServerImpl) handleCrossShardRequest(ctx context.Context, in *api.Message, _ int, receiverShard int) (*api.Reply, error) {
	txID := fmt.Sprintf("%s:%s:%d:%d", in.Sender, in.Receiver, in.Amount, in.Timestamp)
	if !sm.tryLockAccounts(in.Sender) {
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "LOCK_CONFLICT"}, nil
	}
	sm.lock.Lock()
	bal, ok := sm.vault[in.Sender]
	if !ok {
		bal = constants.INITIAL_BALANCE
	}
	if bal < int(in.Amount) {
		sm.lock.Unlock()
		sm.restoreBalances(txID)
		sm.unlockAccounts(in.Sender)
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "INSUFFICIENT_BALANCE"}, nil
	}
	sm.lock.Unlock()
	sm.snapshotBalances(txID, in.Sender)

	localPrepared, _ := s.proposeLogWithPhase("P", txID, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
	if !localPrepared {
		sm.restoreBalances(txID)
		sm.unlockAccounts(in.Sender)
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "PREPARE_FAILED"}, nil
	}

	targetCluster := receiverShard
	targetLeader := targetCluster*constants.MAX_NODES + 1
	port, ok := constants.ServerPorts[targetLeader]
	if !ok {
		sm.restoreBalances(txID)
		sm.unlockAccounts(in.Sender)
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "UNKNOWN_PARTICIPANT"}, nil
	}

	address := fmt.Sprintf("localhost:%s", port)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		sm.restoreBalances(txID)
		sm.unlockAccounts(in.Sender)
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "PARTICIPANT_UNREACHABLE"}, nil
	}
	defer conn.Close()

	client := api.NewClientServerTxnsClient(conn)
	pctx, pcancel := context.WithTimeout(ctx, constants.REQUEST_TIMEOUT*time.Millisecond)
	defer pcancel()
	prepReq := &api.Prepare2PCRequest{
		Sender:    in.Sender,
		Receiver:  in.Receiver,
		Amount:    in.Amount,
		TxId:      txID,
		Timestamp: in.GetTimestamp(),
	}
	prepResp, err := client.Prepare2PC(pctx, prepReq)
	if err != nil || prepResp == nil {
		s.proposeLogWithPhase("A", txID, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
		sm.restoreBalances(txID)
		sm.unlockAccounts(in.Sender)
		dctx, dcancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		defer dcancel()
		decideReq := &api.Decide2PCRequest{
			TxId:      txID,
			Sender:    in.Sender,
			Receiver:  in.Receiver,
			Amount:    in.Amount,
			Commit:    false,
			Timestamp: in.GetTimestamp(),
		}
		client.Decide2PC(dctx, decideReq)
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "INSUFFICIENT_QUORUM"}, nil
	}
	if !prepResp.Prepared {
		errMsg := prepResp.GetError()
		if errMsg == "" {
			errMsg = "INSUFFICIENT_QUORUM"
		}
		s.proposeLogWithPhase("A", txID, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
		sm.restoreBalances(txID)
		sm.unlockAccounts(in.Sender)
		dctx, dcancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		defer dcancel()
		decideReq := &api.Decide2PCRequest{
			TxId:      txID,
			Sender:    in.Sender,
			Receiver:  in.Receiver,
			Amount:    in.Amount,
			Commit:    false,
			Timestamp: in.GetTimestamp(),
		}
		client.Decide2PC(dctx, decideReq)
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: errMsg}, nil
	}

	localCommit, res := s.proposeLogWithPhase("C", txID, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
	if !localCommit {
		s.proposeLogWithPhase("A", txID, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
		sm.restoreBalances(txID)
		sm.unlockAccounts(in.Sender)
		dctx, dcancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		defer dcancel()
		decideReq := &api.Decide2PCRequest{
			TxId:      txID,
			Sender:    in.Sender,
			Receiver:  in.Receiver,
			Amount:    in.Amount,
			Commit:    false,
			Timestamp: in.GetTimestamp(),
		}
		client.Decide2PC(dctx, decideReq)
		return &api.Reply{Result: false, ServerId: int32(s.id), Error: "INSUFFICIENT_QUORUM"}, nil
	}

	dctx, dcancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer dcancel()
	decideReq := &api.Decide2PCRequest{
		TxId:      txID,
		Sender:    in.Sender,
		Receiver:  in.Receiver,
		Amount:    in.Amount,
		Commit:    true,
		Timestamp: in.GetTimestamp(),
	}
	client.Decide2PC(dctx, decideReq)

	reply := &api.Reply{
		BallotVal: int32(s.ballot.BallotVal),
		ServerId:  int32(s.id),
		Timestamp: in.GetTimestamp(),
		ClientId:  in.GetClientId(),
		Result:    res,
	}
	return reply, nil
}

func (s *ServerImpl) proposeLogWithPhase(phase string, txID string, sender string, receiver string, amount int, timestamp int64) (bool, bool) {
	s.lock.Lock()
	if !s.isServerRunning() || s.state != constants.Leader {
		s.lock.Unlock()
		return false, false
	}
	s.seqNum++
	currSeqNum := s.seqNum
	record := &LogRecord{
		SeqNum: currSeqNum,
		Ballot: &BallotNumber{BallotVal: int(s.ballot.BallotVal), ServerID: s.id},
		Txn: &ClientRequestTxn{
			Sender:   sender,
			Reciever: receiver,
			Amount:   amount,
		},
		IsCommitted: false,
		Phase:       phase,
		TxID:        txID,
	}
	s.lock.Unlock()

	logStore.append(record)
	quorum := int((constants.MAX_NODES / 2) + 1)

	ok := false
	attempts := 0
	for !ok {
		s.lock.Lock()
		isLeader := s.state == constants.Leader
		if !isLeader {
			s.lock.Unlock()
			return false, false
		}
		if !s.isServerRunning() {
			s.lock.Unlock()
			return false, false
		}
		if s.ballot.BallotVal < s.leaderBallot.BallotVal || (s.ballot.BallotVal == s.leaderBallot.BallotVal && s.id < s.leaderBallot.ServerID) {
			s.state = constants.Follower
			s.lock.Unlock()
			return false, false
		}
		s.lock.Unlock()
		ok = s.peerManager.broadcastAccept(quorum, record, timestamp)
		attempts++
		if !ok && attempts >= 3 {
			return false, false
		}
	}

	logStore.markCommitted(currSeqNum)
	sm.applyTxn()
	s.peerManager.broadcastCommit(record)
	return true, true
}

func (sm *StateMachine) startExec() {
	logs.Debug("Start")
	defer logs.Debug("Exit")

	go func() {
		for range sm.execPool {
			sm.execCommitLogs()
		}
	}()
}

func (sm *StateMachine) tryLockAccounts(accounts ...string) bool {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	if len(accounts) == 0 {
		return true
	}

	seen := make(map[string]struct{})
	for _, acc := range accounts {
		if acc == "" {
			continue
		}
		if _, ok := seen[acc]; ok {
			continue
		}
		seen[acc] = struct{}{}
		if sm.locks[acc] {
			return false
		}
	}
	for acc := range seen {
		sm.locks[acc] = true
	}
	return true
}

func (sm *StateMachine) unlockAccounts(accounts ...string) {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	if len(accounts) == 0 {
		return
	}

	seen := make(map[string]struct{})
	for _, acc := range accounts {
		if acc == "" {
			continue
		}
		if _, ok := seen[acc]; ok {
			continue
		}
		seen[acc] = struct{}{}
	}
	for acc := range seen {
		delete(sm.locks, acc)
	}
}

func (sm *StateMachine) snapshotBalances(txID string, accounts ...string) {
	if txID == "" || len(accounts) == 0 {
		return
	}
	sm.lock.Lock()
	entry, ok := sm.wal[txID]
	if !ok {
		entry = make(map[string]int)
		sm.wal[txID] = entry
	}
	for _, acc := range accounts {
		if acc == "" {
			continue
		}
		if _, exists := entry[acc]; exists {
			continue
		}
		bal, ok := sm.vault[acc]
		if !ok {
			bal = constants.INITIAL_BALANCE
		}
		entry[acc] = bal
	}
	localCopy := make(map[string]int, len(entry))
	for k, v := range entry {
		localCopy[k] = v
	}
	sm.lock.Unlock()
	persistWAL(txID, localCopy)
}

func (sm *StateMachine) restoreBalances(txID string) {
	if txID == "" {
		return
	}
	sm.lock.Lock()
	entry, ok := sm.wal[txID]
	if !ok {
		sm.lock.Unlock()
		return
	}
	for acc, bal := range entry {
		sm.vault[acc] = bal
	}
	delete(sm.wal, txID)
	sm.lock.Unlock()
	deleteWAL(txID)
}

func (sm *StateMachine) execCommitLogs() {
	logs.Debug("Start")
	defer logs.Debug("Exit")

	for {
		logStore.lock.Lock()
		sm.lock.Lock()
		nextCommitIdx := sm.lastExecutedCommitNum + 1
		logToApply, ok := logStore.records[nextCommitIdx]
		if !ok || !logToApply.IsCommitted {
			sm.lock.Unlock()
			logStore.lock.Unlock()
			if server != nil {
				server.startLogCatchup()
			}
			return
		}
		sender := logToApply.Txn.Sender
		reciever := logToApply.Txn.Reciever

		cluster := constants.ClusterOf(server.id)
		senderShard := shardOfAccount(sender)
		receiverShard := shardOfAccount(reciever)

		if senderShard != -1 && senderShard == cluster {
			if _, ok := sm.vault[sender]; !ok {
				sm.vault[sender] = constants.INITIAL_BALANCE
				logs.Infof("Detected new user, Initializing Balance for User: %s with %d", sender, constants.INITIAL_BALANCE)
			}
		}
		if receiverShard != -1 && receiverShard == cluster {
			if _, ok := sm.vault[reciever]; !ok {
				sm.vault[reciever] = constants.INITIAL_BALANCE
			}
		}

		var res bool
		walToDelete := ""
		phase := logToApply.Phase
		if phase == "" || phase == "N" {
			if senderShard == cluster && receiverShard == cluster {
				if sm.vault[sender] >= logToApply.Txn.Amount {
					sm.vault[sender] -= logToApply.Txn.Amount
					sm.vault[reciever] += logToApply.Txn.Amount
					res = true
				}
			}
		} else if phase == "P" {
			res = true
		} else if phase == "C" {
			if senderShard != -1 && cluster == senderShard {
				if sm.vault[sender] >= logToApply.Txn.Amount {
					sm.vault[sender] -= logToApply.Txn.Amount
					res = true
				}
				delete(sm.locks, sender)
			}
			if receiverShard != -1 && cluster == receiverShard {
				sm.vault[reciever] += logToApply.Txn.Amount
				res = true
				delete(sm.locks, reciever)
			}
			if logToApply.TxID != "" {
				if _, ok := sm.wal[logToApply.TxID]; ok {
					delete(sm.wal, logToApply.TxID)
					walToDelete = logToApply.TxID
				}
			}
		} else if phase == "A" {
			if logToApply.TxID != "" {
				if entry, ok := sm.wal[logToApply.TxID]; ok {
					for acc, bal := range entry {
						sm.vault[acc] = bal
					}
					delete(sm.wal, logToApply.TxID)
					walToDelete = logToApply.TxID
				}
			}
			cluster := constants.ClusterOf(server.id)
			senderShard := shardOfAccount(sender)
			receiverShard := shardOfAccount(reciever)
			if senderShard != -1 && cluster == senderShard {
				delete(sm.locks, sender)
			}
			if receiverShard != -1 && cluster == receiverShard {
				delete(sm.locks, reciever)
			}
		}
		sm.lastExecutedCommitNum++
		logToApply.IsExecuted = true
		executedIdx := sm.lastExecutedCommitNum
		senderBalance := sm.vault[sender]
		recieverBalance := sm.vault[reciever]
		execChan, ok := sm.executionStatus[logToApply.SeqNum]
		if ok {
			execChan <- res
			close(execChan)
			delete(sm.executionStatus, logToApply.SeqNum)
		}
		sm.lock.Unlock()
		logStore.lock.Unlock()

		persistVaultEntry(sender, senderBalance)
		persistVaultEntry(reciever, recieverBalance)
		persistLastExecuted(executedIdx)
		persistLogRecord(logToApply)
		if walToDelete != "" {
			deleteWAL(walToDelete)
		}
	}
}

func (ls *LogStore) append(record *LogRecord) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	ls.lock.Lock()

	ls.records[record.SeqNum] = record
	logs.Infof("Added record to log with seqNum %d", record.SeqNum)
	ls.lock.Unlock()

	go ls.marshal()
}

func (sm *StateMachine) registerSignal(seqNum int) chan bool {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	sm.lock.Lock()
	defer sm.lock.Unlock()
	execChan := make(chan bool, 1)
	sm.executionStatus[seqNum] = execChan
	return execChan
}

func (ls *LogStore) markCommitted(seqNum int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	ls.lock.Lock()

	record, ok := ls.records[seqNum]
	if ok {
		record.IsCommitted = true
		logs.Infof("Log with seqNum %d commited", seqNum)
	} else {
		logs.Warnf("No record found with seqNum: %d", seqNum)
	}
	ls.lock.Unlock()

	if ok {
		persistLogRecord(record)
	}
}

func (sm *StateMachine) applyTxn() {

	logs.Debug("Enter")
	defer logs.Debug("Exit")

	select {
	case sm.execPool <- struct{}{}:
	default:
	}

}

func (s *ServerImpl) GetBalance(ctx context.Context, in *api.BalanceRequest) (*api.BalanceReply, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	if !s.isServerRunning() {
		return &api.BalanceReply{}, s.downErr()
	}

	account := in.GetAccount()
	sm.lock.Lock()
	bal, ok := sm.vault[account]
	if !ok {
		bal = constants.INITIAL_BALANCE
	}
	sm.lock.Unlock()

	return &api.BalanceReply{
		Balance: int32(bal),
		Found:   ok,
	}, nil
}

func (s *ServerImpl) Prepare2PC(ctx context.Context, in *api.Prepare2PCRequest) (*api.Prepare2PCReply, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	if !s.isServerRunning() {
		return &api.Prepare2PCReply{Prepared: false, Error: "SERVER_DOWN"}, s.downErr()
	}

	s.lock.Lock()
	if s.state != constants.Leader {
		leaderID := s.leaderBallot.ServerID
		s.lock.Unlock()
		if leaderID == 0 || leaderID == s.id {
			return &api.Prepare2PCReply{Prepared: false, Error: "NOT_LEADER"}, nil
		}
		s.peerManager.lock.Lock()
		leaderPeer, ok := s.peerManager.peers[leaderID]
		s.peerManager.lock.Unlock()
		if !ok {
			return &api.Prepare2PCReply{Prepared: false, Error: "NOT_LEADER"}, nil
		}
		client := api.NewClientServerTxnsClient(leaderPeer.conn)
		forwardCtx, cancel := context.WithTimeout(context.Background(), constants.FORWARD_TIMEOUT*time.Millisecond)
		defer cancel()
		return client.Prepare2PC(forwardCtx, in)
	}
	s.lock.Unlock()

	if !sm.tryLockAccounts(in.Receiver) {
		return &api.Prepare2PCReply{Prepared: false, Error: "LOCK_CONFLICT"}, nil
	}
	sm.snapshotBalances(in.TxId, in.Receiver)

	ok, _ := s.proposeLogWithPhase("P", in.TxId, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
	if !ok {
		sm.restoreBalances(in.TxId)
		sm.unlockAccounts(in.Receiver)
		return &api.Prepare2PCReply{Prepared: false, Error: "PREPARE_FAILED"}, nil
	}

	return &api.Prepare2PCReply{Prepared: true}, nil
}

func (s *ServerImpl) Decide2PC(ctx context.Context, in *api.Decide2PCRequest) (*api.Blank, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	if !s.isServerRunning() {
		return &api.Blank{}, s.downErr()
	}

	s.lock.Lock()
	if s.state != constants.Leader {
		leaderID := s.leaderBallot.ServerID
		s.lock.Unlock()
		if leaderID == 0 || leaderID == s.id {
			return &api.Blank{}, nil
		}
		s.peerManager.lock.Lock()
		leaderPeer, ok := s.peerManager.peers[leaderID]
		s.peerManager.lock.Unlock()
		if !ok {
			return &api.Blank{}, nil
		}
		client := api.NewClientServerTxnsClient(leaderPeer.conn)
		forwardCtx, cancel := context.WithTimeout(context.Background(), constants.FORWARD_TIMEOUT*time.Millisecond)
		defer cancel()
		return client.Decide2PC(forwardCtx, in)
	}
	s.lock.Unlock()

	if in.Commit {
		s.proposeLogWithPhase("C", in.TxId, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
	} else {
		s.proposeLogWithPhase("A", in.TxId, in.Sender, in.Receiver, int(in.Amount), in.GetTimestamp())
	}

	return &api.Blank{}, nil
}

func (s *ServerImpl) PrintLog(ctx context.Context, in *api.Blank) (*api.Logs, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	// s.checkServerStatus()

	logStore.lock.Lock()
	defer logStore.lock.Unlock()

	keys := make([]int, 0, len(logStore.records))
	for k := range logStore.records {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	entries := make([]*api.LogRecord, 0, len(logStore.records))
	for _, seqNum := range keys {
		record := logStore.records[seqNum]
		entry := &api.LogRecord{
			SeqNum:      int32(record.SeqNum),
			BallotVal:   int32(record.Ballot.BallotVal),
			ServerId:    int32(record.Ballot.ServerID),
			Sender:      record.Txn.Sender,
			Receiver:    record.Txn.Reciever,
			Amount:      int32(record.Txn.Amount),
			IsCommitted: record.IsCommitted,
			Phase:       record.Phase,
			TxId:        record.TxID,
		}
		entries = append(entries, entry)
	}
	return &api.Logs{Logs: entries}, nil
}

func (s *ServerImpl) PrintDB(ctx context.Context, in *api.Blank) (*api.Vault, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	// s.checkServerStatus()
	sm.lock.Lock()
	defer sm.lock.Unlock()

	vaultCopy := make(map[string]int32)
	for k, v := range sm.vault {
		vaultCopy[k] = int32(v)
	}
	return &api.Vault{Vault: vaultCopy}, nil
}

func (s *ServerImpl) PrintStatus(ctx context.Context, in *api.RequestInfo) (*api.Status, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	// s.checkServerStatus()

	seqNum := int(in.GetSeqNum())
	logStore.lock.Lock()
	defer logStore.lock.Unlock()
	record, ok := logStore.records[seqNum]
	if !ok {
		return &api.Status{Status: api.TxnState_NOSTATUS}, nil
	}
	if record.IsExecuted {
		return &api.Status{Status: api.TxnState_EXECUTED}, nil
	}
	if record.IsCommitted {
		return &api.Status{Status: api.TxnState_COMMITTED}, nil
	}
	return &api.Status{Status: api.TxnState_ACCEPTED}, nil
}

func (s *ServerImpl) PrintView(ctx context.Context, in *api.Blank) (*api.ViewLogs, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	// s.checkServerStatus()
	s.lock.Lock()
	defer s.lock.Unlock()

	historyCopy := make([]*api.NewViewReq, len(s.viewLog))
	copy(historyCopy, s.viewLog)

	return &api.ViewLogs{Views: historyCopy}, nil
}

func (pm *PeerManager) initPeerConnections(currServer int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	pm.lock.Lock()
	defer pm.lock.Unlock()

	for serverId, port := range constants.ServerPorts {
		if serverId == currServer {
			continue
		}
		if constants.ClusterOf(serverId) != constants.ClusterOf(currServer) {
			continue
		}

		addr := fmt.Sprintf("localhost:%s", port)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logs.Warnf("failed to create connection to server %d: %v", serverId, err)
			continue
		}
		client := api.NewPaxosReplicationClient(conn)
		pm.peers[serverId] = &Peer{
			id:   serverId,
			conn: conn,
			api:  client,
		}
		logs.Infof("Successfully created connections for server-%d", serverId)
	}
}

func (pm *PeerManager) broadcastAccept(quoram int, record *LogRecord, timestamp int64) bool {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	var acceptedVotes int32 = 1
	qourumChan := make(chan bool, 1)
	apiRecord := &api.LogRecord{
		SeqNum:    int32(record.SeqNum),
		BallotVal: int32(record.Ballot.BallotVal),
		ServerId:  int32(record.Ballot.ServerID),
		Sender:    record.Txn.Sender,
		Receiver:  record.Txn.Reciever,
		Amount:    int32(record.Txn.Amount),
		Timestamp: timestamp,
		Phase:     record.Phase,
		TxId:      record.TxID,
	}

	for _, peer := range pm.peers {
		go func(p *Peer) {
			// *NOTE* Not using peer manager lock because fetching conn is a read-only operation
			// We never create/edit conn after its created for the firsts time, which is sequentials
			ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
			defer cancel()
			resp, err := p.api.Accept(ctx, apiRecord)
			if err != nil {
				logs.Warnf("Failed to send accpet rpc to server-%d with error %v", peer.id, err)
				return
			}
			// TODO to check the resp for a different ballot or different message
			if resp != nil && resp.Success {
				if atomic.AddInt32(&acceptedVotes, 1) == int32(quoram) {
					select {
					case qourumChan <- true:
					default:
					}
				}
			}
		}(peer)
	}

	select {
	case <-qourumChan:
		logs.Infof("Qouram achieved for seqNum %d", record.SeqNum)
		return true
	case <-time.After(constants.REQUEST_TIMEOUT * time.Millisecond):
		logs.Warnf("Quorum not achieved for seqNum %d. Timed out. Recieved voted %d, needed %d", record.SeqNum, acceptedVotes, quoram)
		return false
	}

}

func (pm *PeerManager) broadcastCommit(record *LogRecord) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	apiRecord := &api.LogRecord{
		SeqNum:    int32(record.SeqNum),
		BallotVal: int32(record.Ballot.BallotVal),
		ServerId:  int32(record.Ballot.ServerID),
		Sender:    record.Txn.Sender,
		Receiver:  record.Txn.Reciever,
		Amount:    int32(record.Txn.Amount),
		Phase:     record.Phase,
		TxId:      record.TxID,
	}

	for _, peer := range pm.peers {
		go func(p *Peer) {
			// *NOTE* Not using peer manager lock because fetching conn is a read-only operation
			// We never create/edit conn after its created for the firsts time, which is sequential
			ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
			defer cancel()
			_, err := p.api.Commit(ctx, apiRecord)
			if err != nil {
				logs.Warnf("Failed to send commit rpc to server-%d with error %v", peer.id, err)
				return
			}
		}(peer)
	}

}

func (s *ServerImpl) Accept(ctx context.Context, in *api.LogRecord) (*api.AcceptResp, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	if !s.isServerRunning() {
		return &api.AcceptResp{Success: false}, s.downErr()
	}

	// TODO maybe check if we are current leader, if me have multiple leader we need to reject incoming requests
	s.lock.Lock()
	if s.state == constants.Follower {
		select {
		case s.leaderPulse <- true:
		default:
		}
	}

	if in.BallotVal < int32(s.leaderBallot.BallotVal) {
		logs.Warnf("Recieved Accept for <%d,, %d> but current Highest ballot is <%d, %d>", in.GetBallotVal(), in.GetServerId(), s.leaderBallot.BallotVal, s.leaderBallot.ServerID)
		s.lock.Unlock()
		return &api.AcceptResp{Success: false}, nil
	} else if in.BallotVal == int32(s.leaderBallot.BallotVal) && in.ServerId < int32(s.leaderBallot.ServerID) {
		logs.Warnf("Recieved Accept for <%d,, %d> but current Highest ballot is <%d, %d>", in.GetBallotVal(), in.GetServerId(), s.leaderBallot.BallotVal, s.leaderBallot.ServerID)
		s.lock.Unlock()
		return &api.AcceptResp{Success: false}, nil
	}
	s.ballot = &BallotNumber{BallotVal: int(in.BallotVal), ServerID: s.id}
	s.lock.Unlock()
	if in.Sender == "HEARTBEAT" {
		return &api.AcceptResp{Success: true}, nil
	}
	logs.Infof("Recieved accept RPC from Server-%d with ballot <%d, %d> for seqNum: #%d, with client txn <%s,%s,%d,%d>", in.GetServerId(), in.GetBallotVal(), in.GetServerId(), in.GetSeqNum(), in.GetSender(), in.GetReceiver(), in.GetAmount(), in.GetTimestamp())

	logStore.lock.Lock()

	seqNum := int(in.GetSeqNum())
	if existingRecord, ok := logStore.records[seqNum]; ok {
		if existingRecord.IsCommitted || existingRecord.IsExecuted {
			logs.Infof("Accept for seq #%d already committed/executed, ignoring.", seqNum)
			logStore.lock.Unlock()
			return &api.AcceptResp{Success: true}, nil
		}
	}
	logStore.lock.Unlock()

	record := &LogRecord{
		SeqNum: int(in.SeqNum), // What if it over writes server's some log???? *TODO* Check for consistency
		Ballot: &BallotNumber{BallotVal: int(in.BallotVal), ServerID: int(in.ServerId)},
		Txn: &ClientRequestTxn{
			Sender:   in.Sender,
			Reciever: in.Receiver,
			Amount:   int(in.Amount),
		},
		IsCommitted: false,
		Phase:       in.GetPhase(),
		TxID:        in.GetTxId(),
	}
	// **Check** Do I need to add to log or just add  the commited log??????
	logStore.append(record)
	return &api.AcceptResp{
		Record:  in,
		NodeID:  int32(s.id),
		Success: true,
	}, nil
}

func (s *ServerImpl) Commit(ctx context.Context, in *api.LogRecord) (*api.Blank, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	if !s.isServerRunning() {
		return &api.Blank{}, s.downErr()
	}

	logs.Infof("Recieved commit RPC from Server-%d with ballot <%d, %d> for seqNum: #%d, with client txn <%s,%s,%d,%d>", in.GetServerId(), in.GetBallotVal(), in.GetServerId(), in.GetSeqNum(), in.GetSender(), in.GetReceiver(), in.GetAmount(), in.GetTimestamp())

	s.lock.Lock()
	if s.state == constants.Follower {
		select {
		case s.leaderPulse <- true:
		default:
		}
	}
	if in.BallotVal < int32(s.leaderBallot.BallotVal) {
		logs.Warnf("Recieved Commit for <%d,, %d> but current Highest ballot is <%d, %d>", in.GetBallotVal(), in.GetServerId(), s.leaderBallot.BallotVal, s.leaderBallot.ServerID)
		s.lock.Unlock()
		return &api.Blank{}, nil
	} else if in.BallotVal == int32(s.leaderBallot.BallotVal) && in.ServerId < int32(s.leaderBallot.ServerID) {
		logs.Warnf("Recieved Commit for <%d,, %d> but current Highest ballot is <%d, %d>", in.GetBallotVal(), in.GetServerId(), s.leaderBallot.BallotVal, s.leaderBallot.ServerID)
		s.lock.Unlock()
		return &api.Blank{}, nil
	}
	s.ballot = &BallotNumber{BallotVal: int(in.BallotVal), ServerID: s.id}
	s.lock.Unlock()
	// Note: in runs/Commit_Before_Accept we recieved a commit message before the accept message. current
	// hangling leaves a permanent hole that can't be repaired until a new-view message that makes this node faulty
	// halting future commit messages from being executed
	// We can blindly trust a commit message, Doing that Bellow
	logStore.lock.Lock()

	seqNum := int(in.GetSeqNum())
	record, ok := logStore.records[seqNum]
	if ok && (record.IsCommitted || record.IsExecuted) {
		logs.Infof("Commit for seq #%d already processed, ignoring.", seqNum)
		logStore.lock.Unlock()
		sm.applyTxn()
		return &api.Blank{}, nil
	}

	if !ok {
		// If the log entry doesn't exist, the Accept was likely missed.
		// Since a Commit proves quorum was met, we can safely create the entry.
		logs.Warnf("Commit received for seqNum #%d before Accept. Creating log entry.", seqNum)
		record = &LogRecord{
			SeqNum: seqNum,
			Ballot: &BallotNumber{BallotVal: int(in.BallotVal), ServerID: int(in.ServerId)},
			Txn: &ClientRequestTxn{
				Sender:   in.Sender,
				Reciever: in.Receiver,
				Amount:   int(in.Amount),
			},
			Phase: in.GetPhase(),
			TxID:  in.GetTxId(),
		}
		logStore.records[seqNum] = record
	}
	record.IsCommitted = true
	logStore.lock.Unlock()

	persistLogRecord(record)
	sm.applyTxn()

	return &api.Blank{}, nil
}

func (s *ServerImpl) monitorLeader() {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	logs.Info("Monitoring Leader for timeout expiration")
	tpTimer := time.NewTimer(s.tp)
	for {
		if !s.isServerRunning() {
			continue
		}
		select {
		case <-s.leaderTimer.C:
			s.lock.Lock()
			isFollower := (s.state == constants.Follower)
			s.isPromised = false
			timeElapsed := time.Since(s.lastPrepareRecv)
			tp := s.tp
			s.lock.Unlock()
			if isFollower {
				if timeElapsed >= tp {
					logs.Warn("Leader Timed out starting election")
					s.startElection()
				} else {
					logs.Warnf("Leader Timed out but PREPARE seen in last tp")
				}
			}
			s.leaderTimer.Reset(constants.LEADER_TIMEOUT_SECONDS * time.Millisecond)
			//s.leaderTimer.Reset((constants.LEADER_TIMEOUT_SECONDS*1000 + time.Duration(rand.Intn(1000))) * time.Millisecond)

		case <-s.leaderPulse:
			if !s.leaderTimer.Stop() {
				select {
				case <-s.leaderTimer.C:
				default:
				}
			}
			s.leaderTimer.Reset(constants.LEADER_TIMEOUT_SECONDS * time.Millisecond)
		case <-tpTimer.C:
			s.lock.Lock()
			s.isPromised = false
			close(s.prepareChan)
			// time.Sleep(5 * time.Millisecond)
			s.prepareChan = make(chan struct{})
			s.lock.Unlock()
			tpTimer.Reset(s.tp)

		}

	}
}

func (s *ServerImpl) startElection() {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	if !s.isServerRunning() {
		return
	}

	s.lock.Lock()
	if time.Since(s.lastPrepareRecv) < s.tp {
		logs.Infof("Suppressing prepare send: received a new prepare in last tp)")
		s.state = constants.Follower
		s.lock.Unlock()
		return
	}

	s.state = constants.Candidate
	newBallotVal := max(s.leaderBallot.BallotVal, s.ballot.BallotVal)
	s.ballot.BallotVal = newBallotVal + 1
	prepareReq := api.PrepareReq{
		BallotVal: int32(s.ballot.BallotVal),
		ServerId:  int32(s.id),
	}
	s.lock.Unlock()
	logs.Infof("Starting election with ballot <%d, %d>", prepareReq.BallotVal, prepareReq.ServerId)

	quorum := int((constants.MAX_NODES / 2) + 1)
	var promiseVotes int32 = 1
	promiseChan := make(chan *api.PromiseResp, constants.MAX_NODES)

	for _, peer := range s.peerManager.peers {
		go func(p *Peer) {
			if !s.isServerRunning() {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
			defer cancel()
			resp, err := p.api.Prepare(ctx, &prepareReq)
			if err != nil {
				logs.Warnf("Failed to recieve prepare rpc to server-%d with error %v", p.id, err)
				return
			}

			if resp != nil && resp.Success {
				promiseChan <- resp
			}
		}(peer)
	}

	promiseLogs := make([]*api.LogRecord, 0)
	logStore.lock.Lock()
	for _, record := range logStore.records {
		promiseLogs = append(promiseLogs, &api.LogRecord{
			SeqNum:      int32(record.SeqNum),
			BallotVal:   int32(record.Ballot.BallotVal),
			ServerId:    int32(record.Ballot.ServerID),
			Sender:      record.Txn.Sender,
			Receiver:    record.Txn.Reciever,
			Amount:      int32(record.Txn.Amount),
			IsCommitted: record.IsCommitted,
			Phase:       record.Phase,
			TxId:        record.TxID,
		})
	}
	logStore.lock.Unlock()
	for {
		select {
		case promise := <-promiseChan:
			logs.Infof("recieved promise from server-%d", promise.GetServerId())
			promiseLogs = append(promiseLogs, promise.GetAcceptLog()...)
			if atomic.AddInt32(&promiseVotes, 1) == int32(quorum) {
				logs.Infof("Server-%d achieved election quorom with ballotVal %d", prepareReq.ServerId, prepareReq.ServerId)
				s.becomeLeader(int(prepareReq.BallotVal), promiseLogs)
				return
			}
		case <-time.After(constants.LEADER_TIMEOUT_SECONDS * time.Millisecond):
			logs.Warn("Election timed out, if still in candidate state need to revert to follower")
			s.lock.Lock()
			if s.state == constants.Candidate {
				s.state = constants.Follower
			}
			s.lock.Unlock()
			return

		}
	}
}

func (s *ServerImpl) Prepare(ctx context.Context, in *api.PrepareReq) (*api.PromiseResp, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	if !s.isServerRunning() {
		return &api.PromiseResp{}, s.downErr()
	}
	s.lock.Lock()
	inBallot := in.GetBallotVal()
	inServerID := in.GetServerId()
	logs.Infof("Received Prepare request with ballot <%d, %d>", inBallot, inServerID)

	s.lastPrepareRecv = time.Now()
	cycleChan := s.prepareChan

	select {
	case <-cycleChan:
	default:
		key := fmt.Sprintf("%d:%d", inBallot, inServerID)
		s.pendingPrepares[key] = in
		if s.pendingMax == nil || higherBallot(inBallot, inServerID, s.pendingMax.GetBallotVal(), s.pendingMax.GetServerId()) {
			s.pendingMax = in
		}
		s.lock.Unlock()

		select {
		case <-cycleChan:
		case <-ctx.Done():
			return &api.PromiseResp{Success: false, BallotVal: inBallot, ServerId: int32(s.id)}, nil
		}
		s.lock.Lock()
	}

	if s.isPromised {
		logs.Warn("Prepare rejected: already promised in this expiry window")
		s.lock.Unlock()
		return &api.PromiseResp{Success: false, BallotVal: inBallot, ServerId: int32(s.id)}, nil
	}

	if s.pendingMax == nil || higherBallot(inBallot, inServerID, s.pendingMax.GetBallotVal(), s.pendingMax.GetServerId()) {
		s.pendingMax = in
	}

	if (inBallot > int32(s.ballot.BallotVal) || (inBallot == int32(s.ballot.BallotVal) && inServerID > int32(s.ballot.ServerID))) && (s.pendingMax != nil && inBallot == s.pendingMax.GetBallotVal() && inServerID == s.pendingMax.GetServerId()) {
		logs.Infof("Promising for ballot <%d, %d>", inBallot, inServerID)
		s.ballot.BallotVal = int(inBallot)
		s.ballot.ServerID = int(inServerID)
		s.state = constants.Follower
		s.isPromised = true

		s.leaderBallot.BallotVal = int(inBallot)
		s.leaderBallot.ServerID = int(inServerID)

		select {
		case s.leaderPulse <- true:
		default:
		}

		logStore.lock.Lock()
		acceptedLog := make([]*api.LogRecord, 0)
		for _, record := range logStore.records {
			acceptedLog = append(acceptedLog, &api.LogRecord{
				SeqNum:      int32(record.SeqNum),
				BallotVal:   int32(record.Ballot.BallotVal),
				ServerId:    int32(record.Ballot.ServerID),
				Sender:      record.Txn.Sender,
				Receiver:    record.Txn.Reciever,
				Amount:      int32(record.Txn.Amount),
				IsCommitted: record.IsCommitted,
			})
		}
		logStore.lock.Unlock()

		// clear pending for next cycle
		s.pendingPrepares = make(map[string]*api.PrepareReq)
		s.pendingMax = nil

		s.lock.Unlock()
		return &api.PromiseResp{Success: true, BallotVal: inBallot, ServerId: int32(s.id), AcceptLog: acceptedLog}, nil
	}

	// otherwise, reject
	logs.Warnf("Rejecting Prepare <%d,%d>", inBallot, inServerID)
	s.lock.Unlock()
	return &api.PromiseResp{Success: false, BallotVal: inBallot, ServerId: int32(s.id)}, nil
}

func (s *ServerImpl) becomeLeader(ballotVal int, promiseLogs []*api.LogRecord) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	// Need to check if no other server had higher ballot?
	if !s.isServerRunning() {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.state = constants.Leader
	s.leaderBallot.BallotVal = int(ballotVal)
	s.leaderBallot.ServerID = int(s.id)

	logMap := make(map[int32]*api.LogRecord)
	maxSeq := int32(0)

	logsBySeqNum := make(map[int32][]*api.LogRecord)
	for _, rec := range promiseLogs {
		if rec.GetSeqNum() > maxSeq {
			maxSeq = rec.GetSeqNum()
		}

		logsBySeqNum[rec.GetSeqNum()] = append(logsBySeqNum[rec.GetSeqNum()], rec)
	}

	for i := int32(1); i <= maxSeq; i++ {
		logsForSeq, ok := logsBySeqNum[i]
		if !ok {
			// No log found for this seqNum
			continue
		}
		var isCommittedRecord *api.LogRecord = nil
		for _, r := range logsForSeq {
			if r.IsCommitted {
				isCommittedRecord = r
				break
			}
		}

		if isCommittedRecord != nil {
			logMap[i] = isCommittedRecord
		} else {
			if len(logsForSeq) != 0 {
				recWithHighestBallot := logsForSeq[0]

				for _, r := range logsForSeq {
					if r.GetBallotVal() > recWithHighestBallot.GetBallotVal() {
						recWithHighestBallot = r
					} else if r.GetBallotVal() == recWithHighestBallot.GetBallotVal() && r.GetServerId() > recWithHighestBallot.ServerId {
						recWithHighestBallot = r
					}
				}
				logMap[i] = recWithHighestBallot
			}
		}
	}
	finalLog := make([]*api.LogRecord, 0)
	for i := int32(1); i <= maxSeq; i++ {
		if rec, ok := logMap[i]; ok {
			newRec := &api.LogRecord{
				SeqNum:      i,
				BallotVal:   int32(ballotVal),
				ServerId:    int32(s.id),
				Sender:      rec.Sender,
				Receiver:    rec.Receiver,
				Amount:      rec.Amount,
				Timestamp:   rec.Timestamp,
				IsCommitted: false,
				Phase:       rec.Phase,
				TxId:        rec.TxId,
			}
			finalLog = append(finalLog, newRec)
		} else {
			noOp := &api.LogRecord{
				SeqNum:    i,
				BallotVal: int32(ballotVal),
				ServerId:  int32(s.id),
				Sender:    constants.NOOP,
			}
			finalLog = append(finalLog, noOp)
		}
	}
	newViewReq := &api.NewViewReq{
		BallotVal: int32(ballotVal),
		ServerId:  int32(s.id),
		AcceptLog: finalLog,
	}

	s.viewLog = append(s.viewLog, newViewReq)
	s.seqNum = int(maxSeq)
	logStore.lock.Lock()

	for _, rec := range logStore.records {
		rec.Ballot = &BallotNumber{BallotVal: ballotVal, ServerID: s.id}

	}

	logStore.lock.Unlock()

	for peerid, peer := range s.peerManager.peers {
		go func(id int, p *Peer) {
			ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
			defer cancel()
			_, err := p.api.NewView(ctx, newViewReq)
			if err != nil {
				logs.Warnf("Follower %d failed to accept New-View", peerid)
			}

		}(peerid, peer)
	}

	go s.redoConsensus()
}

func (s *ServerImpl) redoConsensus() {

	logs.Debug("Enter")
	defer logs.Debug("Exit")
	if !s.isServerRunning() {
		return
	}
	quorum := int((constants.MAX_NODES / 2) + 1)
	logStore.lock.Lock()
	defer logStore.lock.Unlock()
	for _, rec := range logStore.records {
		go func(q int, rec *LogRecord) {
			ok := false
			for !ok {

				ok = s.peerManager.broadcastAccept(quorum, rec, time.Now().Unix())
				s.lock.Lock()
				isLeader := s.state == constants.Leader
				s.lock.Unlock()
				if !isLeader || !s.isServerRunning() {
					break
				}

			}
			logStore.markCommitted(rec.SeqNum)
			sm.applyTxn()
			s.peerManager.broadcastCommit(rec)
		}(quorum, rec)

	}
}

func (s *ServerImpl) NewView(ctx context.Context, in *api.NewViewReq) (*api.Blank, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	if !s.isServerRunning() {
		return &api.Blank{}, s.downErr()
	}
	logs.Infof("Received NewView from new leader <%d, %d>", in.GetBallotVal(), in.GetServerId())

	s.lock.Lock()
	defer s.lock.Unlock()
	if in.BallotVal < int32(s.leaderBallot.BallotVal) {
		logs.Warnf("Recieved New-View for <%d,, %d> but current Highest ballot is <%d, %d>", in.GetBallotVal(), in.GetServerId(), s.leaderBallot.BallotVal, s.leaderBallot.ServerID)
		return &api.Blank{}, nil
	} else if in.BallotVal == int32(s.leaderBallot.BallotVal) && in.ServerId < int32(s.leaderBallot.ServerID) {
		logs.Warnf("Recieved View for <%d,, %d> but current Highest ballot is <%d, %d>", in.GetBallotVal(), in.GetServerId(), s.leaderBallot.BallotVal, s.leaderBallot.ServerID)
		return &api.Blank{}, nil
	}
	s.ballot = &BallotNumber{BallotVal: int(in.BallotVal), ServerID: s.id}
	s.leaderBallot.BallotVal = int(in.BallotVal)
	s.leaderBallot.ServerID = int(in.ServerId)
	s.state = constants.Follower
	s.viewLog = append(s.viewLog, in)
	select {
	case s.leaderPulse <- true:
	default:
	}

	logStore.lock.Lock()
	defer logStore.lock.Unlock()
	for _, newRecord := range in.GetAcceptLog() {
		seqNum := int(newRecord.GetSeqNum())
		if rec, ok := logStore.records[seqNum]; ok {
			if rec.IsExecuted || rec.IsCommitted {
				rec.Ballot = &BallotNumber{BallotVal: int(newRecord.GetBallotVal()), ServerID: int(newRecord.GetServerId())}
				continue
			}
		}
		logStore.records[int(newRecord.GetSeqNum())] = &LogRecord{
			SeqNum: int(newRecord.GetSeqNum()),
			Ballot: &BallotNumber{BallotVal: int(newRecord.GetBallotVal()), ServerID: int(newRecord.GetServerId())},
			Txn: &ClientRequestTxn{
				Sender:   newRecord.GetSender(),
				Reciever: newRecord.GetReceiver(),
				Amount:   int(newRecord.GetAmount()),
			},
			IsCommitted: false,
			IsExecuted:  false,
			Phase:       newRecord.GetPhase(),
			TxID:        newRecord.GetTxId(),
		}
	}
	go logStore.marshal()
	return &api.Blank{}, nil
}

func higherBallot(aVal, aID, bVal, bID int32) bool {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	if aVal != bVal {
		return aVal > bVal
	}
	return aID > bID
}

func (s *ServerImpl) SimulateNodeFailure(ctx context.Context, in *api.Blank) (*api.Blank, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	s.switchLock.Lock()
	s.isRunning = false
	defer s.switchLock.Unlock()

	logs.Warn("SIMULATING NODE FAILURE: SERVER IS NOW UNRESPONSIVE")
	return &api.Blank{}, nil

}

func (s *ServerImpl) SimulateLeaderFailure(ctx context.Context, in *api.Blank) (*api.Blank, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	s.lock.Lock()
	if s.state != constants.Leader {
		s.lock.Unlock()
		return &api.Blank{}, nil
	}
	s.state = constants.Follower
	s.lock.Unlock()
	s.switchLock.Lock()
	s.isRunning = false
	defer s.switchLock.Unlock()

	logs.Warn("SIMULATING NODE FAILURE: SERVER IS NOW UNRESPONSIVE")
	return &api.Blank{}, nil

}

func (s *ServerImpl) RecoverNode(ctx context.Context, in *api.Blank) (*api.Blank, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	s.switchLock.Lock()
	defer s.switchLock.Unlock()
	s.isRunning = true
	logs.Warn("RECOVERED: SERVER IS NOW RESPONSIVE ")
	return &api.Blank{}, nil
}

func (s *ServerImpl) isServerRunning() bool {
	s.switchLock.Lock()
	defer s.switchLock.Unlock()
	return s.isRunning
}

func (s *ServerImpl) downErr() error {
	return fmt.Errorf("SERVER_DOWN")
}

func (s *ServerImpl) startHeartbeat() {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	ticker := time.NewTicker(constants.HEARTBEAT * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if !s.isServerRunning() {
			continue
		}
		s.lock.Lock()
		isLeader := s.state == constants.Leader
		if isLeader {
			heartbeatMsg := &api.LogRecord{
				BallotVal: int32(s.ballot.BallotVal),
				ServerId:  int32(s.id),
				Sender:    "HEARTBEAT",
			}
			s.lock.Unlock()

			for _, peer := range s.peerManager.peers {
				go func(p *Peer) {
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
					defer cancel()
					p.api.Accept(ctx, heartbeatMsg)
				}(peer)
			}
		} else {
			s.lock.Unlock()
		}
	}
}

func (s *ServerImpl) logCatchupLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if !s.isServerRunning() {
			continue
		}
		s.startLogCatchup()
	}
}

func (ls *LogStore) marshal() {
}

func (s *ServerImpl) startLogCatchup() {
	s.logFetchLock.Lock()
	if s.logFetchRunning || !s.isServerRunning() {
		s.logFetchLock.Unlock()
		return
	}
	s.logFetchRunning = true
	s.logFetchLock.Unlock()

	go func() {
		defer func() {
			s.logFetchLock.Lock()
			s.logFetchRunning = false
			s.logFetchLock.Unlock()
		}()

		if !s.isServerRunning() {
			return
		}

		sm.lock.Lock()
		nextSeq := sm.lastExecutedCommitNum + 1
		sm.lock.Unlock()

		logStore.lock.Lock()
		maxCommitted := 0
		for seq, rec := range logStore.records {
			if rec.IsCommitted && seq > maxCommitted {
				maxCommitted = seq
			}
		}
		if maxCommitted <= nextSeq {
			logStore.lock.Unlock()
			return
		}
		missing := make([]int, 0)
		for seq := nextSeq; seq <= maxCommitted; seq++ {
			rec, ok := logStore.records[seq]
			if !ok || !rec.IsCommitted {
				missing = append(missing, seq)
			}
		}
		logStore.lock.Unlock()
		if len(missing) == 0 {
			return
		}

		for _, target := range missing {
			if !s.isServerRunning() {
				return
			}
			var pulled *api.LogRecord
			for id, p := range s.peerManager.peers {
				if id == s.id || p.conn == nil {
					continue
				}
				client := api.NewPaxosPrintInfoClient(p.conn)
				ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
				resp, err := client.PrintLog(ctx, &api.Blank{})
				cancel()
				if err != nil {
					continue
				}
				for _, r := range resp.GetLogs() {
					if int(r.GetSeqNum()) == target && r.GetIsCommitted() {
						pulled = r
						break
					}
				}
				if pulled != nil {
					break
				}
			}
			if pulled == nil {
				continue
			}

			logStore.lock.Lock()
			var recToPersist *LogRecord
			rec, ok := logStore.records[target]
			if ok {
				if rec.IsCommitted || rec.IsExecuted {
					logStore.lock.Unlock()
					continue
				}
				rec.Ballot = &BallotNumber{BallotVal: int(pulled.BallotVal), ServerID: int(pulled.ServerId)}
				rec.Txn = &ClientRequestTxn{
					Sender:   pulled.Sender,
					Reciever: pulled.Receiver,
					Amount:   int(pulled.Amount),
				}
				rec.IsCommitted = true
				recToPersist = rec
			} else {
				newRec := &LogRecord{
					SeqNum: target,
					Ballot: &BallotNumber{BallotVal: int(pulled.BallotVal), ServerID: int(pulled.ServerId)},
					Txn: &ClientRequestTxn{
						Sender:   pulled.Sender,
						Reciever: pulled.Receiver,
						Amount:   int(pulled.Amount),
					},
					IsCommitted: true,
					Phase:       pulled.GetPhase(),
					TxID:        pulled.GetTxId(),
				}
				logStore.records[target] = newRec
				recToPersist = newRec
			}
			logStore.lock.Unlock()
			if recToPersist != nil {
				persistLogRecord(recToPersist)
			}
		}

		sm.applyTxn()
	}()
}

// func (sm *StateMachine) marshalSnapshot() {
// 	sm.lock.Lock()
// 	snapshot := SnapshotData{
// 		LastIncludedIndex: sm.lastExecutedCommitNum,
// 		Vault:             make(map[string]int),
// 	}
// 	for k, v := range sm.vault {
// 		snapshot.Vault[k] = v
// 	}
// 	sm.lock.Unlock()

// 	data, err := json.MarshalIndent(snapshot, "", "  ")
// 	if err != nil {
// 		logs.Warnf("Failed to marshal snapshot: %v", err)
// 		return
// 	}

// 	filePath := sm.snapshotPath
// 	if err := os.WriteFile(filePath, data, 0644); err != nil {
// 		logs.Warnf("Failed to write snapshot to file %s: %v", filePath, err)
// 	}
// }

// func (sm *StateMachine) unmarshalSnapshot() {

// 	logs.Debug("Enter")
// 	defer logs.Debug("Exit")

// 	filePath := sm.snapshotPath
// 	if _, err := os.Stat(filePath); os.IsNotExist(err) {
// 		logs.Infof("Snapshot file %s not found, starting with initial state.", filePath)
// 		return
// 	}

// 	data, err := os.ReadFile(filePath)
// 	if err != nil {
// 		logs.Warnf("Failed to read snapshot file %s: %v", filePath, err)
// 		return
// 	}
// 	if len(data) == 0 {
// 		return
// 	}

// 	sm.lock.Lock()
// 	defer sm.lock.Unlock()

// 	var snapshot SnapshotData
// 	if err := json.Unmarshal(data, &snapshot); err != nil {
// 		logs.Warnf("Failed to unmarshal snapshot data from %s: %v", filePath, err)
// 		return
// 	}

// 	sm.vault = snapshot.Vault
// 	sm.lastExecutedCommitNum = snapshot.LastIncludedIndex
// 	logs.Infof("Successfully loaded snapshot from %s. Last included index: %d.", filePath, sm.lastExecutedCommitNum)
// }
