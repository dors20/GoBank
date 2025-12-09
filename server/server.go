package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net"
	"os"
	"paxos/api"
	"paxos/constants"
	"paxos/logger"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	}

	pm := &PeerManager{
		peers: make(map[int]*Peer),
	}
	s := constants.Follower

	server = &ServerImpl{
		id:              id,
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
	if id == 1 {
		server.state = constants.Leader
		server.ballot.BallotVal = 1
		server.ballot.ServerID = 1
		server.leaderBallot.BallotVal = 1
		server.leaderBallot.ServerID = 1
	} else {
		server.leaderBallot.BallotVal = 1
		server.leaderBallot.ServerID = 1
	}
	server.port = constants.ServerPorts[id]
	server.peerManager.initPeerConnections(server.id) // Note - not doing as a singleton on becoming first time leader for simplicity
	logStore.unmarshal()
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

	if in.GetTimestamp() <= client.lastRequestTimeStamp {
		logs.Infof("Recieved duplicate request from client-%s", in.ClientId)
		resp := client.lastResponse
		s.clientManager.lock.Unlock()
		if resp == nil {
			resp = &api.Reply{Result: false, ServerId: int32(s.id), Error: "Invalid stale request not found in cache"}
		}

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

	s.seqNum++
	currSeqNum := s.seqNum
	record := &LogRecord{
		SeqNum: currSeqNum,
		Ballot: &BallotNumber{BallotVal: int(s.ballot.BallotVal), ServerID: s.id},
		Txn: &ClientRequestTxn{
			Sender:   in.Sender,
			Reciever: in.Receiver,
			Amount:   int(in.Amount),
		},
		IsCommitted: false,
	}
	// NOTE - need to unlock here otherwise we are doing sequential processing
	s.lock.Unlock()

	logStore.append(record)
	execChannel := sm.registerSignal(currSeqNum)

	// TODO Can make this a server.Qourun const - Good to have
	quorum := int((constants.MAX_NODES / 2) + 1)

	ok = false
	// *REVIEW* Do we want an Infinite for LOOP
	// TODO NEED to Handle on follower side, if its already accepted this message then it should still reply
	// Need to check if this server is still leader and if not we need to insert no-op in that log index
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
		logs.Infof("Cached reply for client-%s with request timestamp: %d", in.ClientId, in.GetTimestamp())
	}
	return reply, nil

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

		if _, ok := sm.vault[sender]; !ok {
			sm.vault[sender] = constants.INITIAL_BALANCE
			logs.Infof("Detected new user, Initializing Balance for User: %s with %d", sender, constants.INITIAL_BALANCE)
		}
		if _, ok := sm.vault[reciever]; !ok {
			sm.vault[reciever] = constants.INITIAL_BALANCE
			logs.Infof("Detected new user, Initializing Balance for User: %s with %d", reciever, constants.INITIAL_BALANCE)
		}

		var res bool
		if sm.vault[sender] >= logToApply.Txn.Amount {
			sm.vault[sender] -= logToApply.Txn.Amount
			sm.vault[reciever] += logToApply.Txn.Amount
			res = true
			logs.Infof("Successfully applied txn with commitIdx %d and segNum %d", nextCommitIdx, logToApply.SeqNum)
		} else {
			logs.Warnf("Insufficient Balance for txn with commitIdx %d and segNum %d", nextCommitIdx, logToApply.SeqNum)
		}
		sm.lastExecutedCommitNum++
		logToApply.IsExecuted = true
		execChan, ok := sm.executionStatus[logToApply.SeqNum]
		if ok {
			execChan <- res
			close(execChan)
			delete(sm.executionStatus, logToApply.SeqNum)
		}
		sm.lock.Unlock()
		logStore.lock.Unlock()
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

	go ls.marshal()
}

func (sm *StateMachine) applyTxn() {

	logs.Debug("Enter")
	defer logs.Debug("Exit")

	select {
	case sm.execPool <- struct{}{}:
	default:
	}

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

	for serverId := 1; serverId <= constants.MAX_NODES; serverId++ {
		if serverId == currServer {
			continue
		}

		addr := fmt.Sprintf("localhost:%s", constants.ServerPorts[serverId])
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logs.Warnf("failed to create connection to server %d: %v", serverId, err) // *NOTE* Not using Fatal , we can treat this as a non byzantine problem
		}
		// TODO also api.Leader Election clients
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
		}
		logStore.records[seqNum] = record
	}
	record.IsCommitted = true
	logStore.lock.Unlock()

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
	return
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	ls.lock.Lock()
	recordsCopy := make(map[int]*LogRecord, len(ls.records))
	maps.Copy(recordsCopy, ls.records)
	ls.lock.Unlock()

	data, err := json.MarshalIndent(recordsCopy, "", "  ")
	if err != nil {
		logs.Warnf("Failed to marshal log store: %v", err)
		return
	}

	filePath := ls.logsPath
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		logs.Warnf("Failed to write log store to file %s: %v", filePath, err)
	}
}

func (ls *LogStore) unmarshal() {
	return
	logs.Debug("enter")
	defer logs.Debug("Exit")

	filePath := ls.logsPath
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		logs.Infof("Log file %s not found, starting with an empty log store.", filePath)
		return
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		logs.Warnf("Failed to read log file %s: %v", filePath, err)
		return
	}

	if len(data) == 0 {
		logs.Infof("Log file %s is empty, starting with an empty log store.", filePath)
		return
	}

	ls.lock.Lock()
	defer ls.lock.Unlock()

	var records map[int]*LogRecord
	err = json.Unmarshal(data, &records)
	if err != nil {
		logs.Warnf("Failed to unmarshal log data from %s: %v", filePath, err)
		return
	}

	maxSeq := 0
	for seq, record := range records {
		record.IsCommitted = false
		record.IsExecuted = false
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	ls.records = records
	if server != nil {
		server.seqNum = maxSeq
	}
	logs.Infof("Successfully loaded %d records from log file %s. Max seqNum set to %d.", len(ls.records), filePath, maxSeq)
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
			for id := 1; id <= constants.MAX_NODES; id++ {
				if id == s.id {
					continue
				}
				p, ok := s.peerManager.peers[id]
				if !ok || p.conn == nil {
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
			} else {
				logStore.records[target] = &LogRecord{
					SeqNum: target,
					Ballot: &BallotNumber{BallotVal: int(pulled.BallotVal), ServerID: int(pulled.ServerId)},
					Txn: &ClientRequestTxn{
						Sender:   pulled.Sender,
						Reciever: pulled.Receiver,
						Amount:   int(pulled.Amount),
					},
					IsCommitted: true,
				}
			}
			logStore.lock.Unlock()
			go logStore.marshal()
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
