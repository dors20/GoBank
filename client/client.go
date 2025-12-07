package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"paxos/api"
	"paxos/constants"
	"paxos/logger"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type serverInfo struct {
	pid  int
	cmd  *exec.Cmd
	conn *grpc.ClientConn
}

type serversData struct {
	lock       sync.Mutex
	servers    map[int]*serverInfo
	currLeader int
}

type TestCase struct {
	Sender          string
	Receiver        string
	Amount          int
	IsLeaderFailure bool
}

type TestSet struct {
	SetNumber    int
	Transactions []TestCase
	LiveNodes    []int
}

type clientState struct {
	lock      sync.Mutex
	nextTxnID int64
}

var logs *zap.SugaredLogger
var port string
var sm *serversData
var clientStateManager map[string]*clientState
var lastPrintedViewIndex map[int]int
var cm sync.Mutex

func main() {
	logs = logger.InitLogger(1, false)
	logs.Debug("Client Started")
	defer logs.Debug("Stopping Client")

	if len(os.Args) < 2 {
		logs.Fatalf("Usage: ./client_bin <path_to_csv_file>")
	}

	testFilePath := os.Args[1]

	cmd := exec.Command("go", "build", "-o", "server_bin", "../server/")
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to build server: %v", err)
	}
	logs.Info("Server Binary built")

	sm = &serversData{
		servers:    make(map[int]*serverInfo),
		currLeader: constants.MAX_NODES,
	}

	clientStateManager = make(map[string]*clientState)
	lastPrintedViewIndex = make(map[int]int)

	for i := 1; i <= constants.MAX_NODES; i++ {

		launchCmd := exec.Command("./server_bin", strconv.Itoa(i))
		err = launchCmd.Start()
		if err != nil {
			logs.Fatalf("Failed to start server-%d process: %v", i, err)
		}

		serverPid := launchCmd.Process.Pid
		sm.servers[i] = &serverInfo{pid: serverPid, cmd: launchCmd}
		logs.Infof("Server-%d started successfully with PID: %d", i, serverPid)
	}

	port = constants.BASE_PORT
	logs.Infof("Client communications on port %s", port)

	defer func() {

		logs.Info("No test cases to execute. Shutting down all servers")

		for i := 1; i <= constants.MAX_NODES; i++ {
			if server, ok := sm.servers[i]; ok {
				if server.conn != nil {
					server.conn.Close()
				}
				err = server.cmd.Process.Kill()
				if err != nil {
					logs.Warnf("Failed to stop server-%d with PID: %d due to err: %v", i, sm.servers[i].pid, err)
				} else {
					logs.Warnf("Successfully shut down server-%d with PID: %d", i, sm.servers[i].pid)
				}
			}
		}
		os.Remove("server_bin")
	}()

	logs.Info("Waiting for servers to start")
	fmt.Println("Waiting for servers to start")
	time.Sleep(1 * time.Second)

	logs.Info("Running test Cases")
	testSets, err := parseTestCases(testFilePath)
	if err != nil {
		logs.Fatalf("Failed to parse test cases from '%s': %v", testFilePath, err)
	}
	// run()
	runCLI(testSets)
	printServerStatus()
	logs.Info("Completed all test cases")

}

func parseTestCases(filePath string) ([]TestSet, error) {
	logs.Debug("enter")
	defer logs.Debug("exit")

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var testSets []TestSet
	var currentSet *TestSet

	if _, err := reader.Read(); err != nil {
		return nil, err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		setNumStr := strings.TrimSpace(record[0])
		if setNumStr != "" {
			if currentSet != nil {
				testSets = append(testSets, *currentSet)
			}
			setNum, _ := strconv.Atoi(setNumStr)
			currentSet = &TestSet{SetNumber: setNum}

			nodesStr := strings.Trim(record[2], "[]")
			nodeParts := strings.Split(nodesStr, ",")
			for _, part := range nodeParts {
				nodeIDStr := strings.TrimSpace(part)
				nodeIDStr = strings.TrimPrefix(nodeIDStr, "n")
				nodeID, err := strconv.Atoi(nodeIDStr)
				if err == nil {
					currentSet.LiveNodes = append(currentSet.LiveNodes, nodeID)
				}
			}
		}

		txnStr := strings.TrimSpace(record[1])
		if txnStr == "" {
			continue
		}

		if strings.ToUpper(txnStr) == "LF" {
			currentSet.Transactions = append(currentSet.Transactions, TestCase{Sender: "LF", Receiver: "LF", Amount: 0, IsLeaderFailure: true})
		} else {
			content := strings.Trim(txnStr, "()")
			parts := strings.Split(content, ",")
			if len(parts) == 3 {
				sender := strings.TrimSpace(parts[0])
				receiver := strings.TrimSpace(parts[1])
				amount, err := strconv.Atoi(strings.TrimSpace(parts[2]))
				if err == nil {
					currentSet.Transactions = append(currentSet.Transactions, TestCase{
						Sender:   sender,
						Receiver: receiver,
						Amount:   amount,
					})
				}
			}
		}
	}

	if currentSet != nil {
		testSets = append(testSets, *currentSet)
	}

	return testSets, nil
}

func runCLI(testSets []TestSet) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	scanner := bufio.NewScanner(os.Stdin)
	currentSetIndex := 0

	fmt.Println("\n--- Paxos Client ---")
	fmt.Println("Commands: next, printdb <id>, printlog <id>, printview <id>, printstatus  <txnid>, exit, help")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := strings.Fields(scanner.Text())
		if len(input) == 0 {
			continue
		}
		command := input[0]

		switch command {
		case "next":
			if currentSetIndex < len(testSets) {
				executeTestSet(testSets[currentSetIndex])
				currentSetIndex++
			} else {
				fmt.Println("All test sets have been executed.")
			}
		case "all":
			printAll()
		case "dball":
			printDbAll()
		case "printdb":
			if len(input) != 2 {
				fmt.Println("Usage: printdb <server_id>")
				continue
			}
			serverID, err := strconv.Atoi(input[1])
			if err != nil || serverID < 1 || serverID > constants.MAX_NODES {
				fmt.Println("Invalid server ID.")
				continue
			}
			printDb(serverID)
		case "printlog":
			if len(input) != 2 {
				fmt.Println("Usage: printlog <server_id>")
				continue
			}
			serverID, err := strconv.Atoi(input[1])
			if err != nil || serverID < 1 || serverID > constants.MAX_NODES {
				fmt.Println("Invalid server ID.")
				continue
			}
			printlog(serverID)
		case "printview":
			if len(input) != 2 {
				fmt.Println("Usage: printview <server_id>")
				continue
			}
			serverID, err := strconv.Atoi(input[1])
			if err != nil || serverID < 1 || serverID > constants.MAX_NODES {
				fmt.Println("Invalid server ID.")
				continue
			}
			printView(serverID)
		case "printstatus":
			if len(input) != 2 {
				fmt.Println("Usage: printstatus  <txn_id>")
				continue
			}
			txnID, err := strconv.ParseInt(input[1], 10, 64)
			if err != nil {
				fmt.Println("Invalid transaction ID.")
				continue
			}
			printStatus(txnID)
		case "help":
			fmt.Println("Commands:")
			fmt.Println("  next                - Run the next test set from the CSV file.")
			fmt.Println("  printdb <server_id> - Print the database (vault) of a specific server.")
			fmt.Println("  printlog <server_id>- Print the transaction log of a specific server.")
			fmt.Println("  printview <server_id>- Print new leader views seen by a server since the last call.")
			fmt.Println("  printstatus <client_name> <txn_id> - Get status of a txn. Not implemented on server.")
			fmt.Println("  exit                - Shut down servers and exit the client.")
		case "exit":
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
	}
}

func executeTestSet(ts TestSet) {

	logs.Debug("Enter")
	defer logs.Debug("Exit")
	recoverServer(ts.LiveNodes)

	logs.Infof("--- Executing Test Set %d ---", ts.SetNumber)
	logs.Infof("Live nodes for this set: %v", ts.LiveNodes)

	var wgFailure sync.WaitGroup
	for id := range sm.servers {
		simFailure := true
		if slices.Contains(ts.LiveNodes, id) {
			simFailure = false
		}

		if simFailure {
			wgFailure.Add(1)
			go func(serverID int) {
				defer wgFailure.Done()
				simulateFailure(serverID)
			}(id)
		}
	}
	wgFailure.Wait()
	// Group transactions by sender to run them in dedicated goroutines
	txnsBySender := make(map[string][]TestCase)
	for _, txn := range ts.Transactions {
		if txn.IsLeaderFailure {
			txnsBySender["LF"] = append(txnsBySender["LF"], txn)
		} else {
			txnsBySender[txn.Sender] = append(txnsBySender[txn.Sender], txn)
		}
	}

	var wg sync.WaitGroup
	for sender, txns := range txnsBySender {
		wg.Add(1)
		go func(senderName string, transactions []TestCase) {
			defer wg.Done()
			if senderName == "LF" {
				for range transactions {
					sm.lock.Lock()
					leaderID := sm.currLeader
					logs.Infof("Current Leader is expected to be server-%d", leaderID)
					sm.lock.Unlock()
					simulateLeaderFailure(leaderID)
				}

			} else {
				cm.Lock()
				if _, ok := clientStateManager[senderName]; !ok {
					clientStateManager[senderName] = &clientState{nextTxnID: 1}
				}
				cm.Unlock()
				for _, txn := range transactions {
					makeRequest(senderName, txn.Receiver, txn.Amount)
				}
			}
		}(sender, txns)
	}
	wg.Wait()
	logs.Infof("--- Finished Test Set %d ---", ts.SetNumber)
	printViewAll()
}

func makeRequest(sender, receiver string, amount int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	cm.Lock()
	cs := clientStateManager[sender]
	cm.Unlock()
	cs.lock.Lock()
	txnID := cs.nextTxnID
	cs.nextTxnID++
	cs.lock.Unlock()

	req := &api.Message{
		Sender:    sender,
		Receiver:  receiver,
		Amount:    int32(amount),
		ClientId:  sender,
		Timestamp: txnID,
	}

	sm.lock.Lock()
	leaderID := sm.currLeader
	sm.lock.Unlock()
	client, err := sm.getClientServerConn()
	if err != nil {
		logs.Warnf("Could not get client for leader %d, attempting broadcast.", leaderID)
		broadcastRequest(req)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()

	reply, err := client.Request(ctx, req)
	if err != nil || (reply != nil && strings.Contains(reply.Error, "NOT_LEADER")) {
		logs.Warnf("Request to leader %d failed or was rejected. Broadcasting to all servers.", leaderID)
		broadcastRequest(req)
	} else if reply != nil {
		logs.Infof("Request %s- tnx-%d successful. Leader is %d. Result: %t", sender, txnID, reply.ServerId, reply.Result)
		sm.lock.Lock()
		sm.currLeader = int(reply.ServerId)
		sm.lock.Unlock()
	}
}

func broadcastRequest(req *api.Message) *api.Reply {
	var wg sync.WaitGroup
	replyChan := make(chan *api.Reply, constants.MAX_NODES)

	for id := range sm.servers {
		wg.Add(1)
		go func(serverID int) {
			defer wg.Done()
			client, err := sm.getServerClient(serverID)
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
			defer cancel()
			if reply, err := client.Request(ctx, req); err == nil && reply != nil && !strings.Contains(reply.Error, "NOT_LEADER") && reply.Result == true {
				replyChan <- reply
			}
		}(id)
	}

	wg.Wait()
	close(replyChan)

	if firstReply, ok := <-replyChan; ok {
		logs.Infof("Broadcast done. Got reply %v for request %s-%d", firstReply.Result, req.ClientId, req.Timestamp)
		sm.lock.Lock()
		sm.currLeader = int(firstReply.ServerId)
		sm.lock.Unlock()
		return firstReply
	} else {
		logs.Debugf("Broadcast failed for request %s-%d. No server responded.", req.ClientId, req.Timestamp)
	}

	return nil
}

// Good to have for performance
// Unlock before creating a new connection because it might take time
// Reaquire lock and then add it to server map
func (sm *serversData) getClientServerConn() (api.ClientServerTxnsClient, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	sm.lock.Lock()
	serverID := sm.currLeader
	sm.lock.Unlock()
	return sm.getServerClient(serverID)
}

func (sm *serversData) getServerClient(serverId int) (api.ClientServerTxnsClient, error) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	sm.lock.Lock()
	defer sm.lock.Unlock()

	server, ok := sm.servers[serverId]
	if !ok {
		logs.Warnf("No server with id %d found in server map", serverId)
		return nil, fmt.Errorf("server with ID %d not found", serverId)
	}

	if server.conn == nil {
		logs.Infof("Creating server %d connection for the first time", serverId)
		address := fmt.Sprintf("localhost:%s", constants.ServerPorts[serverId])
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logs.Fatalf("Could not connect to server %d: %v", serverId, err)
			return nil, fmt.Errorf("Could not connect to server %d: %v", serverId, err)
		}
		server.conn = conn
	}

	client := api.NewClientServerTxnsClient(server.conn)

	return client, nil
}

func (sm *serversData) getConn(serverId int) error {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	server, ok := sm.servers[serverId]
	if !ok {
		logs.Warnf("No server with id %d found in server map", serverId)
		return fmt.Errorf("server with ID %d not found", serverId)
	}

	if server.conn == nil {
		logs.Infof("Creating server %d connection for the first time", serverId)
		// TODO Safety check for bound
		address := fmt.Sprintf("localhost:%s", constants.ServerPorts[serverId])
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logs.Fatalf("Could not connect to server %d: %v", serverId, err)
			return fmt.Errorf("Could not connect to server %d: %v", serverId, err)
		}
		server.conn = conn
	}
	return nil
}

func printServerStatus() {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	for i := 1; i <= constants.MAX_NODES; i++ {
		// Print DB
		printDb(i)
		// Print Log
		printlog(i)
	}

}

func printStatus(seqnum int64) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	for i := 1; i <= constants.MAX_NODES; i++ {
		printReqStatus(i, int(seqnum))
	}

}

func printReqStatus(serverID, seqNum int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	client, err := getPrintClient(serverID)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()

	status, err := client.PrintStatus(ctx, &api.RequestInfo{SeqNum: int32(seqNum)})
	if err != nil {
		logs.Warnf("PrintStatus failed: %v", err)
	} else {
		logs.Infof("*PRINTSTATUS* Server %d Status for Seq #%d: %s", serverID, seqNum, status.GetStatus().String())
		//fmt.Printf("*PRINTSTATUS* Server %d Status for Seq #%d: %s", serverID, seqNum, status.GetStatus().String())
	}

}

func printDb(serverID int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	client, err := getPrintClient(serverID)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()

	db, err := client.PrintDB(ctx, &api.Blank{})
	if err != nil {
		logs.Warnf("PrintDB failed: %v", err)
	} else {
		logs.Infof("*PRINT DB* Server %d Vault: %v", serverID, db.GetVault())
		//fmt.Printf("*PRINT DB* Server %d Vault: %v", serverID, db.GetVault())
	}
}

func printlog(serverID int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	client, err := getPrintClient(serverID)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()

	logStore, err := client.PrintLog(ctx, &api.Blank{})
	if err != nil {
		logs.Warnf("PrintLog failed: %v", err)
	} else {
		logs.Infof("Server %d Log:", serverID)
		//fmt.Printf("Server %d Log:", serverID)
		for _, entry := range logStore.GetLogs() {
			logs.Infof("*PRINT LOGS* - Seq: %d, From: %s, To: %s, Amt: %d, Committed: %t, ballotVal: %d, serverID: %d", entry.GetSeqNum(), entry.GetSender(), entry.GetReceiver(), entry.GetAmount(), entry.GetIsCommitted(), entry.GetBallotVal(), entry.ServerId)
			//fmt.Printf("*PRINT LOGS* - Seq: %d, From: %s, To: %s, Amt: %d, Committed: %t, ballotVal: %d, serverID: %d", entry.GetSeqNum(), entry.GetSender(), entry.GetReceiver(), entry.GetAmount(), entry.GetIsCommitted(), entry.GetBallotVal(), entry.ServerId)
		}
	}
}

func printAll() {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	for id := range sm.servers {
		printDb(id)
		printlog(id)
	}
}

func printDbAll() {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	for id := range sm.servers {
		printDb(id)
	}
}

func printViewAll() {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	for i := 1; i <= constants.MAX_NODES; i++ {
		printView(i)
	}
}

func printView(serverID int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	client, err := getPrintClient(serverID)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()

	viewHistory, err := client.PrintView(ctx, &api.Blank{})
	if err != nil {
		logs.Warnf("PrintView for server %d failed: %v", serverID, err)
	} else {
		allViews := viewHistory.GetViews()
		lastIndex := lastPrintedViewIndex[serverID]
		newViews := allViews[lastIndex:]

		if len(newViews) > 0 {
			fmt.Printf("--- New Leader Views (Server %d) ---\n", serverID)
			for _, view := range newViews {
				fmt.Printf("  - Leader <%d, %d> established.\n", view.GetBallotVal(), view.GetServerId())
			}
			lastPrintedViewIndex[serverID] = len(allViews) // Update the index
		} else {
			fmt.Printf("No new leader views for server %d since last check.\n", serverID)
		}
	}
}

func getPrintClient(serverID int) (api.PaxosPrintInfoClient, error) {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	server, ok := sm.servers[serverID]
	if !ok {
		logs.Warnf("No server info for server-%d found", serverID)
		return nil, fmt.Errorf("no server Info found")
	}
	if server.conn == nil {
		err := sm.getConn(serverID)
		if err != nil {
			logs.Warnf("Failed to create conn for server-%d", serverID)
			return nil, fmt.Errorf("failed to create client")
		}
	}
	client := api.NewPaxosPrintInfoClient(server.conn)

	return client, nil
}

func simulateFailure(serverId int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	client, err := sm.getServerClient(serverId)
	if err != nil {
		logs.Warnf("Failed to Simulate Failure for server-%d", serverId)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()
	_, err = client.SimulateNodeFailure(ctx, &api.Blank{})
	if err != nil {
		logs.Warnf("Failed to Simulate Failure for server-%d with error: %v", serverId, err)
	} else {
		logs.Infof("Simulated server-%d Failure", serverId)
	}
}

func simulateLeaderFailure(serverId int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	client, err := sm.getServerClient(serverId)
	if err != nil {
		logs.Warnf("Failed to Simulate Failure for server-%d", serverId)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
	defer cancel()
	_, err = client.SimulateNodeFailure(ctx, &api.Blank{})
	if err != nil {
		logs.Warnf("Failed to Simulate Failure for server-%d with error: %v", serverId, err)
	} else {
		logs.Infof("Simulated server-%d Failure", serverId)
	}
}

func recoverServer(nodes []int) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")
	var wg sync.WaitGroup

	for _, id := range nodes {
		wg.Add(1)
		go func(serverID int) {
			defer wg.Done()

			client, err := sm.getServerClient(id)
			if err != nil {
				logs.Warnf("Failed to recover server-%d", id)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
			defer cancel()
			_, err = client.RecoverNode(ctx, &api.Blank{})
			if err != nil {
				logs.Warnf("Failed to recover server-%d with error: %v", id, err)
			} else {
				logs.Infof("recovered server-%d ", id)
			}
		}(id)
	}
	wg.Wait()
}

// func run() {
// 	logs.Debug("Enter")
// 	defer logs.Debug("Exit")

// 	var wg sync.WaitGroup
// 	wg.Add(7)
// 	t := time.Now().UnixNano()
// 	go sm.makeRequest(&wg, "Alice", "Bob", 10, t)
// 	go sm.makeRequest(&wg, "Alice", "Bob", 30, time.Now().UnixNano())
// 	go sm.makeRequest(&wg, "Bob", "Alice", 60, time.Now().UnixNano())
// 	go sm.makeRequest(&wg, "Alice", "Bob", 25, time.Now().UnixNano())
// 	go sm.makeRequest(&wg, "A", "B", 30, time.Now().UnixNano())
// 	go sm.makeRequest(&wg, "B", "A", 60, time.Now().UnixNano())
// 	go sm.makeRequest(&wg, "A", "B", 25, time.Now().UnixNano())
// 	wg.Wait()
// 	// Duplicate request
// 	time.Sleep(20 * time.Second)
// 	var wg1 sync.WaitGroup
// 	wg1.Add(1)
// 	go sm.makeRequest(&wg1, "Alice", "Bob", 10, time.Now().UnixNano())
// 	wg1.Wait()
// 	time.Sleep(20 * time.Second)
// 	var wg2 sync.WaitGroup
// 	wg2.Add(3)
// 	go sm.makeRequest(&wg2, "Mark", "jack", 30, time.Now().UnixNano())
// 	go sm.makeRequest(&wg2, "jack", "Alice", 60, time.Now().UnixNano())
// 	go sm.makeRequest(&wg2, "jack", "Bob", 25, time.Now().UnixNano())
// 	wg2.Wait()
// 	time.Sleep(30 * time.Second)
// 	var wg3 sync.WaitGroup
// 	wg3.Add(2)
// 	go sm.makeRequest(&wg3, "jack", "jack", 60, time.Now().UnixNano())
// 	go sm.makeRequest(&wg3, "zack", "cat", 25, time.Now().UnixNano())
// 	wg3.Wait()
// 	time.Sleep(5 * time.Second)
// 	printReqStatus(1, 1)
// 	printServerStatus(1)
// 	printServerStatus(2)
// 	printServerStatus(3)
// 	printView(1)
// 	printView(2)
// 	printView(3)
// 	logs.Info("All requests completed")
// }
