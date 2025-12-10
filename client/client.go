package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"paxos/api"
	"paxos/constants"
	"paxos/logger"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"os/exec"

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
var sm *serversData

var buildServerBinaryOnce sync.Once

func ensureServerBinary() error {
	var buildErr error
	buildServerBinaryOnce.Do(func() {
		if _, err := os.Stat("server_bin"); err == nil {
			return
		}
		cmd := exec.Command("go", "build", "-o", "server_bin", "../server/")
		if err := cmd.Run(); err != nil {
			buildErr = err
		} else {
			logs.Infof("Server binary built successfully")
		}
	})
	return buildErr
}

var clientStateManager map[string]*clientState
var lastPrintedViewIndex map[int]int
var cm sync.Mutex

type CmdType int

const (
	CmdTransfer CmdType = iota
	CmdRead
	CmdFail
	CmdRecover
)

type Cmd struct {
	Type     CmdType
	Sender   int
	Receiver int
	Amount   int
	Account  int
	NodeID   int
}

type InputSet struct {
	SetNumber int
	LiveNodes []int
	Commands  []Cmd
}

type perfStats struct {
	txnCount     int64
	totalLatency time.Duration
	maxLatency   time.Duration
	start        time.Time
	end          time.Time
	lock         sync.Mutex
}

func (p *perfStats) record(latency time.Duration) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.txnCount == 0 {
		p.start = time.Now()
	}
	p.txnCount++
	p.totalLatency += latency
	if latency > p.maxLatency {
		p.maxLatency = latency
	}
}

func (p *perfStats) finish() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.start.IsZero() {
		return
	}
	p.end = time.Now()
}

func main() {
	logs = logger.InitLogger(1, false)
	logs.Debug("Client Started")
	defer logs.Debug("Stopping Client")

	if len(os.Args) < 2 {
		logs.Fatalf("Usage: ./client_bin <path_to_csv_file>")
	}

	testFilePath := os.Args[1]

	if err := ensureServerBinary(); err != nil {
		logs.Fatalf("Failed to build server binary: %v", err)
	}
	defer os.Remove("server_bin")

	testSets, err := parseTestCasesForClient(testFilePath)
	if err != nil {
		logs.Fatalf("Failed to parse test cases from '%s': %v", testFilePath, err)
	}

	runCLI(testSets)
}

func parseCommand(s string) (Cmd, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Cmd{}, fmt.Errorf("empty command")
	}

	if strings.HasPrefix(s, "F(") && strings.HasSuffix(s, ")") {
		inner := strings.TrimSpace(s[2 : len(s)-1])
		inner = strings.TrimSpace(strings.TrimPrefix(inner, "n"))
		id, err := strconv.Atoi(inner)
		if err != nil {
			return Cmd{}, err
		}
		return Cmd{Type: CmdFail, NodeID: id}, nil
	}

	if strings.HasPrefix(s, "R(") && strings.HasSuffix(s, ")") {
		inner := strings.TrimSpace(s[2 : len(s)-1])
		inner = strings.TrimSpace(strings.TrimPrefix(inner, "n"))
		id, err := strconv.Atoi(inner)
		if err != nil {
			return Cmd{}, err
		}
		return Cmd{Type: CmdRecover, NodeID: id}, nil
	}

	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		inner := strings.TrimSpace(s[1 : len(s)-1])
		if inner == "" {
			return Cmd{}, fmt.Errorf("empty tuple command")
		}
		parts := strings.Split(inner, ",")
		if len(parts) == 1 {
			account, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			if err != nil {
				return Cmd{}, err
			}
			return Cmd{Type: CmdRead, Account: account}, nil
		}
		if len(parts) == 3 {
			sender, err := strconv.Atoi(strings.TrimSpace(parts[0]))
			if err != nil {
				return Cmd{}, err
			}
			receiver, err := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil {
				return Cmd{}, err
			}
			amount, err := strconv.Atoi(strings.TrimSpace(parts[2]))
			if err != nil {
				return Cmd{}, err
			}
			return Cmd{
				Type:     CmdTransfer,
				Sender:   sender,
				Receiver: receiver,
				Amount:   amount,
			}, nil
		}
	}

	return Cmd{}, fmt.Errorf("unrecognized command format: %s", s)
}

func parseTestCasesForClient(path string) ([]InputSet, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil {
		return nil, err
	}

	var sets []InputSet
	var current *InputSet

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(rec) < 2 {
			continue
		}

		setStr := strings.TrimSpace(rec[0])
		if setStr != "" {
			if current != nil {
				sets = append(sets, *current)
			}
			n, err := strconv.Atoi(setStr)
			if err != nil {
				return nil, err
			}
			current = &InputSet{SetNumber: n}

			if len(rec) >= 3 {
				liveStr := strings.TrimSpace(rec[2])
				if liveStr != "" {
					nodesStr := strings.Trim(liveStr, "[]")
					if nodesStr != "" {
						parts := strings.Split(nodesStr, ",")
						for _, p := range parts {
							s := strings.TrimSpace(p)
							s = strings.TrimPrefix(s, "n")
							if s == "" {
								continue
							}
							id, err := strconv.Atoi(s)
							if err != nil {
								return nil, err
							}
							current.LiveNodes = append(current.LiveNodes, id)
						}
					}
				}
			}
		}

		if current == nil {
			continue
		}

		cmdStr := strings.TrimSpace(rec[1])
		if cmdStr == "" {
			continue
		}
		cmd, err := parseCommand(cmdStr)
		if err != nil {
			return nil, err
		}
		current.Commands = append(current.Commands, cmd)
	}

	if current != nil {
		sets = append(sets, *current)
	}

	return sets, nil
}

func leaderForAccount(account int) int {
	clusterID := constants.ClusterForAccountID(account)
	if clusterID < 0 {
		return 1
	}
	servers, ok := constants.ClusterServers[clusterID]
	if !ok || len(servers) == 0 {
		return 1
	}
	// Use the first server in the cluster as the initial contact;
	// the server will forward to the current leader if needed.
	return servers[0]
}

func runSetWithManagerCLI(m *TestCaseManager, set InputSet, stats *perfStats) error {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	baseTs := time.Now().UnixNano()
	seq := int64(1)
	stats.start = time.Now()

	logs.Infof("Executing set %d with %d commands and live nodes %v", set.SetNumber, len(set.Commands), set.LiveNodes)

	var wg sync.WaitGroup

	for _, cmd := range set.Commands {
		switch cmd.Type {
		case CmdTransfer:
			ts := baseTs + seq
			seq++
			c := cmd
			wg.Add(1)
			go func() {
				defer wg.Done()
				clusterID := constants.ClusterForAccountID(c.Sender)
				serverIDs, ok := constants.ClusterServers[clusterID]
				if !ok || len(serverIDs) == 0 {
					logs.Warnf("Transfer txn in set %d from %d to %d amount=%d has no servers for cluster %d", set.SetNumber, c.Sender, c.Receiver, c.Amount, clusterID)
					return
				}
				req := &api.Message{
					Sender:    strconv.Itoa(c.Sender),
					Receiver:  strconv.Itoa(c.Receiver),
					Amount:    int32(c.Amount),
					ClientId:  fmt.Sprintf("%d_%d", c.Sender, ts),
					Timestamp: ts,
				}
				for attempt := 0; attempt < 20; attempt++ {
					if attempt > 0 {
						time.Sleep(time.Duration(constants.LEADER_TIMEOUT_SECONDS) * time.Millisecond)
					}
					sid := serverIDs[attempt%len(serverIDs)]
					client, err := m.getServerClient(sid)
					if err != nil {
						logs.Warnf("Transfer txn in set %d from %d to %d amount=%d failed to get client for server-%d: %v", set.SetNumber, c.Sender, c.Receiver, c.Amount, sid, err)
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
					start := time.Now()
					reply, err := client.Request(ctx, req)
					cancel()
					stats.record(time.Since(start))
					if err != nil {
						logs.Warnf("Transfer txn in set %d from %d to %d amount=%d via server-%d failed: %v", set.SetNumber, c.Sender, c.Receiver, c.Amount, sid, err)
						continue
					}
					if reply == nil {
						logs.Warnf("Transfer txn in set %d from %d to %d amount=%d via server-%d returned nil reply", set.SetNumber, c.Sender, c.Receiver, c.Amount, sid)
						continue
					}
					if reply.Error != "" {
						// Treat business-logic rejections as final, everything else retryable.
						if strings.Contains(reply.Error, "INSUFFICIENT_BALANCE") ||
							// strings.Contains(reply.Error, "LOCK_CONFLICT") ||
							strings.Contains(reply.Error, "INSUFFICIENT_QUORUM") {
							logs.Infof("Transfer txn in set %d from %d to %d amount=%d rejected by server-%d: %s", set.SetNumber, c.Sender, c.Receiver, c.Amount, sid, reply.Error)
							return
						}
						logs.Infof("Transfer txn in set %d from %d to %d amount=%d via server-%d got retryable error=%s", set.SetNumber, c.Sender, c.Receiver, c.Amount, sid, reply.Error)
						continue
					}
					if reply.Error != "" && strings.Contains(reply.Error, "LOCK_CONFLICT") {
						time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
						continue
					}
					if !reply.Result {
						logs.Infof("Transfer txn in set %d from %d to %d amount=%d via server-%d returned result=false (no error). Treating as processed/final.", set.SetNumber, c.Sender, c.Receiver, c.Amount, sid)
						return
					}
					logs.Infof("Transfer txn in set %d from %d to %d amount=%d completed via server-%d (leader=%d)", set.SetNumber, c.Sender, c.Receiver, c.Amount, sid, reply.ServerId)
					return
				}
				logs.Warnf("Transfer txn in set %d from %d to %d amount=%d exhausted retries in cluster %d", set.SetNumber, c.Sender, c.Receiver, c.Amount, clusterID)
			}()
		case CmdRead:
			c := cmd
			wg.Add(1)
			go func() {
				defer wg.Done()
				clusterID := constants.ClusterForAccountID(c.Account)
				serverIDs, ok := constants.ClusterServers[clusterID]
				if !ok || len(serverIDs) == 0 {
					logs.Warnf("Read-only txn in set %d for account %d has no servers for cluster %d", set.SetNumber, c.Account, clusterID)
					return
				}
				for attempt := 0; attempt < 20; attempt++ {
					if attempt > 0 {
						time.Sleep(time.Duration(constants.LEADER_TIMEOUT_SECONDS) * time.Millisecond)
					}
					sid := serverIDs[attempt%len(serverIDs)]
					client, err := m.getServerClient(sid)
					if err != nil {
						logs.Warnf("Read-only txn in set %d for account %d failed to get client for server-%d: %v", set.SetNumber, c.Account, sid, err)
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
					start := time.Now()
					resp, err := client.GetBalance(ctx, &api.BalanceRequest{Account: strconv.Itoa(c.Account)})
					cancel()
					stats.record(time.Since(start))
					if err != nil {
						logs.Warnf("Read-only txn in set %d for account %d via server-%d failed: %v", set.SetNumber, c.Account, sid, err)
						continue
					}
					bal := resp.GetBalance()
					logs.Infof("Read-only txn in set %d for account %d via server-%d returned balance=%d", set.SetNumber, c.Account, sid, bal)
					fmt.Printf("READ account=%d balance=%d\n", c.Account, bal)
					return
				}
				logs.Warnf("Read-only txn in set %d for account %d exhausted retries in cluster %d", set.SetNumber, c.Account, clusterID)
			}()
		case CmdFail:
			wg.Wait()
			if err := m.FailNode(cmd.NodeID); err != nil {
				logs.Warnf("FailNode(%d) issued in set %d failed: %v", cmd.NodeID, set.SetNumber, err)
				return err
			}
			logs.Infof("FailNode(%d) issued in set %d", cmd.NodeID, set.SetNumber)
		case CmdRecover:
			wg.Wait()
			if err := m.RecoverNode(cmd.NodeID); err != nil {
				logs.Warnf("RecoverNode(%d) issued in set %d failed: %v", cmd.NodeID, set.SetNumber, err)
				return err
			}
			logs.Infof("RecoverNode(%d) issued in set %d", cmd.NodeID, set.SetNumber)
		}
	}

	wg.Wait()

	stats.finish()
	logs.Infof("Finished executing set %d", set.SetNumber)
	return nil
}

func cliPrintBalance(m *TestCaseManager, account int) {
	clusterID := constants.ClusterForAccountID(account)
	if clusterID < 0 {
		fmt.Printf("Could not determine cluster for account %d\n", account)
		return
	}

	serverIDs, ok := constants.ClusterServers[clusterID]
	if !ok || len(serverIDs) == 0 {
		fmt.Printf("No servers configured for cluster %d\n", clusterID)
		return
	}

	parts := make([]string, 0, len(serverIDs))

	for _, sid := range serverIDs {
		client, err := m.getServerClient(sid)
		if err != nil {
			parts = append(parts, fmt.Sprintf("n%d : error", sid))
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		resp, err := client.GetBalance(ctx, &api.BalanceRequest{Account: strconv.Itoa(account)})
		cancel()
		if err != nil {
			parts = append(parts, fmt.Sprintf("n%d : error", sid))
			continue
		}
		parts = append(parts, fmt.Sprintf("n%d : %d", sid, resp.GetBalance()))
	}

	fmt.Println(strings.Join(parts, ", "))
}

func cliPrintDB(m *TestCaseManager) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	maxID := m.maxServerID()
	for sid := 1; sid <= maxID; sid++ {
		client, err := m.getPrintClient(sid)
		if err != nil {
			fmt.Printf("PrintDB: server %d unavailable: %v\n", sid, err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		db, err := client.PrintDB(ctx, &api.Blank{})
		cancel()
		if err != nil {
			fmt.Printf("PrintDB failed for server %d: %v\n", sid, err)
			continue
		}
		logs.Infof("*PRINT DB* Server %d Vault: %v", sid, db.GetVault())
		fmt.Printf("*PRINT DB* Server %d Vault: %v\n", sid, db.GetVault())
	}
}

func cliPrintView(m *TestCaseManager) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	maxID := m.maxServerID()
	for sid := 1; sid <= maxID; sid++ {
		client, err := m.getPrintClient(sid)
		if err != nil {
			fmt.Printf("PrintView: server %d unavailable: %v\n", sid, err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		views, err := client.PrintView(ctx, &api.Blank{})
		cancel()
		if err != nil {
			fmt.Printf("PrintView failed for server %d: %v\n", sid, err)
			continue
		}
		if len(views.GetViews()) == 0 {
			continue
		}
		fmt.Printf("Server %d new views:\n", sid)
		for _, v := range views.GetViews() {
			logs.Infof("Server %d new view: ballot=%d leader=%d", sid, v.GetBallotVal(), v.GetServerId())
			fmt.Printf("  ballot=%d leader=%d\n", v.GetBallotVal(), v.GetServerId())
		}
	}
}

func printPerformance(stats *perfStats) {
	if stats == nil || stats.txnCount == 0 {
		fmt.Println("No transactions recorded for this set.")
		return
	}
	totalWindow := stats.end.Sub(stats.start)
	if totalWindow <= 0 {
		totalWindow = time.Nanosecond
	}
	avgLatencyMs := float64(stats.totalLatency.Milliseconds()) / float64(stats.txnCount)
	throughput := float64(stats.txnCount) / totalWindow.Seconds()
	logs.Infof("Performance: txns=%d throughput=%.2f txns/s avg-latency=%.3f ms max-latency=%.3f ms",
		stats.txnCount, throughput, avgLatencyMs, float64(stats.maxLatency.Milliseconds()))
	fmt.Printf("Performance: txns=%d throughput=%.2f txns/s avg-latency=%.3f ms max-latency=%.3f ms\n", stats.txnCount, throughput, avgLatencyMs, float64(stats.maxLatency.Milliseconds()))
}

func cliPrintReshard() {
	fmt.Println("Resharding not implemented.")
}

func cliPrintLogs(m *TestCaseManager) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	maxID := m.maxServerID()
	for sid := 1; sid <= maxID; sid++ {
		client, err := m.getPrintClient(sid)
		if err != nil {
			fmt.Printf("PrintLog: server %d unavailable: %v\n", sid, err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		logStore, err := client.PrintLog(ctx, &api.Blank{})
		cancel()
		if err != nil {
			fmt.Printf("PrintLog failed for server %d: %v\n", sid, err)
			continue
		}
		fmt.Printf("Server %d Log:\n", sid)
		for _, entry := range logStore.GetLogs() {
			logs.Infof("*PRINT LOGS* - Seq: %d, From: %s, To: %s, Amt: %d, Committed: %t, ballotVal: %d, serverID: %d, phase: %s, txId: %s",
				entry.GetSeqNum(), entry.GetSender(), entry.GetReceiver(), entry.GetAmount(), entry.GetIsCommitted(), entry.GetBallotVal(), entry.ServerId, entry.GetPhase(), entry.GetTxId())
			fmt.Printf("*PRINT LOGS* - Seq: %d, From: %s, To: %s, Amt: %d, Committed: %t, ballotVal: %d, serverID: %d, phase: %s, txId: %s\n",
				entry.GetSeqNum(), entry.GetSender(), entry.GetReceiver(), entry.GetAmount(), entry.GetIsCommitted(), entry.GetBallotVal(), entry.ServerId, entry.GetPhase(), entry.GetTxId())
		}
	}
}

func runCLI(testSets []InputSet) {
	logs.Debug("Enter")
	defer logs.Debug("Exit")

	scanner := bufio.NewScanner(os.Stdin)
	currentIndex := -1
	var manager *TestCaseManager
	var currentSet *InputSet
	var stats *perfStats

	fmt.Println("\n--- Project 3 Client ---")
	fmt.Println("Commands: next, skip, printbalance <account>, printdb, printview, printlog, performance, exit, help")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "printbalance(") && strings.HasSuffix(lower, ")") {
			if manager == nil {
				fmt.Println("No active test set. Run 'next' first.")
				continue
			}
			inner := strings.TrimSpace(line[len("PrintBalance(") : len(line)-1])
			account, err := strconv.Atoi(inner)
			if err != nil {
				fmt.Println("Invalid account id.")
				continue
			}
			cliPrintBalance(manager, account)
			continue
		}

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		cmd := strings.ToLower(fields[0])

		switch cmd {
		case "next":
			if manager != nil {
				logs.Infof("Stopping previous set before starting next")
				manager.Stop()
				manager = nil
			}
			if currentIndex+1 >= len(testSets) {
				fmt.Println("All test sets have been processed.")
				continue
			}
			currentIndex++
			set := testSets[currentIndex]
			currentSet = &set
			logs.Infof("Starting set %d", set.SetNumber)
			fmt.Printf("Starting test set %d with live nodes %v\n", set.SetNumber, set.LiveNodes)
			m := NewTestCaseManager()
			if err := m.Start(set.LiveNodes); err != nil {
				fmt.Printf("Failed to start servers for set %d: %v\n", set.SetNumber, err)
				continue
			}
			if err := m.initConnections(); err != nil {
				fmt.Printf("Failed to init connections for set %d: %v\n", set.SetNumber, err)
				m.Stop()
				continue
			}
			stats = &perfStats{}
			if err := runSetWithManagerCLI(m, set, stats); err != nil {
				fmt.Printf("Error executing set %d: %v\n", set.SetNumber, err)
			}
			manager = m
			logs.Infof("Finished set %d", set.SetNumber)
			fmt.Printf("Finished test set %d.\n", set.SetNumber)
		case "skip":
			if manager != nil {
				logs.Infof("Stopping previous set before skipping to next")
				manager.Stop()
				manager = nil
			}
			if currentIndex+1 >= len(testSets) {
				fmt.Println("All test sets have been processed.")
				continue
			}
			currentIndex++
			set := testSets[currentIndex]
			currentSet = nil
			logs.Infof("Skipping set %d", set.SetNumber)
			fmt.Printf("Skipping test set %d\n", set.SetNumber)
		case "printbalance":
			if len(fields) != 2 {
				fmt.Println("Usage: printbalance <account>")
				continue
			}
			if manager == nil {
				fmt.Println("No active test set. Run 'next' first.")
				continue
			}
			account, err := strconv.Atoi(fields[1])
			if err != nil {
				fmt.Println("Invalid account id.")
				continue
			}
			cliPrintBalance(manager, account)
		case "printdb":
			if manager == nil {
				fmt.Println("No active test set. Run 'next' first.")
				continue
			}
			logs.Infof("CLI printdb for current set index %d", currentIndex)
			cliPrintDB(manager)
		case "printview":
			if manager == nil {
				fmt.Println("No active test set. Run 'next' first.")
				continue
			}
			logs.Infof("CLI printview for current set index %d", currentIndex)
			cliPrintView(manager)
		case "printlog":
			if manager == nil {
				fmt.Println("No active test set. Run 'next' first.")
				continue
			}
			logs.Infof("CLI printlog for current set index %d", currentIndex)
			cliPrintLogs(manager)
		case "performance":
			if stats == nil || stats.txnCount == 0 {
				fmt.Println("No performance data for current set.")
				continue
			}
			logs.Infof("CLI performance for current set index %d", currentIndex)
			printPerformance(stats)
		case "printreshard":
			cliPrintReshard()
		case "help":
			fmt.Println("Commands: next, skip, printbalance <account>, printdb, printview, performance, exit, help")
		case "exit":
			if manager != nil {
				logs.Infof("Stopping manager on exit")
				manager.Stop()
				manager = nil
			}
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
		}
		_ = currentSet
	}
}
func executeTestSet(ts TestSet) {

	logs.Debug("Enter")
	defer logs.Debug("Exit")
	start := time.Now()
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
	elapsed := time.Since(start)
	fmt.Printf("Test Set %d completed in %v\n", ts.SetNumber, elapsed)
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

	lockRetry := 0
	for i := 0; i < 20; i++ {
		reply := sendWithRetryOnce(req)
		if reply != nil && !strings.Contains(reply.Error, "NOT_LEADER") {
			if reply.Error == "LOCK_CONFLICT" {
				if lockRetry < 10 {
					lockRetry++
					backoff := 30 + rand.Intn(40) + 10*lockRetry
					time.Sleep(time.Duration(backoff) * time.Millisecond)
					continue
				}
			}
			if reply.Error == "INSUFFICIENT_QUORUM" {
				return
			}
			logs.Infof("Request %s- tnx-%d completed. Leader is %d. Result: %t", sender, txnID, reply.ServerId, reply.Result)
			sm.lock.Lock()
			sm.currLeader = int(reply.ServerId)
			sm.lock.Unlock()
			return
		}
		time.Sleep(time.Duration(constants.LEADER_TIMEOUT_SECONDS) * time.Millisecond)
	}
}

func sendWithRetryOnce(req *api.Message) *api.Reply {
	sm.lock.Lock()
	leaderID := sm.currLeader
	sm.lock.Unlock()

	client, err := sm.getClientServerConn()
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), constants.REQUEST_TIMEOUT*time.Millisecond)
		reply, errReq := client.Request(ctx, req)
		cancel()
		if errReq == nil && reply != nil && !strings.Contains(reply.Error, "NOT_LEADER") {
			return reply
		}
	}

	logs.Warnf("Request to leader %d failed or was rejected. Broadcasting to all servers.", leaderID)
	return broadcastRequest(req)
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
			if reply, err := client.Request(ctx, req); err == nil && reply != nil && !strings.Contains(reply.Error, "NOT_LEADER") {
				select {
				case replyChan <- reply:
				default:
				}
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
