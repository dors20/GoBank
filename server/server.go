package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"2PC/constants"
	"2PC/server/api"
	"2PC/server/paxos"
	"2PC/server/paxos/persistbadger"
	"2PC/server/rpcin"
	"2PC/server/rpcout"
	"2PC/server/store"
	"2PC/util"

	"google.golang.org/grpc"
)

func main() {
	serverIDFlag := flag.String("server_id", constants.DefaultServerID, "numeric server identifier")
	peersFlag := flag.String("peers", "", "comma-separated list of id=host:port entries (including this server)")
	flag.Parse()

	selfID := paxos.ServerID(*serverIDFlag)

	peerAddrs, err := parsePeers(*peersFlag, selfID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid peers config: %v\n", err)
		os.Exit(1)
	}

	selfAddr, ok := peerAddrs[selfID]
	if !ok {
		fmt.Fprintf(os.Stderr, "peers config missing self id %s\n", string(selfID))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, err := util.NewLogger(util.LoggerConfig{
		Component: "server",
		ServerID:  string(selfID),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger init error: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		cancel()
	}()

	storeCtx, storeCancel := context.WithTimeout(ctx, 10*time.Second)
	defer storeCancel()

	dataPath := "data"

	kvStore, err := store.Open(storeCtx, store.Config{
		Path: dataPath,
	})
	if err != nil {
		logger.Error("store open failed", util.ErrorField(err))
		os.Exit(1)
	}
	defer kvStore.Close()
	applier := rpcin.NewTxnApplier(kvStore, logger)

	paxosPath := filepath.Join(dataPath, fmt.Sprintf("paxos_%s", string(selfID)))
	persistence, err := persistbadger.Open(ctx, persistbadger.Config{
		Path: paxosPath,
	})
	if err != nil {
		logger.Error("persistence init failed", util.ErrorField(err))
		os.Exit(1)
	}
	defer persistence.Close()

	transport, err := rpcout.NewGRPCTransport(selfID, peerAddrs, logger)
	if err != nil {
		logger.Error("transport init failed", util.ErrorField(err))
		os.Exit(1)
	}
	defer transport.Close()

	var peers []paxos.ServerID
	for id := range peerAddrs {
		if id == selfID {
			continue
		}
		peers = append(peers, id)
	}

	node, err := paxos.NewNode(paxos.Config{
		Self:        selfID,
		Peers:       peers,
		Transport:   transport,
		Logger:      logger,
		OnCommit:    applier.Apply,
		Persistence: persistence,
	})
	if err != nil {
		logger.Error("paxos init failed", util.ErrorField(err))
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", selfAddr)
	if err != nil {
		logger.Error("listen failed", util.ErrorField(err))
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	api.RegisterKVServer(grpcServer, rpcin.New(kvStore, logger))
	api.RegisterPaxosReplicationServer(grpcServer, rpcin.NewPaxosServer(node, selfID, kvStore, logger))
	api.RegisterClientServerTxnsServer(grpcServer, rpcin.NewClientTxnServer(node, selfID, transport, logger))
	api.RegisterPaxosPrintInfoServer(grpcServer, rpcin.NewPrintServer(node, kvStore, logger))

	go func() {
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			logger.Error("grpc serve failed", util.ErrorField(serveErr))
			cancel()
		}
	}()

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})

	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
}

func parsePeers(peers string, self paxos.ServerID) (map[paxos.ServerID]string, error) {
	result := make(map[paxos.ServerID]string)
	if peers == "" {
		result[self] = fmt.Sprintf(":%d", constants.BASE_SERVER_PORT)
		return result, nil
	}

	for _, part := range strings.Split(peers, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q", part)
		}

		id := paxos.ServerID(strings.TrimSpace(kv[0]))
		addr := strings.TrimSpace(kv[1])
		result[id] = addr
	}

	if _, ok := result[self]; !ok {
		result[self] = fmt.Sprintf(":%d", constants.BASE_SERVER_PORT)
	}

	return result, nil
}
