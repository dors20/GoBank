package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"2PC/constants"
	"2PC/server/api"
	"2PC/server/rpcin"
	"2PC/server/store"
	"2PC/util"

	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, err := util.NewLogger(util.LoggerConfig{
		Component: "server",
		ServerID:  constants.DefaultServerID,
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

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", constants.BASE_SERVER_PORT))
	if err != nil {
		logger.Error("listen failed", util.ErrorField(err))
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	api.RegisterKVServer(grpcServer, rpcin.New(kvStore))

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
