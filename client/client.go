package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"2PC/constants"
	"2PC/server/api"
	"2PC/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger, err := util.NewLogger(util.LoggerConfig{
		Component: "client",
		ServerID:  constants.DefaultServerID,
		LogPath:   "client/client_log",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger init error: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: client <command> [args]")
		os.Exit(1)
	}

	addr := fmt.Sprintf("localhost:%d", constants.BASE_SERVER_PORT)

	dialCtx, dialCancel := context.WithTimeout(
		context.Background(),
		time.Duration(constants.RequestTimeoutMillis)*time.Millisecond,
	)
	defer dialCancel()

	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := api.NewKVClient(conn)

	cmd := os.Args[1]

	switch cmd {
	case "ro":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: client ro <key>")
			os.Exit(1)
		}

		key := os.Args[2]

		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(constants.RequestTimeoutMillis)*time.Millisecond,
		)
		defer cancel()

		resp, err := client.ReadOnlyRequest(ctx, &api.ReadOnlyPayload{
			Key: key,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "rpc error: %v\n", err)
			os.Exit(1)
		}

		if !resp.Found {
			fmt.Println("not found")
			return
		}

		fmt.Println(string(resp.Value))
	case "intra":
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "usage: client intra <key> <value>")
			os.Exit(1)
		}

		key := os.Args[2]
		value := os.Args[3]

		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(constants.RequestTimeoutMillis)*time.Millisecond,
		)
		defer cancel()

		_, err := client.IntraShardRWReq(ctx, &api.IntraShardRWPayload{
			Key:   key,
			Value: []byte(value),
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "rpc error: %v\n", err)
			os.Exit(1)
		}
	case "inter":
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "usage: client inter <key> <value>")
			os.Exit(1)
		}

		key := os.Args[2]
		value := os.Args[3]

		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(constants.RequestTimeoutMillis)*time.Millisecond,
		)
		defer cancel()

		_, err := client.InterShardRWReq(ctx, &api.InterShardRWPayload{
			Key:   key,
			Value: []byte(value),
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "rpc error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown command")
		os.Exit(1)
	}
}
