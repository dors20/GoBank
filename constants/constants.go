package constants

import "go.uber.org/zap"

const (
	Follower = iota
	Candidate
	Leader
	Failed
)

// Define other constants like timeouts here
// TODO

// SYSTEM CONFIG
const MAX_CLIENTS = 10
const MAX_NODES = 3
const NUM_CLUSTERS = 3

// NETWORK CONFIG
const LEADER_TIMEOUT_SECONDS = 200
const REQUEST_TIMEOUT = 500
const FORWARD_TIMEOUT = 250
const BASE_PORT = "9100"
const PREPARE_TIMEOUT = 100
const HEARTBEAT = 50

// STATE MACHINE CONFIG
const INITIAL_BALANCE = 10 // TODO spec mentions 10, using 100 for now easier to analyze logs
const NOOP = "no-op"

// LOGGER
const LOG_LEVEL = zap.InfoLevel

// Can do base_Port+1
// Port 9101 - 9110 reserved if we need multiple client instances
// Static ports and IP based on the assumption syaing all nodes are aware of all other clients and nodes
var ServerPorts = map[int]string{
	1: "9111",
	2: "9112",
	3: "9113",
	4: "9114",
	5: "9115",
	6: "9116",
	7: "9117",
	8: "9118",
	9: "9119",
}

func ClusterOf(id int) int {
	if id <= 0 {
		return -1
	}
	return (id - 1) / MAX_NODES
}
