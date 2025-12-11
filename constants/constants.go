package constants

import (
	"sort"

	"go.uber.org/zap"
)

const (
	Follower = iota
	Candidate
	Leader
	Failed
)

// SYSTEM CONFIG
const MAX_CLIENTS = 9000
const MAX_NODES = 3
const NUM_CLUSTERS = 3
const MAX_INFLIGHT = 300

// NETWORK CONFIG
const LEADER_TIMEOUT_SECONDS = 200
const REQUEST_TIMEOUT = 500
const FORWARD_TIMEOUT = 250
const BASE_PORT = "9100"
const PREPARE_TIMEOUT = 50
const HEARTBEAT = 50

// STATE MACHINE CONFIG
const INITIAL_BALANCE = 10 // TODO spec mentions 10, using 100 for now easier to analyze logs
const NOOP = "no-op"

// LOGGER
const LOG_LEVEL = zap.WarnLevel

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

// cluster 0: servers 1,2,3
// cluster 1: servers 4,5,6
// cluster 2: servers 7,8,9
var ClusterServers = map[int][]int{
	0: {1, 2, 3},
	1: {4, 5, 6},
	2: {7, 8, 9},
}

// var ServerPorts = map[int]string{
// 	1:  "9111",
// 	2:  "9112",
// 	3:  "9113",
// 	4:  "9114",
// 	5:  "9115",
// 	6:  "9116",
// 	7:  "9117",
// 	8:  "9118",
// 	9:  "9119",
// 	10: "9120",
// 	11: "9121",
// 	12: "9122",
// 	13: "9123",
// 	14: "9124",
// 	15: "9125",
// 	16: "9126",
// 	17: "9127",
// 	18: "9128",
// 	19: "9129",
// 	20: "9130",
// 	21: "9131",
// 	22: "9132",
// 	23: "9133",
// 	24: "9134",
// 	25: "9135",
// 	26: "9136",
// }

// cluster 0: servers 1,2,3
// cluster 1: servers 4,5,6
// cluster 2: servers 7,8,9
// var ClusterServers = map[int][]int{
// 	0: {1, 2, 3, 4, 5},
// 	1: {6, 7, 8},
// 	2: {9, 10, 11, 12, 13, 14, 15},
// }

func NumClusters() int {
	return len(ClusterServers)
}

func ClusterIDs() []int {
	ids := make([]int, 0, len(ClusterServers))
	for id := range ClusterServers {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func ClusterOf(id int) int {
	return ClusterOfServer(id)
}

func ClusterOfServer(id int) int {
	if id <= 0 {
		return -1
	}
	for cid, servers := range ClusterServers {
		for _, sid := range servers {
			if sid == id {
				return cid
			}
		}
	}
	return -1
}

func ClusterSize(clusterID int) int {
	servers, ok := ClusterServers[clusterID]
	if !ok {
		return 0
	}
	return len(servers)
}

func ClusterLeader(clusterID int) int {
	servers, ok := ClusterServers[clusterID]
	if !ok || len(servers) == 0 {
		return -1
	}
	return servers[0]
}

func ClusterQuorum(clusterID int) int {
	size := ClusterSize(clusterID)
	if size == 0 {
		return 0
	}
	return size/2 + 1
}

func ClusterForAccountID(id int) int {
	if id <= 0 || id > MAX_CLIENTS {
		return -1
	}
	n := NumClusters()
	if n <= 0 {
		return -1
	}
	segment := MAX_CLIENTS / n
	if segment <= 0 {
		return -1
	}
	for cluster := 0; cluster < n-1; cluster++ {
		start := cluster*segment + 1
		end := (cluster + 1) * segment
		if id >= start && id <= end {
			return cluster
		}
	}
	return n - 1
}
