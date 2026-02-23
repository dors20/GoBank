# GoBank

A fault-tolerant distributed banking system, implementing consensus with Multi-Paxos and cross-shard transactions with Two-Phase Commit (2PC).

## Project Description

GoBank is a distributed banking application where clients submit **balance** (read-only) and **transfer** (read-write) transactions. The system uses **state machine replication** so all replicas stay consistent. It is built in two phases:

- ** A single-cluster banking system over **Multi-Paxos** (Stable-Leader Paxos). One leader per cluster orders transfer transactions via prepare/promise, accept/accepted, and commit; leader election handles failures with new-view and catch-up.
- A **sharded**, multi-cluster system. Data is partitioned into shards (e.g. C1: 1–3000, C2: 3001–6000, C3: 6001–9000). **Intra-shard** transfers use Multi-Paxos within one cluster; **cross-shard** transfers use **Two-Phase Commit** between the coordinator cluster (sender’s shard) and the participant cluster (receiver’s shard), with prepare/prepared and commit/abort, write-ahead logging (WAL), and lock management.


## Architecture

- **Nodes:** 9 nodes (n1–n9) in 3 clusters of 3. Each cluster replicates one data shard. Nodes are separate processes; no shared memory. Communication via **gRPC** (see `api/api.proto`).
- **Data:** 9000 accounts (ids 1–9000), range-partitioned by cluster. Initial balance 10 per account. Storage is a key-value store (Badger).
- **Client:** Single client process reads CSV test files (Set Number, Transactions, Live Nodes), sends requests to cluster leaders (n1, n4, n7 for C1, C2, C3), handles retries and timeouts. Supports balance queries, transfers, and special commands `F(ni)` / `R(ni)` for fail/recover.
- **Consensus:** Multi-Paxos per cluster (prepare, promise, accept, accepted, commit, new-view). Cross-shard 2PC: coordinator prepares and runs Paxos with phase `P`, participant prepares and runs Paxos; then coordinator runs commit phase `C` or abort `A`, participant replicates and acknowledges; WAL used for undo on abort/timeout.
- **Code layout:**
  - `api/` — Protobuf and gRPC service definitions (`ClientServerTxns`, `PaxosReplication`, `PaxosPrintInfo`, 2PC RPCs).
  - `server/` — Node process: Paxos state, log store, state machine (vault, locks, WAL), 2PC coordinator/participant logic, failure simulation.
  - `client/` — Test manager: CSV parsing, test sets, benchmark runner, CLI (next, skip, printbalance, printdb, printview, performance, printreshard, benchmark, exit).
  - `constants/` — Cluster/shard mapping, ports, timeouts, `ClusterOfServer`, `ClusterForAccountID`.

## Advanced Features Implemented

- **Benchmarking:** Configurable workload generator (e.g. `benchmark_gen.py`) and client command `benchmark` with parameters for read-only vs read-write ratio, intra-shard vs cross-shard ratio, and data distribution (uniform/skewed). Output written to CSV (e.g. `client/benchmark.csv`).
- **Shard redistribution (Resharding):** `computeReshardMapping` uses transaction history (e.g. cross-shard pairs) to compute a new account-to-cluster mapping. `printreshard` prints moved records as triplets (account, source cluster, dest cluster). Mapping persisted in `reshard_mapping.json` and loaded by server and client.
- **Performance metrics:** Throughput (transactions per second) and latency (avg/max) from client send to reply. Exposed via `performance` CLI and after each benchmark run.

## Test Coverage

Robust test suites are provided in CSV format:

- **Testcases1.csv** —  (single-cluster Multi-Paxos): 10 sets with various live node configurations. Covers leader election on startup, intra-cluster transfers, balance reads, backup and leader failures (`F`/`R`), multi-step sequences, and stress (e.g. Set 10 with many transfers). Format: Set Number, Transactions `(sender, receiver, amount)` or `(account)` for balance, Live Nodes `[n1, n2, ...]`.
- **Testcases2.csv** — (multi-cluster + 2PC): 10 sets with 9 nodes and 3 clusters. Covers intra-shard and cross-shard transfers, balance checks across shards, node failures and recoveries during both intra- and cross-shard flows, concurrent transactions on same/different shards, and large workloads (e.g. Set 10 with hundreds of transfers across all three shards). Same column format with extended transaction IDs (e.g. 1001–9000) and live-node sets per set.

Each set is run independently with a full system flush between sets; the client prompts before processing the next set so that `PrintBalance`, `PrintDB`, `PrintView`, and `Performance` can be used after each set.

## Build and Run

Generate protobuf/grpc code:

```bash
protoc --go_out=. --go-grpc_out=. api/api.proto
```

Run servers (one process per node, or as configured) and the client with the desired test CSV (e.g. Project 1 or Project 3 testcases). From the client CLI you can run sets (next/skip), print balance/DB/view, run benchmarks, print performance, and trigger reshard computation (printreshard).

## Benchmark Results
- **Single Shard** - 30000 TPS
- **30 % Reads and 10% Cross Shard** - 9000 TPS
