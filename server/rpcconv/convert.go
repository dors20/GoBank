package rpcconv

import (
	"strconv"

	"2PC/server/api"
	"2PC/server/paxos"
)

func ProtoToServerID(id int32) paxos.ServerID {
	return paxos.ServerID(strconv.FormatInt(int64(id), 10))
}

func ServerIDToProto(id paxos.ServerID) int32 {
	val, err := strconv.ParseInt(string(id), 10, 32)
	if err != nil {
		return 0
	}
	return int32(val)
}

func ProtoToBallot(val int32, serverID int32) paxos.Ballot {
	return paxos.Ballot{
		Val:      uint64(val),
		ServerID: ProtoToServerID(serverID),
	}
}

func BallotToProto(b paxos.Ballot) (int32, int32) {
	return int32(b.Val), ServerIDToProto(b.ServerID)
}

func ProtoToTransaction(msg *api.Message) paxos.Transaction {
	return paxos.Transaction{
		Sender:   paxos.ClientID(msg.Sender),
		Receiver: paxos.ClientID(msg.Receiver),
		Amount:   int64(msg.Amount),
	}
}

func TransactionToMessage(tx paxos.Transaction, timestamp int64, clientID string) *api.Message {
	return &api.Message{
		Sender:    int32(tx.Sender),
		Receiver:  int32(tx.Receiver),
		Amount:    int32(tx.Amount),
		Timestamp: timestamp,
		ClientId:  clientID,
	}
}

func ProtoToLogEntry(rec *api.LogRecord) paxos.LogEntry {
	if rec == nil {
		return paxos.LogEntry{}
	}

	return paxos.LogEntry{
		SeqNum: paxos.SeqNum(rec.SeqNum),
		Ballot: ProtoToBallot(rec.BallotVal, rec.ServerId),
		Tx: paxos.Transaction{
			Sender:   paxos.ClientID(rec.Sender),
			Receiver: paxos.ClientID(rec.Receiver),
			Amount:   int64(rec.Amount),
		},
		Committed: rec.IsCommitted,
	}
}

func LogEntryToProto(entry paxos.LogEntry) *api.LogRecord {
	bVal, bServer := BallotToProto(entry.Ballot)

	return &api.LogRecord{
		SeqNum:      int32(entry.SeqNum),
		BallotVal:   bVal,
		ServerId:    bServer,
		Sender:      int32(entry.Tx.Sender),
		Receiver:    int32(entry.Tx.Receiver),
		Amount:      int32(entry.Tx.Amount),
		IsCommitted: entry.Committed,
	}
}
