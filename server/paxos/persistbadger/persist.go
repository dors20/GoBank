package persistbadger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"2PC/server/paxos"

	"github.com/dgraph-io/badger/v4"
)

type Config struct {
	Path string
}

type Store struct {
	db *badger.DB
}

var (
	metaKey   = []byte("meta")
	logPrefix = []byte("log/")
)

type metaRecord struct {
	LastBallot   ballotRecord `json:"last_ballot"`
	PromisedBall ballotRecord `json:"promised_ballot"`
	CommitIndex  uint64       `json:"commit_index"`
}

type ballotRecord struct {
	Val uint64 `json:"val"`
	ID  string `json:"id"`
}

type logRecord struct {
	Seq       uint64       `json:"seq"`
	Ballot    ballotRecord `json:"ballot"`
	Sender    int64        `json:"sender"`
	Receiver  int64        `json:"receiver"`
	Amount    int64        `json:"amount"`
	Committed bool         `json:"committed"`
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(cfg.Path).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open paxos persistence: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Load(ctx context.Context) (paxos.PersistentState, error) {
	state := paxos.PersistentState{
		LastBallot:   paxos.Ballot{Val: 1},
		PromisedBall: paxos.Ballot{Val: 1},
	}

	meta, err := s.readMeta(ctx)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return state, err
	}
	if err == nil {
		state.LastBallot = meta.LastBallot.toBallot()
		state.PromisedBall = meta.PromisedBall.toBallot()
		state.CommitIndex = paxos.SeqNum(meta.CommitIndex)
	}

	err = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
		})
		defer it.Close()

		for it.Seek(logPrefix); it.ValidForPrefix(logPrefix); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var rec logRecord
			if err := json.Unmarshal(val, &rec); err != nil {
				return err
			}
			state.LogEntries = append(state.LogEntries, rec.toLogEntry())
		}
		return nil
	})
	if err != nil {
		return state, fmt.Errorf("load log: %w", err)
	}

	return state, nil
}

func (s *Store) SaveBallots(ctx context.Context, last paxos.Ballot, promised paxos.Ballot) error {
	return s.updateMeta(ctx, func(meta *metaRecord) {
		meta.LastBallot = fromBallot(last)
		meta.PromisedBall = fromBallot(promised)
	})
}

func (s *Store) SaveCommitIndex(ctx context.Context, idx paxos.SeqNum) error {
	return s.updateMeta(ctx, func(meta *metaRecord) {
		meta.CommitIndex = uint64(idx)
	})
}

func (s *Store) SaveLogEntry(ctx context.Context, entry paxos.LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(toLogRecord(entry))
	if err != nil {
		return fmt.Errorf("encode log entry: %w", err)
	}
	key := logKey(uint64(entry.SeqNum))
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

func (s *Store) updateMeta(ctx context.Context, mutate func(*metaRecord)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		meta, err := s.readMetaTxn(txn)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if meta == nil {
			meta = &metaRecord{}
		}
		mutate(meta)
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return txn.Set(metaKey, data)
	})
}

func (s *Store) readMeta(ctx context.Context) (*metaRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var meta *metaRecord
	err := s.db.View(func(txn *badger.Txn) error {
		rec, err := s.readMetaTxn(txn)
		if err != nil {
			return err
		}
		meta = rec
		return nil
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Store) readMetaTxn(txn *badger.Txn) (*metaRecord, error) {
	item, err := txn.Get(metaKey)
	if err != nil {
		return nil, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	var meta metaRecord
	if err := json.Unmarshal(val, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func fromBallot(b paxos.Ballot) ballotRecord {
	return ballotRecord{
		Val: b.Val,
		ID:  string(b.ServerID),
	}
}

func (b ballotRecord) toBallot() paxos.Ballot {
	return paxos.Ballot{
		Val:      b.Val,
		ServerID: paxos.ServerID(b.ID),
	}
}

func toLogRecord(entry paxos.LogEntry) logRecord {
	return logRecord{
		Seq:       uint64(entry.SeqNum),
		Ballot:    fromBallot(entry.Ballot),
		Sender:    int64(entry.Tx.Sender),
		Receiver:  int64(entry.Tx.Receiver),
		Amount:    entry.Tx.Amount,
		Committed: entry.Committed,
	}
}

func (r logRecord) toLogEntry() paxos.LogEntry {
	return paxos.LogEntry{
		SeqNum:    paxos.SeqNum(r.Seq),
		Ballot:    r.Ballot.toBallot(),
		Tx:        paxos.Transaction{Sender: paxos.ClientID(r.Sender), Receiver: paxos.ClientID(r.Receiver), Amount: r.Amount},
		Committed: r.Committed,
	}
}

func logKey(seq uint64) []byte {
	return append(logPrefix, []byte(fmt.Sprintf("%020d", seq))...)
}

var _ paxos.Persistence = (*Store)(nil)
