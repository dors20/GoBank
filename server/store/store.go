package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type Config struct {
	Path string
}

type Store struct {
	db *badger.DB
}

var ErrKeyNotFound = errors.New("key not found")

func Open(ctx context.Context, cfg Config) (*Store, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	opts := badger.DefaultOptions(cfg.Path)
	opts = opts.WithLogger(nil)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("store open: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var value []byte

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound
			}

			return err
		}

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		value = val

		return nil
	})

	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}

		return nil, fmt.Errorf("store get: %w", err)
	}

	return value, nil
}

func (s *Store) Put(ctx context.Context, key []byte, value []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})

	if err != nil {
		return fmt.Errorf("store put: %w", err)
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, key []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrKeyNotFound
		}

		return fmt.Errorf("store delete: %w", err)
	}

	return nil
}
