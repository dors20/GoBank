package rpcin

import (
	"context"

	"2PC/server/api"
	"2PC/server/store"
)

type Server struct {
	api.UnimplementedKVServer

	store *store.Store
}

func New(store *store.Store) *Server {
	return &Server{
		store: store,
	}
}

func (s *Server) ReadOnlyRequest(ctx context.Context, req *api.ReadOnlyPayload) (*api.ReadOnlyResponse, error) {
	value, err := s.store.Get(ctx, []byte(req.Key))
	if err != nil {
		if err == store.ErrKeyNotFound {
			return &api.ReadOnlyResponse{
				Found: false,
			}, nil
		}

		return &api.ReadOnlyResponse{
			Error: err.Error(),
		}, nil
	}

	return &api.ReadOnlyResponse{
		Value: value,
		Found: true,
	}, nil
}

func (s *Server) IntraShardRWReq(ctx context.Context, req *api.IntraShardRWPayload) (*api.IntraShardRWResp, error) {
	if req.Delete {
		err := s.store.Delete(ctx, []byte(req.Key))
		if err != nil {
			if err == store.ErrKeyNotFound {
				return &api.IntraShardRWResp{
					Found: false,
				}, nil
			}

			return &api.IntraShardRWResp{
				Error: err.Error(),
			}, nil
		}

		return &api.IntraShardRWResp{
			Found: true,
		}, nil
	}

	err := s.store.Put(ctx, []byte(req.Key), req.Value)
	if err != nil {
		return &api.IntraShardRWResp{
			Error: err.Error(),
		}, nil
	}

	return &api.IntraShardRWResp{
		Found: true,
	}, nil
}

func (s *Server) InterShardRWReq(ctx context.Context, req *api.InterShardRWPayload) (*api.InterShardRWResp, error) {
	if req.Delete {
		err := s.store.Delete(ctx, []byte(req.Key))
		if err != nil {
			if err == store.ErrKeyNotFound {
				return &api.InterShardRWResp{
					Found: false,
				}, nil
			}

			return &api.InterShardRWResp{
				Error: err.Error(),
			}, nil
		}

		return &api.InterShardRWResp{
			Found: true,
		}, nil
	}

	err := s.store.Put(ctx, []byte(req.Key), req.Value)
	if err != nil {
		return &api.InterShardRWResp{
			Error: err.Error(),
		}, nil
	}

	return &api.InterShardRWResp{
		Found: true,
	}, nil
}
