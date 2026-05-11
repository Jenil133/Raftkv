package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
)

// NotLeaderError means the request must go to another node. Hint is the node
// this one believes is leader, or zero if unknown.
type NotLeaderError struct{ Hint raft.NodeID }

func (e *NotLeaderError) Error() string {
	if e.Hint == 0 {
		return "not leader (leader unknown)"
	}
	return fmt.Sprintf("not leader (try node %d)", e.Hint)
}

// errLostLeadership means the proposed entry was displaced by another leader.
var errLostLeadership = errors.New("kv: entry superseded after leadership change")

type applyResult struct {
	res Result
	err error
}

type waiter struct {
	term uint64
	ch   chan applyResult
}

// Server exposes the replicated store through a Raft node. It implements
// kvpb.KVServer.
type Server struct {
	kvpb.UnimplementedKVServer

	node  *raft.Node
	store *Store
	// RequestTimeout caps how long one request may wait on consensus.
	RequestTimeout time.Duration

	mu        sync.Mutex
	waiters   map[uint64]*waiter
	applied   uint64
	appliedCh chan struct{} // closed and replaced whenever applied advances
	done      chan struct{}
}

// NewServer starts consuming committed entries from node.
func NewServer(node *raft.Node) *Server {
	s := &Server{
		node:           node,
		store:          NewStore(),
		RequestTimeout: 3 * time.Second,
		waiters:        make(map[uint64]*waiter),
		appliedCh:      make(chan struct{}),
		done:           make(chan struct{}),
	}
	go s.applyLoop()
	return s
}

// Store exposes the underlying state machine (read-only use in tests).
func (s *Server) Store() *Store { return s.store }

// Wait blocks until the apply loop exits, which happens after the node stops.
func (s *Server) Wait() { <-s.done }

func (s *Server) applyLoop() {
	defer close(s.done)
	for e := range s.node.Applied() {
		var res Result
		if e.Type == raft.EntryNormal {
			var cmd kvpb.Command
			if err := proto.Unmarshal(e.Data, &cmd); err != nil {
				panic(fmt.Sprintf("kv: corrupt committed command at index %d: %v", e.Index, err))
			}
			res = s.store.Apply(&cmd)
		}
		s.mu.Lock()
		if w, ok := s.waiters[e.Index]; ok {
			delete(s.waiters, e.Index)
			if w.term == e.Term {
				w.ch <- applyResult{res: res}
			} else {
				w.ch <- applyResult{err: errLostLeadership}
			}
		}
		s.applied = e.Index
		close(s.appliedCh)
		s.appliedCh = make(chan struct{})
		s.mu.Unlock()
	}
	// Node stopped: unblock anyone still waiting.
	s.mu.Lock()
	for idx, w := range s.waiters {
		w.ch <- applyResult{err: raft.ErrStopped}
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

func (s *Server) notLeader() error {
	return &NotLeaderError{Hint: s.node.Status().Leader}
}

// execute proposes cmd and waits until it is applied.
func (s *Server) execute(ctx context.Context, cmd *kvpb.Command) (Result, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return Result{}, err
	}
	// Holding mu across Propose guarantees the waiter is registered before the
	// apply loop can see the entry, even on a single-node cluster.
	s.mu.Lock()
	idx, term, ok := s.node.Propose(data)
	if !ok {
		s.mu.Unlock()
		return Result{}, s.notLeader()
	}
	w := &waiter{term: term, ch: make(chan applyResult, 1)}
	s.waiters[idx] = w
	s.mu.Unlock()

	t := time.NewTicker(25 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case r := <-w.ch:
			if errors.Is(r.err, errLostLeadership) {
				return Result{}, s.notLeader()
			}
			return r.res, r.err
		case <-ctx.Done():
			s.dropWaiter(idx, w)
			return Result{}, ctx.Err()
		case <-t.C:
			if st := s.node.Status(); st.Term != term || st.Role != raft.Leader {
				s.dropWaiter(idx, w)
				return Result{}, s.notLeader()
			}
		}
	}
}

func (s *Server) dropWaiter(idx uint64, w *waiter) {
	s.mu.Lock()
	if s.waiters[idx] == w {
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

func (s *Server) waitApplied(ctx context.Context, idx uint64) error {
	for {
		s.mu.Lock()
		if s.applied >= idx {
			s.mu.Unlock()
			return nil
		}
		ch := s.appliedCh
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// read serves a linearizable read: confirm leadership, wait for the state
// machine to catch up to the read index, then read locally.
func (s *Server) read(ctx context.Context, key string) ([]byte, bool, error) {
	idx, err := s.node.ReadIndex(ctx)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, false, s.notLeader()
		}
		return nil, false, err
	}
	if err := s.waitApplied(ctx, idx); err != nil {
		return nil, false, err
	}
	v, ok := s.store.Get(key)
	return v, ok, nil
}

func (s *Server) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.RequestTimeout)
}

// classify turns an error into a response status and leader hint.
func classify(err error) (kvpb.Status, uint64, string) {
	if err == nil {
		return kvpb.Status_OK, 0, ""
	}
	var nl *NotLeaderError
	switch {
	case errors.As(err, &nl):
		return kvpb.Status_NOT_LEADER, uint64(nl.Hint), ""
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return kvpb.Status_TIMEOUT, 0, err.Error()
	default:
		return kvpb.Status_ERROR, 0, err.Error()
	}
}

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	_, err := s.execute(ctx, &kvpb.Command{
		Op: kvpb.Op_PUT, Key: req.Key, Value: req.Value, ClientId: req.ClientId, Seq: req.Seq,
	})
	st, hint, msg := classify(err)
	return &kvpb.PutResponse{Status: st, LeaderHint: hint, Error: msg}, nil
}

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	v, found, err := s.read(ctx, req.Key)
	st, hint, msg := classify(err)
	return &kvpb.GetResponse{Status: st, LeaderHint: hint, Error: msg, Found: found, Value: v}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	res, err := s.execute(ctx, &kvpb.Command{
		Op: kvpb.Op_DELETE, Key: req.Key, ClientId: req.ClientId, Seq: req.Seq,
	})
	st, hint, msg := classify(err)
	return &kvpb.DeleteResponse{Status: st, LeaderHint: hint, Error: msg, Existed: res.Found}, nil
}

func (s *Server) CAS(ctx context.Context, req *kvpb.CASRequest) (*kvpb.CASResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	res, err := s.execute(ctx, &kvpb.Command{
		Op: kvpb.Op_CAS, Key: req.Key, Value: req.Value, Expected: req.Expected,
		ExpectAbsent: req.ExpectAbsent, ClientId: req.ClientId, Seq: req.Seq,
	})
	st, hint, msg := classify(err)
	return &kvpb.CASResponse{
		Status: st, LeaderHint: hint, Error: msg,
		Swapped: res.Swapped, Found: res.Found, Current: res.Value,
	}, nil
}
