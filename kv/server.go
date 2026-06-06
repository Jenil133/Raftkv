package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	observe       Observer
	snapshotEvery uint64 // take a snapshot after this many applied entries; 0 = never
	lastSnapshot  uint64 // index of the newest snapshot (apply loop only)
	snapshotting  atomic.Bool

	mu        sync.Mutex
	waiters   map[uint64]*waiter
	applied   uint64
	appliedCh chan struct{} // closed and replaced whenever applied advances
	done      chan struct{}
}

// Option configures a Server.
type Option func(*Server)

// Observer is told the outcome and latency of every request, e.g. to export
// metrics. op is one of put, get, delete, cas, scan.
type Observer func(op string, status kvpb.Status, d time.Duration)

// WithObserver installs an Observer.
func WithObserver(o Observer) Option { return func(s *Server) { s.observe = o } }

// WithSnapshotEvery makes the server snapshot its state (letting Raft discard
// the covered log prefix) every n applied entries.
func WithSnapshotEvery(n uint64) Option { return func(s *Server) { s.snapshotEvery = n } }

// NewServer starts consuming committed entries from node.
func NewServer(node *raft.Node, opts ...Option) *Server {
	s := &Server{
		node:           node,
		store:          NewStore(),
		RequestTimeout: 3 * time.Second,
		waiters:        make(map[uint64]*waiter),
		appliedCh:      make(chan struct{}),
		done:           make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	go s.applyLoop()
	go s.watchLeadership()
	return s
}

// Store exposes the underlying state machine (read-only use in tests).
func (s *Server) Store() *Store { return s.store }

// Wait blocks until the apply loop exits, which happens after the node stops.
func (s *Server) Wait() { <-s.done }

func (s *Server) applyLoop() {
	defer close(s.done)
	for ap := range s.node.Applied() {
		if ap.Snapshot != nil {
			s.applySnapshot(ap.Snapshot)
			continue
		}
		e := ap.Entry
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
		s.advanceApplied(e.Index)
		s.mu.Unlock()

		if s.snapshotEvery > 0 && e.Index-s.lastSnapshot >= s.snapshotEvery && !s.snapshotting.Load() {
			s.startSnapshot(e.Index)
		}
	}
	// Node stopped: unblock anyone still waiting.
	s.mu.Lock()
	for idx, w := range s.waiters {
		w.ch <- applyResult{err: raft.ErrStopped}
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

// advanceApplied records progress and wakes waitApplied callers. mu held.
func (s *Server) advanceApplied(idx uint64) {
	s.applied = idx
	close(s.appliedCh)
	s.appliedCh = make(chan struct{})
}

// startSnapshot copies the state machine as of index (cheap: values are
// immutable) and encodes and persists it in the background, so applying new
// entries never waits on serialisation or disk.
func (s *Server) startSnapshot(index uint64) {
	img := s.store.image()
	s.lastSnapshot = index
	s.snapshotting.Store(true)
	go func() {
		defer s.snapshotting.Store(false)
		data, err := img.encode()
		if err != nil {
			panic(fmt.Sprintf("kv: snapshot failed: %v", err))
		}
		if err := s.node.Snapshot(index, data); err != nil && !errors.Is(err, raft.ErrStopped) {
			panic(fmt.Sprintf("kv: raft snapshot at %d failed: %v", index, err))
		}
	}()
}

// applySnapshot replaces the state machine with a snapshot delivered by Raft,
// either at startup or after the leader sent one to a lagging follower.
func (s *Server) applySnapshot(snap *raft.Snapshot) {
	if err := s.store.Restore(snap.Data); err != nil {
		panic(fmt.Sprintf("kv: corrupt snapshot at index %d: %v", snap.Index, err))
	}
	s.mu.Lock()
	// Anything waiting on an index the snapshot swallowed has an unknown fate
	// from this node's view; make the client retry (dedupe keeps it safe).
	for idx, w := range s.waiters {
		if idx <= snap.Index {
			delete(s.waiters, idx)
			w.ch <- applyResult{err: errLostLeadership}
		}
	}
	s.advanceApplied(snap.Index)
	s.mu.Unlock()
	s.lastSnapshot = snap.Index
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

	select {
	case r := <-w.ch:
		if errors.Is(r.err, errLostLeadership) {
			return Result{}, s.notLeader()
		}
		return r.res, r.err
	case <-ctx.Done():
		s.dropWaiter(idx, w)
		return Result{}, ctx.Err()
	}
}

// watchLeadership fails pending writes once this node stops leading the term
// they were proposed in. Their entries may or may not commit; the client
// retries with the same sequence number, which dedupe makes safe.
func (s *Server) watchLeadership() {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		st := s.node.Status()
		s.mu.Lock()
		for idx, w := range s.waiters {
			if st.Role != raft.Leader || st.Term != w.term {
				delete(s.waiters, idx)
				w.ch <- applyResult{err: errLostLeadership}
			}
		}
		s.mu.Unlock()
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

func (s *Server) record(op string, st kvpb.Status, start time.Time) {
	if s.observe != nil {
		s.observe(op, st, time.Since(start))
	}
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

// Scan returns a linearizable view of this group's keys under a prefix.
func (s *Server) Scan(ctx context.Context, req *kvpb.ScanRequest) (*kvpb.ScanResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	start := time.Now()
	idx, err := s.node.ReadIndex(ctx)
	if err == nil {
		err = s.waitApplied(ctx, idx)
	} else if errors.Is(err, raft.ErrNotLeader) {
		err = s.notLeader()
	}
	st, hint, msg := classify(err)
	s.record("scan", st, start)
	resp := &kvpb.ScanResponse{Status: st, LeaderHint: hint, Error: msg}
	if err == nil {
		resp.Pairs = s.store.Scan(req.Prefix, req.After, int(req.Limit))
	}
	return resp, nil
}

func (s *Server) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	start := time.Now()
	_, err := s.execute(ctx, &kvpb.Command{
		Op: kvpb.Op_PUT, Key: req.Key, Value: req.Value, ClientId: req.ClientId, Seq: req.Seq,
	})
	st, hint, msg := classify(err)
	s.record("put", st, start)
	return &kvpb.PutResponse{Status: st, LeaderHint: hint, Error: msg}, nil
}

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	start := time.Now()
	v, found, err := s.read(ctx, req.Key)
	st, hint, msg := classify(err)
	s.record("get", st, start)
	return &kvpb.GetResponse{Status: st, LeaderHint: hint, Error: msg, Found: found, Value: v}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := s.execute(ctx, &kvpb.Command{
		Op: kvpb.Op_DELETE, Key: req.Key, ClientId: req.ClientId, Seq: req.Seq,
	})
	st, hint, msg := classify(err)
	s.record("delete", st, start)
	return &kvpb.DeleteResponse{Status: st, LeaderHint: hint, Error: msg, Existed: res.Found}, nil
}

func (s *Server) CAS(ctx context.Context, req *kvpb.CASRequest) (*kvpb.CASResponse, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := s.execute(ctx, &kvpb.Command{
		Op: kvpb.Op_CAS, Key: req.Key, Value: req.Value, Expected: req.Expected,
		ExpectAbsent: req.ExpectAbsent, DeleteOnMatch: req.DeleteOnMatch,
		ClientId: req.ClientId, Seq: req.Seq,
	})
	st, hint, msg := classify(err)
	s.record("cas", st, start)
	return &kvpb.CASResponse{
		Status: st, LeaderHint: hint, Error: msg,
		Swapped: res.Swapped, Found: res.Found, Current: res.Value,
	}, nil
}
