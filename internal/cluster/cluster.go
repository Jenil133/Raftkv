// Package cluster builds in-process Raft+KV clusters over memnet for tests.
package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/storage"
	"github.com/Jenil133/raftkv/transport/memnet"
)

// Member is one node of the cluster. Raft and KV are nil while it is crashed.
type Member struct {
	ID      raft.NodeID
	Storage raft.Storage
	Raft    *raft.Node
	KV      *kv.Server
}

// Cluster is a set of members sharing a simulated network.
type Cluster struct {
	t   testing.TB
	Net *memnet.Network
	IDs []raft.NodeID

	mu      sync.Mutex
	members map[raft.NodeID]*Member
	cfg     func(*raft.Config)
}

// Option customises the cluster.
type Option func(*Cluster)

// WithRaftConfig lets tests tweak each node's raft.Config.
func WithRaftConfig(f func(*raft.Config)) Option {
	return func(c *Cluster) { c.cfg = f }
}

// New starts an n-node cluster and stops it when the test ends.
func New(t testing.TB, n int, opts ...Option) *Cluster {
	t.Helper()
	c := &Cluster{
		t:       t,
		Net:     memnet.New(time.Now().UnixNano()),
		members: make(map[raft.NodeID]*Member),
	}
	for _, o := range opts {
		o(c)
	}
	for i := 1; i <= n; i++ {
		c.IDs = append(c.IDs, raft.NodeID(i))
	}
	for _, id := range c.IDs {
		c.members[id] = &Member{ID: id, Storage: storage.NewMemory()}
		c.startMember(id)
	}
	t.Cleanup(c.Shutdown)
	return c
}

func (c *Cluster) startMember(id raft.NodeID) {
	m := c.members[id]
	cfg := raft.Config{
		ID:                 id,
		Peers:              c.IDs,
		Transport:          c.Net.Endpoint(id),
		Storage:            m.Storage,
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
	}
	if c.cfg != nil {
		c.cfg(&cfg)
	}
	node, err := raft.NewNode(cfg)
	if err != nil {
		c.t.Fatalf("new node %d: %v", id, err)
	}
	m.Raft = node
	m.KV = kv.NewServer(node)
	c.Net.Register(id, node)
	node.Start()
}

// Crash stops a node but keeps its storage, like a process kill.
func (c *Cluster) Crash(id raft.NodeID) {
	c.mu.Lock()
	m := c.members[id]
	node, srv := m.Raft, m.KV
	m.Raft, m.KV = nil, nil
	c.mu.Unlock()
	if node == nil {
		return
	}
	c.Net.Unregister(id)
	node.Stop()
	srv.Wait()
}

// Restart boots a crashed node from its storage.
func (c *Cluster) Restart(id raft.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[id].Raft != nil {
		return
	}
	c.startMember(id)
}

// Shutdown stops every node.
func (c *Cluster) Shutdown() {
	for _, id := range c.IDs {
		c.Crash(id)
	}
}

// Member returns the member record (fields may be nil if crashed).
func (c *Cluster) Member(id raft.NodeID) Member {
	c.mu.Lock()
	defer c.mu.Unlock()
	return *c.members[id]
}

// Up reports whether id is running.
func (c *Cluster) Up(id raft.NodeID) bool { return c.Member(id).Raft != nil }

// Leaders returns running nodes that currently believe they lead.
func (c *Cluster) Leaders() []raft.NodeID {
	var out []raft.NodeID
	for _, id := range c.IDs {
		if m := c.Member(id); m.Raft != nil && m.Raft.Status().Role == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// WaitLeader waits for exactly one leader among nodes (all running nodes if
// none given) and returns it.
func (c *Cluster) WaitLeader(timeout time.Duration, among ...raft.NodeID) raft.NodeID {
	c.t.Helper()
	if len(among) == 0 {
		among = c.IDs
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []raft.NodeID
		for _, id := range among {
			if m := c.Member(id); m.Raft != nil && m.Raft.Status().Role == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		// Stale leaders in older terms can briefly coexist; require one in the
		// highest term.
		if best := highestTermLeader(c, leaders); best != 0 {
			return best
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %v", timeout)
	return 0
}

func highestTermLeader(c *Cluster, leaders []raft.NodeID) raft.NodeID {
	if len(leaders) == 0 {
		return 0
	}
	var best raft.NodeID
	var bestTerm uint64
	count := 0
	for _, id := range leaders {
		st := c.Member(id).Raft.Status()
		switch {
		case st.Term > bestTerm:
			best, bestTerm, count = id, st.Term, 1
		case st.Term == bestTerm:
			count++
		}
	}
	if count != 1 {
		return 0
	}
	return best
}

// Eventually polls cond until it holds or the timeout expires.
func (c *Cluster) Eventually(timeout time.Duration, msg string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for: %s", msg)
}

// Client returns a KV client that talks to the nodes directly (no network
// faults between client and node), tracking crashes.
func (c *Cluster) Client() *kv.Client {
	eps := make(map[raft.NodeID]kvpb.KVClient, len(c.IDs))
	for _, id := range c.IDs {
		eps[id] = &endpoint{c: c, id: id}
	}
	cl := kv.NewClient(eps)
	cl.AttemptTimeout = time.Second
	return cl
}

type endpoint struct {
	c  *Cluster
	id raft.NodeID
}

func (e *endpoint) srv() (*kv.Server, error) {
	if m := e.c.Member(e.id); m.KV != nil {
		return m.KV, nil
	}
	return nil, fmt.Errorf("node %d is down", e.id)
}

func (e *endpoint) Put(ctx context.Context, in *kvpb.PutRequest, _ ...grpc.CallOption) (*kvpb.PutResponse, error) {
	s, err := e.srv()
	if err != nil {
		return nil, err
	}
	return s.Put(ctx, in)
}

func (e *endpoint) Get(ctx context.Context, in *kvpb.GetRequest, _ ...grpc.CallOption) (*kvpb.GetResponse, error) {
	s, err := e.srv()
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, in)
}

func (e *endpoint) Delete(ctx context.Context, in *kvpb.DeleteRequest, _ ...grpc.CallOption) (*kvpb.DeleteResponse, error) {
	s, err := e.srv()
	if err != nil {
		return nil, err
	}
	return s.Delete(ctx, in)
}

func (e *endpoint) CAS(ctx context.Context, in *kvpb.CASRequest, _ ...grpc.CallOption) (*kvpb.CASResponse, error) {
	s, err := e.srv()
	if err != nil {
		return nil, err
	}
	return s.CAS(ctx, in)
}
