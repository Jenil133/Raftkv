// Package cluster builds in-process Raft+KV clusters over memnet for tests.
package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/shard"
	"github.com/Jenil133/raftkv/storage"
	"github.com/Jenil133/raftkv/transport/memnet"
)

// Member is one node of the cluster. Its groups are nil while it is crashed.
// Raft and KV are shortcuts for shard 0.
type Member struct {
	ID       raft.NodeID
	Storages []raft.Storage // one per shard; survive crashes
	Groups   []shard.Group
	Host     *shard.Host
	Raft     *raft.Node
	KV       *kv.Server
}

// TB is the subset of testing.TB the cluster needs, so it can also run
// outside tests (see Runner).
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// Cluster is a set of members sharing a simulated network.
type Cluster struct {
	t   TB
	Net *memnet.Network
	IDs []raft.NodeID

	shards        int
	snapshotEvery uint64
	netSeed       int64
	cfg           func(*raft.Config)

	mu      sync.Mutex
	members map[raft.NodeID]*Member
}

// Option customises the cluster.
type Option func(*Cluster)

// WithRaftConfig lets tests tweak each node's raft.Config.
func WithRaftConfig(f func(*raft.Config)) Option {
	return func(c *Cluster) { c.cfg = f }
}

// WithShards runs n Raft groups per node.
func WithShards(n int) Option { return func(c *Cluster) { c.shards = n } }

// WithNetSeed seeds the simulated network's fault randomness.
func WithNetSeed(seed int64) Option { return func(c *Cluster) { c.netSeed = seed } }

// WithSnapshotEvery enables snapshots/compaction every n applied entries.
func WithSnapshotEvery(n uint64) Option { return func(c *Cluster) { c.snapshotEvery = n } }

// New starts an n-node cluster and stops it when the test ends.
func New(t TB, n int, opts ...Option) *Cluster {
	t.Helper()
	c := &Cluster{
		t:       t,
		shards:  1,
		netSeed: time.Now().UnixNano(),
		members: make(map[raft.NodeID]*Member),
	}
	for _, o := range opts {
		o(c)
	}
	c.Net = memnet.New(c.netSeed)
	for i := 1; i <= n; i++ {
		c.IDs = append(c.IDs, raft.NodeID(i))
	}
	for _, id := range c.IDs {
		m := &Member{ID: id}
		for g := 0; g < c.shards; g++ {
			m.Storages = append(m.Storages, storage.NewMemory())
		}
		c.members[id] = m
		c.startMember(id)
	}
	t.Cleanup(c.Shutdown)
	return c
}

// Shards returns the number of Raft groups per node.
func (c *Cluster) Shards() int { return c.shards }

func (c *Cluster) startMember(id raft.NodeID) {
	m := c.members[id]
	groups := make([]shard.Group, c.shards)
	for g := 0; g < c.shards; g++ {
		cfg := raft.Config{
			ID:                 id,
			Peers:              c.IDs,
			Transport:          c.Net.EndpointGroup(uint32(g), id),
			Storage:            m.Storages[g],
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 200 * time.Millisecond,
			HeartbeatInterval:  20 * time.Millisecond,
		}
		if c.cfg != nil {
			c.cfg(&cfg)
		}
		node, err := raft.NewNode(cfg)
		if err != nil {
			c.t.Fatalf("new node %d shard %d: %v", id, g, err)
		}
		var opts []kv.Option
		if c.snapshotEvery > 0 {
			opts = append(opts, kv.WithSnapshotEvery(c.snapshotEvery))
		}
		groups[g] = shard.Group{Raft: node, KV: kv.NewServer(node, opts...)}
	}
	m.Groups = groups
	m.Host = shard.NewHost(groups)
	m.Raft, m.KV = groups[0].Raft, groups[0].KV
	for g, grp := range groups {
		c.Net.RegisterGroup(uint32(g), id, grp.Raft)
		grp.Raft.Start()
	}
}

// Crash stops a node but keeps its storage, like a process kill.
func (c *Cluster) Crash(id raft.NodeID) {
	c.mu.Lock()
	m := c.members[id]
	groups := m.Groups
	m.Groups, m.Host, m.Raft, m.KV = nil, nil, nil, nil
	c.mu.Unlock()
	if groups == nil {
		return
	}
	c.Net.Unregister(id)
	for _, g := range groups {
		g.Raft.Stop()
	}
	for _, g := range groups {
		g.KV.Wait()
	}
}

// Restart boots a crashed node from its storage.
func (c *Cluster) Restart(id raft.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[id].Groups != nil {
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

// Member returns the member record (group fields are nil if crashed).
func (c *Cluster) Member(id raft.NodeID) Member {
	c.mu.Lock()
	defer c.mu.Unlock()
	return *c.members[id]
}

// Up reports whether id is running.
func (c *Cluster) Up(id raft.NodeID) bool { return c.Member(id).Groups != nil }

// Node returns the Raft node for (id, shard), or nil if crashed.
func (c *Cluster) Node(id raft.NodeID, g int) *raft.Node {
	if m := c.Member(id); m.Groups != nil {
		return m.Groups[g].Raft
	}
	return nil
}

// Server returns the KV server for (id, shard), or nil if crashed.
func (c *Cluster) Server(id raft.NodeID, g int) *kv.Server {
	if m := c.Member(id); m.Groups != nil {
		return m.Groups[g].KV
	}
	return nil
}

// Leaders returns running nodes that believe they lead shard 0.
func (c *Cluster) Leaders() []raft.NodeID { return c.LeadersOf(0) }

// LeadersOf returns running nodes that believe they lead shard g.
func (c *Cluster) LeadersOf(g int) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range c.IDs {
		if n := c.Node(id, g); n != nil && n.Status().Role == raft.Leader {
			out = append(out, id)
		}
	}
	return out
}

// WaitLeader waits for exactly one shard-0 leader among nodes (all if none
// given) and returns it.
func (c *Cluster) WaitLeader(timeout time.Duration, among ...raft.NodeID) raft.NodeID {
	c.t.Helper()
	return c.WaitLeaderOf(0, timeout, among...)
}

// WaitLeaderOf is WaitLeader for shard g.
func (c *Cluster) WaitLeaderOf(g int, timeout time.Duration, among ...raft.NodeID) raft.NodeID {
	c.t.Helper()
	if len(among) == 0 {
		among = c.IDs
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []raft.NodeID
		for _, id := range among {
			if n := c.Node(id, g); n != nil && n.Status().Role == raft.Leader {
				leaders = append(leaders, id)
			}
		}
		// Stale leaders in older terms can briefly coexist; require exactly
		// one in the highest term.
		if best := highestTermLeader(c, g, leaders); best != 0 {
			return best
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("shard %d: no leader elected within %v", g, timeout)
	return 0
}

// WaitAllLeaders waits until every shard has a leader.
func (c *Cluster) WaitAllLeaders(timeout time.Duration) {
	c.t.Helper()
	for g := 0; g < c.shards; g++ {
		c.WaitLeaderOf(g, timeout)
	}
}

func highestTermLeader(c *Cluster, g int, leaders []raft.NodeID) raft.NodeID {
	if len(leaders) == 0 {
		return 0
	}
	var best raft.NodeID
	var bestTerm uint64
	count := 0
	for _, id := range leaders {
		n := c.Node(id, g)
		if n == nil {
			continue
		}
		st := n.Status()
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

func (c *Cluster) endpoints() map[raft.NodeID]kvpb.KVClient {
	eps := make(map[raft.NodeID]kvpb.KVClient, len(c.IDs))
	for _, id := range c.IDs {
		eps[id] = &endpoint{c: c, id: id}
	}
	return eps
}

// Client returns a KV client that talks to the nodes directly (no network
// faults between client and node), tracking crashes. With several shards it
// still works but bounces between shard leaders; prefer ShardClient.
func (c *Cluster) Client() *kv.Client {
	cl := kv.NewClient(c.endpoints())
	cl.AttemptTimeout = time.Second
	return cl
}

// ShardClient returns a shard-routing client.
func (c *Cluster) ShardClient() *shard.Client {
	cl := shard.NewClient(c.endpoints(), c.shards)
	for _, s := range cl.Shards() {
		s.AttemptTimeout = time.Second
	}
	return cl
}

type endpoint struct {
	c  *Cluster
	id raft.NodeID
}

func (e *endpoint) host() (*shard.Host, error) {
	if m := e.c.Member(e.id); m.Host != nil {
		return m.Host, nil
	}
	return nil, fmt.Errorf("node %d is down", e.id)
}

func (e *endpoint) Put(ctx context.Context, in *kvpb.PutRequest, _ ...grpc.CallOption) (*kvpb.PutResponse, error) {
	h, err := e.host()
	if err != nil {
		return nil, err
	}
	return h.Put(ctx, in)
}

func (e *endpoint) Get(ctx context.Context, in *kvpb.GetRequest, _ ...grpc.CallOption) (*kvpb.GetResponse, error) {
	h, err := e.host()
	if err != nil {
		return nil, err
	}
	return h.Get(ctx, in)
}

func (e *endpoint) Delete(ctx context.Context, in *kvpb.DeleteRequest, _ ...grpc.CallOption) (*kvpb.DeleteResponse, error) {
	h, err := e.host()
	if err != nil {
		return nil, err
	}
	return h.Delete(ctx, in)
}

func (e *endpoint) CAS(ctx context.Context, in *kvpb.CASRequest, _ ...grpc.CallOption) (*kvpb.CASResponse, error) {
	h, err := e.host()
	if err != nil {
		return nil, err
	}
	return h.CAS(ctx, in)
}

func (e *endpoint) Scan(ctx context.Context, in *kvpb.ScanRequest, _ ...grpc.CallOption) (*kvpb.ScanResponse, error) {
	h, err := e.host()
	if err != nil {
		return nil, err
	}
	return h.Scan(ctx, in)
}

// Runner implements TB for use outside tests: Fatalf panics with a
// *FatalError and Close runs the registered cleanups.
type Runner struct {
	mu       sync.Mutex
	cleanups []func()
}

// FatalError is the panic value Runner.Fatalf raises.
type FatalError struct{ Msg string }

func (e *FatalError) Error() string { return e.Msg }

func (r *Runner) Helper() {}

func (r *Runner) Fatalf(format string, args ...any) {
	panic(&FatalError{Msg: fmt.Sprintf(format, args...)})
}

func (r *Runner) Cleanup(f func()) {
	r.mu.Lock()
	r.cleanups = append(r.cleanups, f)
	r.mu.Unlock()
}

// Close runs cleanups in reverse registration order.
func (r *Runner) Close() {
	r.mu.Lock()
	fs := r.cleanups
	r.cleanups = nil
	r.mu.Unlock()
	for i := len(fs) - 1; i >= 0; i-- {
		fs[i]()
	}
}
