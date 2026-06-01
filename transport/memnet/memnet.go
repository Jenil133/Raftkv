// Package memnet is an in-process network for Raft nodes with injectable
// faults: crashed nodes, partitions, packet loss and delay.
package memnet

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/Jenil133/raftkv/raft"
)

// ErrUnreachable is returned when a message is dropped or the peer is down.
var ErrUnreachable = errors.New("memnet: unreachable")

type link struct{ from, to raft.NodeID }

// hkey addresses one Raft group on one node. Several groups (shards) can share
// a node, and faults apply to the node, hence to all its groups.
type hkey struct {
	group uint32
	node  raft.NodeID
}

// Network connects registered handlers.
type Network struct {
	mu       sync.RWMutex
	handlers map[hkey]raft.Handler
	blocked  map[link]bool
	isolated map[raft.NodeID]bool
	dropRate float64
	dupRate  float64
	minDelay time.Duration
	maxDelay time.Duration
	rng      *rand.Rand
	rngMu    sync.Mutex
}

// New returns a network whose randomness is seeded with seed.
func New(seed int64) *Network {
	return &Network{
		handlers: make(map[hkey]raft.Handler),
		blocked:  make(map[link]bool),
		isolated: make(map[raft.NodeID]bool),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Register attaches h as the receiver for id in group 0.
func (n *Network) Register(id raft.NodeID, h raft.Handler) { n.RegisterGroup(0, id, h) }

// RegisterGroup attaches h as the receiver for id in the given group,
// replacing any previous one.
func (n *Network) RegisterGroup(group uint32, id raft.NodeID, h raft.Handler) {
	n.mu.Lock()
	n.handlers[hkey{group, id}] = h
	n.mu.Unlock()
}

// Unregister detaches id from every group; calls to it fail (a crashed node).
func (n *Network) Unregister(id raft.NodeID) {
	n.mu.Lock()
	for k := range n.handlers {
		if k.node == id {
			delete(n.handlers, k)
		}
	}
	n.mu.Unlock()
}

// Isolate cuts id off from everyone in both directions.
func (n *Network) Isolate(id raft.NodeID) {
	n.mu.Lock()
	n.isolated[id] = true
	n.mu.Unlock()
}

// Reconnect undoes Isolate.
func (n *Network) Reconnect(id raft.NodeID) {
	n.mu.Lock()
	delete(n.isolated, id)
	n.mu.Unlock()
}

// Partition splits the listed groups from each other; nodes within a group
// still talk. Nodes not listed are unaffected by the split among others.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					n.blocked[link{x, y}] = true
				}
			}
		}
	}
}

// Block drops traffic from -> to only (one-way fault).
func (n *Network) Block(from, to raft.NodeID) {
	n.mu.Lock()
	n.blocked[link{from, to}] = true
	n.mu.Unlock()
}

// Heal removes all partitions, blocks and isolation.
func (n *Network) Heal() {
	n.mu.Lock()
	n.blocked = make(map[link]bool)
	n.isolated = make(map[raft.NodeID]bool)
	n.mu.Unlock()
}

// SetDropRate drops each message (request or reply) with probability p.
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	n.dropRate = p
	n.mu.Unlock()
}

// SetDupRate delivers each request a second time, after a random delay, with
// probability p. The duplicate's reply is discarded, as a retransmitted
// packet's would be.
func (n *Network) SetDupRate(p float64) {
	n.mu.Lock()
	n.dupRate = p
	n.mu.Unlock()
}

// SetDelay adds a uniformly random one-way delay in [min, max].
func (n *Network) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	n.minDelay, n.maxDelay = min, max
	n.mu.Unlock()
}

// Endpoint returns the group-0 raft.Transport that node from uses.
func (n *Network) Endpoint(from raft.NodeID) raft.Transport { return n.EndpointGroup(0, from) }

// EndpointGroup returns the raft.Transport node from uses for one group.
func (n *Network) EndpointGroup(group uint32, from raft.NodeID) raft.Transport {
	return &endpoint{net: n, from: from, group: group}
}

type endpoint struct {
	net   *Network
	from  raft.NodeID
	group uint32
}

func (e *endpoint) RequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	r, err := e.net.deliver(ctx, e.group, e.from, to, func(h raft.Handler) (any, error) {
		a := *args
		return h.HandleRequestVote(&a)
	})
	if err != nil {
		return nil, err
	}
	return r.(*raft.RequestVoteReply), nil
}

func (e *endpoint) AppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	r, err := e.net.deliver(ctx, e.group, e.from, to, func(h raft.Handler) (any, error) {
		a := *args
		a.Entries = append([]raft.Entry(nil), args.Entries...)
		return h.HandleAppendEntries(&a)
	})
	if err != nil {
		return nil, err
	}
	return r.(*raft.AppendEntriesReply), nil
}

func (e *endpoint) InstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	r, err := e.net.deliver(ctx, e.group, e.from, to, func(h raft.Handler) (any, error) {
		a := *args
		a.Data = append([]byte(nil), args.Data...)
		return h.HandleInstallSnapshot(&a)
	})
	if err != nil {
		return nil, err
	}
	return r.(*raft.InstallSnapshotReply), nil
}

func (n *Network) reachable(group uint32, from, to raft.NodeID) (raft.Handler, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	h, ok := n.handlers[hkey{group, to}]
	if !ok || n.isolated[from] || n.isolated[to] || n.blocked[link{from, to}] {
		return nil, false
	}
	return h, true
}

func (n *Network) lossy() (drop, dup bool, delay time.Duration) {
	n.mu.RLock()
	p, pd, lo, hi := n.dropRate, n.dupRate, n.minDelay, n.maxDelay
	n.mu.RUnlock()
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	if p > 0 && n.rng.Float64() < p {
		drop = true
	}
	if pd > 0 && n.rng.Float64() < pd {
		dup = true
	}
	if hi > 0 {
		delay = lo
		if hi > lo {
			delay += time.Duration(n.rng.Int63n(int64(hi - lo)))
		}
	}
	return
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// deliver models request -> handler -> reply with faults on each leg.
func (n *Network) deliver(ctx context.Context, group uint32, from, to raft.NodeID, invoke func(raft.Handler) (any, error)) (any, error) {
	h, ok := n.reachable(group, from, to)
	if !ok {
		return nil, ErrUnreachable
	}
	drop, dup, delay := n.lossy()
	if drop {
		return nil, ErrUnreachable
	}
	if dup {
		_, _, d2 := n.lossy()
		go func() {
			time.Sleep(d2 + time.Millisecond)
			if h, ok := n.reachable(group, from, to); ok {
				invoke(h) // reply of the duplicate is lost
			}
		}()
	}
	if err := sleep(ctx, delay); err != nil {
		return nil, err
	}
	if h, ok = n.reachable(group, from, to); !ok {
		return nil, ErrUnreachable
	}
	reply, err := invoke(h)
	if err != nil {
		return nil, err
	}
	// Reply leg.
	drop, _, delay = n.lossy()
	if drop {
		return nil, ErrUnreachable
	}
	if err := sleep(ctx, delay); err != nil {
		return nil, err
	}
	if _, ok = n.reachable(group, to, from); !ok {
		return nil, ErrUnreachable
	}
	return reply, nil
}
