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

// Network connects registered handlers.
type Network struct {
	mu       sync.RWMutex
	handlers map[raft.NodeID]raft.Handler
	blocked  map[link]bool
	isolated map[raft.NodeID]bool
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration
	rng      *rand.Rand
	rngMu    sync.Mutex
}

// New returns a network whose randomness is seeded with seed.
func New(seed int64) *Network {
	return &Network{
		handlers: make(map[raft.NodeID]raft.Handler),
		blocked:  make(map[link]bool),
		isolated: make(map[raft.NodeID]bool),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Register attaches h as the receiver for id, replacing any previous one.
func (n *Network) Register(id raft.NodeID, h raft.Handler) {
	n.mu.Lock()
	n.handlers[id] = h
	n.mu.Unlock()
}

// Unregister detaches id; calls to it fail (a crashed node).
func (n *Network) Unregister(id raft.NodeID) {
	n.mu.Lock()
	delete(n.handlers, id)
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

// SetDelay adds a uniformly random one-way delay in [min, max].
func (n *Network) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	n.minDelay, n.maxDelay = min, max
	n.mu.Unlock()
}

// Endpoint returns the raft.Transport that node from uses.
func (n *Network) Endpoint(from raft.NodeID) raft.Transport {
	return &endpoint{net: n, from: from}
}

type endpoint struct {
	net  *Network
	from raft.NodeID
}

func (e *endpoint) RequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var reply *raft.RequestVoteReply
	err := e.net.deliver(ctx, e.from, to, func(h raft.Handler) (err error) {
		a := *args
		reply, err = h.HandleRequestVote(&a)
		return err
	})
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (e *endpoint) AppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var reply *raft.AppendEntriesReply
	err := e.net.deliver(ctx, e.from, to, func(h raft.Handler) (err error) {
		a := *args
		a.Entries = append([]raft.Entry(nil), args.Entries...)
		reply, err = h.HandleAppendEntries(&a)
		return err
	})
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (n *Network) reachable(from, to raft.NodeID) (raft.Handler, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	h, ok := n.handlers[to]
	if !ok || n.isolated[from] || n.isolated[to] || n.blocked[link{from, to}] {
		return nil, false
	}
	return h, true
}

func (n *Network) lossy() (drop bool, delay time.Duration) {
	n.mu.RLock()
	p, lo, hi := n.dropRate, n.minDelay, n.maxDelay
	n.mu.RUnlock()
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	if p > 0 && n.rng.Float64() < p {
		drop = true
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
func (n *Network) deliver(ctx context.Context, from, to raft.NodeID, call func(raft.Handler) error) error {
	h, ok := n.reachable(from, to)
	if !ok {
		return ErrUnreachable
	}
	drop, delay := n.lossy()
	if drop {
		return ErrUnreachable
	}
	if err := sleep(ctx, delay); err != nil {
		return err
	}
	if h, ok = n.reachable(from, to); !ok {
		return ErrUnreachable
	}
	if err := call(h); err != nil {
		return err
	}
	// Reply leg.
	drop, delay = n.lossy()
	if drop {
		return ErrUnreachable
	}
	if err := sleep(ctx, delay); err != nil {
		return err
	}
	if _, ok = n.reachable(to, from); !ok {
		return ErrUnreachable
	}
	return nil
}
