package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Config configures a Node.
type Config struct {
	ID        NodeID
	Peers     []NodeID // every member of the group, including ID
	Transport Transport
	Storage   Storage

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	// MaxEntriesPerMsg bounds the batch size of a single AppendEntries.
	MaxEntriesPerMsg int
	// ApplyBuffer is the capacity of the channel returned by Applied.
	ApplyBuffer int

	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 40 * time.Millisecond
	}
	if c.MaxEntriesPerMsg == 0 {
		c.MaxEntriesPerMsg = 256
	}
	if c.ApplyBuffer == 0 {
		c.ApplyBuffer = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// Node is one member of a Raft group.
type Node struct {
	cfg    Config
	id     NodeID
	peers  []NodeID // everyone except self
	quorum int
	logger *slog.Logger
	rng    *rand.Rand

	mu       sync.Mutex
	role     Role
	term     uint64
	votedFor NodeID
	leader   NodeID
	log      *raftLog

	commitIndex uint64
	lastApplied uint64

	// Leader state, valid while role == Leader.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	wake       map[NodeID]chan struct{}
	hbSeq      uint64
	peerAck    map[NodeID]uint64
	reads      []*readReq

	electionDeadline  time.Time
	lastLeaderContact time.Time

	applyCond *sync.Cond
	applyCh   chan Entry

	ctx     context.Context
	cancel  context.CancelFunc
	stopped bool
	wg      sync.WaitGroup
}

// NewNode loads persisted state and returns a node that is not yet running.
func NewNode(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if cfg.Transport == nil || cfg.Storage == nil {
		return nil, fmt.Errorf("raft: transport and storage are required")
	}
	var peers []NodeID
	self := false
	for _, p := range cfg.Peers {
		if p == cfg.ID {
			self = true
			continue
		}
		peers = append(peers, p)
	}
	if !self {
		return nil, fmt.Errorf("raft: node %d not in peer list", cfg.ID)
	}

	hs, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	l := newRaftLog()
	for _, e := range entries {
		if e.Index != l.lastIndex()+1 {
			return nil, fmt.Errorf("raft: storage has non-contiguous log at index %d", e.Index)
		}
		l.append(e)
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		id:       cfg.ID,
		peers:    peers,
		quorum:   (len(peers)+1)/2 + 1,
		logger:   cfg.Logger.With("node", cfg.ID),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID)*7919)),
		term:     hs.Term,
		votedFor: hs.VotedFor,
		log:      l,
		applyCh:  make(chan Entry, cfg.ApplyBuffer),
		ctx:      ctx,
		cancel:   cancel,
	}
	n.applyCond = sync.NewCond(&n.mu)
	return n, nil
}

// Start launches the background goroutines.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetElectionDeadline()
	n.goLocked(n.tickLoop)
	n.goLocked(n.applyLoop)
}

// Stop halts the node and closes the Applied channel once drained.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.cancel()
	n.failReadsLocked(ErrStopped)
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// Applied delivers committed entries in log order. The channel is closed when
// the node stops.
func (n *Node) Applied() <-chan Entry { return n.applyCh }

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Term:        n.term,
		Role:        n.role,
		Leader:      n.leader,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LastIndex:   n.log.lastIndex(),
	}
}

// Propose appends data to the log if this node is the leader. It returns the
// index and term the entry was placed at; the entry is not yet committed.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader {
		return 0, 0, false
	}
	e := Entry{Term: n.term, Index: n.log.lastIndex() + 1, Type: EntryNormal, Data: data}
	n.appendLocked(e)
	n.syncLocked()
	n.afterLocalAppendLocked()
	return e.Index, e.Term, true
}

func (n *Node) afterLocalAppendLocked() {
	if len(n.peers) == 0 {
		n.advanceCommitLocked()
		return
	}
	n.wakeAllLocked()
}

// goLocked starts f as a tracked goroutine. mu must be held.
func (n *Node) goLocked(f func()) {
	if n.stopped {
		return
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
}

func (n *Node) tickLoop() {
	tick := n.cfg.ElectionTimeoutMin / 10
	if tick > 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		n.mu.Lock()
		if !n.stopped && n.role != Leader && time.Now().After(n.electionDeadline) {
			n.startPreVoteLocked()
		}
		n.mu.Unlock()
	}
}

func (n *Node) resetElectionDeadline() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	d := n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)))
	n.electionDeadline = time.Now().Add(d)
}

// ---- persistence helpers (fail-stop on I/O errors) ----

func (n *Node) mustPersist(err error) {
	if err != nil {
		panic(fmt.Sprintf("raft node %d: storage failure: %v", n.id, err))
	}
}

func (n *Node) saveHardStateLocked() {
	n.mustPersist(n.cfg.Storage.SaveHardState(HardState{Term: n.term, VotedFor: n.votedFor}))
}

func (n *Node) syncLocked() { n.mustPersist(n.cfg.Storage.Sync()) }

func (n *Node) appendLocked(es ...Entry) {
	n.log.append(es...)
	n.mustPersist(n.cfg.Storage.Append(es))
}

func (n *Node) truncateLocked(from uint64) {
	n.log.truncateFrom(from)
	n.mustPersist(n.cfg.Storage.TruncateFrom(from))
}

// ---- apply path ----

func (n *Node) applyLoop() {
	defer close(n.applyCh)
	for {
		n.mu.Lock()
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		lo, hi := n.lastApplied+1, n.commitIndex
		batch := make([]Entry, 0, hi-lo+1)
		for i := lo; i <= hi; i++ {
			batch = append(batch, n.log.entry(i))
		}
		n.mu.Unlock()

		for _, e := range batch {
			select {
			case n.applyCh <- e:
			case <-n.ctx.Done():
				return
			}
			n.mu.Lock()
			n.lastApplied = e.Index
			n.mu.Unlock()
		}
	}
}

// ---- term / role transitions ----

// becomeFollowerLocked moves to follower in the given term. If the term is
// newer, the vote is cleared and the new term is persisted (not yet synced).
func (n *Node) becomeFollowerLocked(term uint64, leader NodeID) {
	wasLeader := n.role == Leader
	if term > n.term {
		n.term = term
		n.votedFor = 0
		n.saveHardStateLocked()
		n.syncLocked()
	}
	n.role = Follower
	n.leader = leader
	if wasLeader {
		n.failReadsLocked(ErrNotLeader)
		n.nextIndex, n.matchIndex, n.wake, n.peerAck = nil, nil, nil, nil
	}
}
