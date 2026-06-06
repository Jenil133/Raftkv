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
	// MaxEntriesPerMsg and MaxBytesPerMsg bound the batch carried by a single
	// AppendEntries; bigger batches mean fewer fsync rounds per entry.
	MaxEntriesPerMsg int
	MaxBytesPerMsg   int
	// SnapshotChunkSize bounds the bytes sent per InstallSnapshot RPC.
	SnapshotChunkSize int
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
		c.MaxEntriesPerMsg = 4096
	}
	if c.MaxBytesPerMsg == 0 {
		c.MaxBytesPerMsg = 2 << 20 // well under gRPC's 4 MiB default message limit
	}
	if c.SnapshotChunkSize == 0 {
		c.SnapshotChunkSize = 256 << 10
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
	// durableIndex is how far the leader's own log is known to be on disk. The
	// leader appends without waiting for fsync and counts itself toward the
	// commit quorum only up to here (Raft thesis §10.2.1).
	durableIndex uint64
	syncCh       chan struct{}

	// Leader state, valid while role == Leader.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	wake       map[NodeID]chan struct{}
	hbSeq      uint64
	peerAck    map[NodeID]uint64
	reads      []*readReq
	snapOff    map[NodeID]uint64 // bytes of the snapshot already sent per peer

	electionDeadline  time.Time
	lastLeaderContact time.Time

	// Snapshot state. The log offset always equals snapshot.Index.
	// snapMu serialises snapshot writes to storage; lock order is mu, snapMu.
	snapMu      sync.Mutex
	snapshot    Snapshot
	pendingSnap *Snapshot // snapshot waiting to be handed to the state machine
	incoming    *incomingSnapshot
	stats       Stats

	applyCond *sync.Cond
	applyCh   chan Apply

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

	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	hs := st.HardState
	l := newRaftLog()
	if st.Snapshot.Index > 0 {
		l.reset(st.Snapshot.Index, st.Snapshot.Term)
	}
	for _, e := range st.Entries {
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
		applyCh:  make(chan Apply, cfg.ApplyBuffer),
		syncCh:   make(chan struct{}, 1),
		ctx:      ctx,
		cancel:   cancel,
	}
	if st.Snapshot.Index > 0 {
		// The state machine starts empty and is restored from the snapshot
		// before any entry is delivered.
		snap := st.Snapshot
		n.snapshot = snap
		n.pendingSnap = &snap
		n.commitIndex = snap.Index
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
	n.goLocked(n.syncLoop)
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

// Applied delivers committed entries and installed snapshots in order. The
// channel is closed when the node stops.
func (n *Node) Applied() <-chan Apply { return n.applyCh }

// Stats returns the node's counters.
func (n *Node) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

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

		SnapshotIndex: n.log.offset,
		LogLength:     n.log.lastIndex() - n.log.offset,
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
	// Replicate right away; the local fsync happens in parallel (syncLoop)
	// and many proposals share one fsync.
	n.signalSync()
	n.wakeAllLocked()
	return e.Index, e.Term, true
}

func (n *Node) signalSync() {
	select {
	case n.syncCh <- struct{}{}:
	default:
	}
}

// syncLoop group-commits the leader's log: every wakeup fsyncs whatever has
// been appended so far, then lets the commit index advance.
func (n *Node) syncLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.syncCh:
		}
		n.mu.Lock()
		target, term, leader := n.log.lastIndex(), n.term, n.role == Leader
		n.mu.Unlock()

		err := n.cfg.Storage.Sync()

		n.mu.Lock()
		n.mustPersist(err)
		// Only trust target if we have been leader of the same term the whole
		// time: a leader never truncates its own log within a term.
		if leader && n.role == Leader && n.term == term && target > n.durableIndex {
			n.durableIndex = target
			n.advanceCommitLocked()
		}
		n.mu.Unlock()
	}
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
		for !n.stopped && n.pendingSnap == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if snap := n.pendingSnap; snap != nil {
			n.pendingSnap = nil
			n.mu.Unlock()
			select {
			case n.applyCh <- Apply{Snapshot: snap}:
			case <-n.ctx.Done():
				return
			}
			n.mu.Lock()
			n.lastApplied = snap.Index
			n.mu.Unlock()
			continue
		}
		lo, hi := n.lastApplied+1, n.commitIndex
		batch := make([]Entry, 0, hi-lo+1)
		for i := lo; i <= hi; i++ {
			batch = append(batch, n.log.entry(i))
		}
		n.mu.Unlock()

		for _, e := range batch {
			select {
			case n.applyCh <- Apply{Entry: e}:
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
		n.nextIndex, n.matchIndex, n.wake, n.peerAck, n.snapOff = nil, nil, nil, nil, nil
	}
}
