package raft

import (
	"context"
	"fmt"
	"time"
)

// incomingSnapshot accumulates the chunks of a snapshot being received.
type incomingSnapshot struct {
	index, term uint64
	buf         []byte
}

// Snapshot is called by the state machine once it has applied every entry up
// to index and serialised its state into data. The node persists the snapshot
// and discards the log prefix it covers. Writing the snapshot happens outside
// the node's main lock, so replication and elections carry on meanwhile.
func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrStopped
	}
	// Stop waits for us, so storage is never closed under our feet.
	n.wg.Add(1)
	defer n.wg.Done()
	if index <= n.log.offset {
		n.mu.Unlock()
		return nil // already covered by an equal or newer snapshot
	}
	// The state machine has applied index, so it is committed: no leader can
	// ever truncate it, and the term below stays valid after we unlock.
	if index > n.commitIndex {
		n.mu.Unlock()
		return fmt.Errorf("raft: snapshot index %d beyond commit index %d", index, n.commitIndex)
	}
	term, ok := n.log.term(index)
	n.mu.Unlock()
	if !ok {
		return fmt.Errorf("raft: snapshot index %d not in log", index)
	}
	snap := Snapshot{Index: index, Term: term, Data: data}

	// snapMu orders snapshot writes with installs from a leader; storage
	// ignores a snapshot older than the one it holds.
	n.snapMu.Lock()
	err := n.cfg.Storage.SaveSnapshot(snap)
	n.snapMu.Unlock()
	n.mustPersist(err)

	n.mu.Lock()
	defer n.mu.Unlock()
	if index > n.log.offset {
		if t, ok := n.log.term(index); ok && t == term {
			n.log.compactTo(index)
			n.snapshot = snap
			n.stats.SnapshotsTaken++
		}
	}
	return nil
}

// sendSnapshotLocked builds the next InstallSnapshot chunk for peer. mu held.
func (n *Node) snapshotChunkLocked(peer NodeID) *InstallSnapshotArgs {
	snap := n.snapshot
	off := n.snapOff[peer]
	if off > uint64(len(snap.Data)) {
		off = 0
	}
	end := off + uint64(n.cfg.SnapshotChunkSize)
	if end > uint64(len(snap.Data)) {
		end = uint64(len(snap.Data))
	}
	return &InstallSnapshotArgs{
		Term:              n.term,
		LeaderID:          n.id,
		LastIncludedIndex: snap.Index,
		LastIncludedTerm:  snap.Term,
		Offset:            off,
		Data:              snap.Data[off:end],
		Done:              end == uint64(len(snap.Data)),
	}
}

// replicateSnapshot sends one snapshot chunk to a follower that has fallen
// behind the compacted prefix of the leader's log.
func (n *Node) replicateSnapshot(peer NodeID, term uint64) (stillLeader, more bool) {
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.term != term {
		n.mu.Unlock()
		return false, false
	}
	args := n.snapshotChunkLocked(peer)
	seq := n.hbSeq
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout())
	reply, err := n.cfg.Transport.InstallSnapshot(ctx, peer, args)
	cancel()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.role != Leader || n.term != term {
		return false, false
	}
	if err != nil {
		return true, false
	}
	if reply.Term > n.term {
		n.becomeFollowerLocked(reply.Term, 0)
		n.resetElectionDeadline()
		return false, false
	}
	if seq > n.peerAck[peer] {
		n.peerAck[peer] = seq
	}
	sent := args.Offset + uint64(len(args.Data))
	if args.Done && reply.NextOffset >= sent {
		// Follower holds the whole snapshot (or already had more).
		delete(n.snapOff, peer)
		if args.LastIncludedIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = args.LastIncludedIndex + 1
		}
		n.stats.SnapshotsSent++
		n.advanceCommitLocked()
		n.completeReadsLocked()
		return true, n.nextIndex[peer] <= n.log.lastIndex()
	}
	n.snapOff[peer] = reply.NextOffset
	n.completeReadsLocked()
	return true, true
}

// HandleInstallSnapshot implements Handler.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if args.Term < n.term {
		return &InstallSnapshotReply{Term: n.term}, nil
	}
	if args.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(args.Term, args.LeaderID)
	}
	n.leader = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionDeadline()
	reply := &InstallSnapshotReply{Term: n.term}
	sent := args.Offset + uint64(len(args.Data))

	// Already have everything this snapshot covers: tell the leader to move on.
	if args.LastIncludedIndex <= n.commitIndex || args.LastIncludedIndex <= n.log.offset {
		reply.NextOffset = sent
		return reply, nil
	}

	inc := n.incoming
	switch {
	case args.Offset == 0:
		inc = &incomingSnapshot{index: args.LastIncludedIndex, term: args.LastIncludedTerm}
		n.incoming = inc
	case inc == nil || inc.index != args.LastIncludedIndex || inc.term != args.LastIncludedTerm:
		n.incoming = nil
		return reply, nil // NextOffset 0: restart from the beginning
	case sent <= uint64(len(inc.buf)):
		reply.NextOffset = uint64(len(inc.buf)) // duplicate chunk
		return reply, nil
	case args.Offset != uint64(len(inc.buf)):
		n.incoming = nil
		return reply, nil
	}
	inc.buf = append(inc.buf, args.Data...)
	reply.NextOffset = uint64(len(inc.buf))
	if !args.Done {
		return reply, nil
	}

	n.incoming = nil
	n.installSnapshotLocked(Snapshot{Index: inc.index, Term: inc.term, Data: inc.buf})
	return reply, nil
}

// installSnapshotLocked adopts a complete snapshot received from the leader.
func (n *Node) installSnapshotLocked(snap Snapshot) {
	if t, ok := n.log.term(snap.Index); ok && t == snap.Term && snap.Index > n.log.offset {
		// Our log already contains the snapshot's last entry: keep what follows.
		n.log.compactTo(snap.Index)
	} else {
		// Log is behind or conflicts with the snapshot: drop all of it.
		n.mustPersist(n.cfg.Storage.TruncateFrom(snap.Index + 1))
		n.log.reset(snap.Index, snap.Term)
	}
	n.snapMu.Lock()
	err := n.cfg.Storage.SaveSnapshot(snap)
	n.snapMu.Unlock()
	n.mustPersist(err)
	n.snapshot = snap
	if snap.Index > n.commitIndex {
		n.commitIndex = snap.Index
	}
	n.pendingSnap = &snap
	n.stats.SnapshotsInstalled++
	n.applyCond.Signal()
}
