package raft

import (
	"context"
	"sort"
	"time"
)

func (n *Node) wakeAllLocked() {
	for _, ch := range n.wake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// replicator drives AppendEntries to one follower for the lifetime of a term.
func (n *Node) replicator(peer NodeID, term uint64, wake chan struct{}) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-wake:
		case <-timer.C:
		}
		for {
			stillLeader, more := n.replicateOnce(peer, term)
			if !stillLeader {
				return
			}
			if !more {
				break
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(n.cfg.HeartbeatInterval)
	}
}

// replicateOnce sends one AppendEntries (possibly an empty heartbeat) to peer.
// It reports whether this node is still leader of term, and whether the peer
// is still behind so another round should follow immediately.
func (n *Node) replicateOnce(peer NodeID, term uint64) (stillLeader, more bool) {
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.term != term {
		n.mu.Unlock()
		return false, false
	}
	next := n.nextIndex[peer]
	prev := next - 1
	prevTerm, _ := n.log.term(prev)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.slice(next, n.cfg.MaxEntriesPerMsg),
		LeaderCommit: n.commitIndex,
	}
	seq := n.hbSeq
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout())
	reply, err := n.cfg.Transport.AppendEntries(ctx, peer, args)
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

	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		if match+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = match + 1
		}
		if seq > n.peerAck[peer] {
			n.peerAck[peer] = seq
		}
		n.advanceCommitLocked()
		n.completeReadsLocked()
		return true, n.nextIndex[peer] <= n.log.lastIndex()
	}

	// Rejected: jump back using the follower's hint instead of one at a time.
	ni := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		if idx := n.log.lastIndexOfTerm(reply.ConflictTerm); idx != 0 {
			ni = idx + 1
		}
	}
	if ni < 1 {
		ni = 1
	}
	if ni > args.PrevLogIndex {
		ni = args.PrevLogIndex // always make progress backwards
	}
	if ni < 1 {
		ni = 1
	}
	n.nextIndex[peer] = ni
	return true, true
}

// advanceCommitLocked moves commitIndex to the highest index replicated on a
// quorum whose entry is from the current term.
func (n *Node) advanceCommitLocked() {
	matches := make([]uint64, 0, len(n.peers)+1)
	matches = append(matches, n.log.lastIndex())
	for _, p := range n.peers {
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	candidate := matches[len(matches)-n.quorum]
	if candidate <= n.commitIndex {
		return
	}
	if t, ok := n.log.term(candidate); !ok || t != n.term {
		return
	}
	n.commitIndex = candidate
	n.applyCond.Signal()
	n.wakeAllLocked() // push the new commit index to followers promptly
}

// HandleAppendEntries implements Handler.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if args.Term < n.term {
		return &AppendEntriesReply{Term: n.term}, nil
	}
	if args.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(args.Term, args.LeaderID)
	}
	n.leader = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionDeadline()
	reply := &AppendEntriesReply{Term: n.term}

	last := n.log.lastIndex()
	if args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1
		return reply, nil
	}
	if t, ok := n.log.term(args.PrevLogIndex); !ok || t != args.PrevLogTerm {
		reply.ConflictTerm = t
		reply.ConflictIndex = n.log.firstIndexOfTerm(t, args.PrevLogIndex)
		return reply, nil
	}

	dirty := false
	for i, e := range args.Entries {
		if e.Index <= n.log.lastIndex() {
			if t, _ := n.log.term(e.Index); t == e.Term {
				continue // already have it
			}
			n.truncateLocked(e.Index)
		}
		n.appendLocked(args.Entries[i:]...)
		dirty = true
		break
	}
	if dirty {
		n.syncLocked()
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		c := args.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Signal()
		}
	}
	reply.Success = true
	return reply, nil
}
