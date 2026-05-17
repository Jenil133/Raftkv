package raft

import (
	"context"
	"time"
)

func (n *Node) rpcTimeout() time.Duration {
	d := 5 * n.cfg.HeartbeatInterval
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	return d
}

// startPreVoteLocked asks the group whether an election would succeed before
// bumping the term, so a partitioned node cannot inflate terms and disrupt a
// healthy leader when it rejoins.
func (n *Node) startPreVoteLocked() {
	n.resetElectionDeadline()
	if len(n.peers) == 0 {
		n.startElectionLocked()
		return
	}
	myDeadline := n.electionDeadline
	curTerm := n.term
	args := &RequestVoteArgs{
		Term:         n.term + 1,
		CandidateID:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
		PreVote:      true,
	}
	votes := 1
	for _, p := range n.peers {
		p := p
		n.goLocked(func() {
			ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout())
			defer cancel()
			reply, err := n.cfg.Transport.RequestVote(ctx, p, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if reply.Term > n.term {
				n.becomeFollowerLocked(reply.Term, 0)
				return
			}
			// Abort if the world moved on: new term, became leader, or heard
			// from a leader / granted a vote (which resets the deadline).
			if n.role == Leader || n.term != curTerm || !n.electionDeadline.Equal(myDeadline) {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes == n.quorum {
					n.startElectionLocked()
				}
			}
		})
	}
}

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leader = 0
	n.saveHardStateLocked()
	n.syncLocked()
	n.resetElectionDeadline()
	term := n.term
	n.stats.Elections++
	n.logger.Debug("starting election", "term", term)

	votes := 1
	if votes >= n.quorum {
		n.becomeLeaderLocked()
		return
	}
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	for _, p := range n.peers {
		p := p
		n.goLocked(func() {
			ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout())
			defer cancel()
			reply, err := n.cfg.Transport.RequestVote(ctx, p, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if reply.Term > n.term {
				n.becomeFollowerLocked(reply.Term, 0)
				return
			}
			if n.role != Candidate || n.term != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes == n.quorum {
					n.becomeLeaderLocked()
				}
			}
		})
	}
}

func (n *Node) becomeLeaderLocked() {
	n.logger.Info("became leader", "term", n.term)
	n.role = Leader
	n.leader = n.id
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	n.wake = make(map[NodeID]chan struct{}, len(n.peers))
	n.peerAck = make(map[NodeID]uint64, len(n.peers))
	n.snapOff = make(map[NodeID]uint64, len(n.peers))
	n.reads = nil
	n.stats.TermsAsLeader++
	last := n.log.lastIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
		n.wake[p] = make(chan struct{}, 1)
	}

	// A no-op in the new term lets the leader commit entries from earlier
	// terms (Raft §5.4.2) and enables linearizable reads.
	n.appendLocked(Entry{Term: n.term, Index: last + 1, Type: EntryNoop})
	n.syncLocked()

	term := n.term
	for _, p := range n.peers {
		p, wake := p, n.wake[p]
		n.goLocked(func() { n.replicator(p, term, wake) })
	}
	n.afterLocalAppendLocked()
}

// HandleRequestVote implements Handler.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}

	if args.PreVote {
		// Grant only if we would vote for this candidate in a real election
		// and have not recently heard from a live leader. No state changes.
		granted := args.Term > n.term &&
			n.role != Leader &&
			time.Since(n.lastLeaderContact) >= n.cfg.ElectionTimeoutMin &&
			n.log.isUpToDate(args.LastLogIndex, args.LastLogTerm)
		return &RequestVoteReply{Term: n.term, VoteGranted: granted}, nil
	}

	if args.Term < n.term {
		return &RequestVoteReply{Term: n.term}, nil
	}
	if args.Term > n.term {
		n.becomeFollowerLocked(args.Term, 0)
	}
	reply := &RequestVoteReply{Term: n.term}
	if (n.votedFor == 0 || n.votedFor == args.CandidateID) &&
		n.log.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.votedFor = args.CandidateID
		n.saveHardStateLocked()
		n.syncLocked()
		n.resetElectionDeadline()
		reply.VoteGranted = true
	}
	return reply, nil
}
