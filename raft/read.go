package raft

import (
	"context"
	"time"
)

type readReq struct {
	index uint64
	seq   uint64
	done  chan error
}

// ReadIndex returns a log index such that, once the state machine has applied
// it, a local read is linearizable. It confirms leadership with a quorum
// heartbeat round. Only the leader can serve it.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return 0, ErrStopped
		}
		if n.role != Leader {
			n.mu.Unlock()
			return 0, ErrNotLeader
		}
		// Leader must have committed an entry of its own term first,
		// otherwise commitIndex may lag behind what earlier leaders committed.
		if t, _ := n.log.term(n.commitIndex); t != n.term {
			n.mu.Unlock()
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		r := &readReq{index: n.commitIndex, done: make(chan error, 1)}
		if len(n.peers) == 0 {
			n.mu.Unlock()
			return r.index, nil
		}
		n.hbSeq++
		r.seq = n.hbSeq
		n.reads = append(n.reads, r)
		n.wakeAllLocked()
		n.mu.Unlock()

		select {
		case err := <-r.done:
			if err != nil {
				return 0, err
			}
			return r.index, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// completeReadsLocked releases reads whose heartbeat round reached a quorum.
func (n *Node) completeReadsLocked() {
	if len(n.reads) == 0 {
		return
	}
	kept := n.reads[:0]
	for _, r := range n.reads {
		acks := 1
		for _, p := range n.peers {
			if n.peerAck[p] >= r.seq {
				acks++
			}
		}
		if acks >= n.quorum {
			r.done <- nil
		} else {
			kept = append(kept, r)
		}
	}
	for i := len(kept); i < len(n.reads); i++ {
		n.reads[i] = nil
	}
	n.reads = kept
}

func (n *Node) failReadsLocked(err error) {
	for _, r := range n.reads {
		r.done <- err
	}
	n.reads = nil
}
