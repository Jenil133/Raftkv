package raft

import (
	"errors"
	"fmt"
)

// NodeID identifies a Raft member. Zero means "no node".
type NodeID uint64

// Role is the current Raft role of a node.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	// EntryNormal carries an opaque state machine command.
	EntryNormal EntryType = iota
	// EntryNoop is appended by a new leader to commit entries of prior terms.
	EntryNoop
)

// Entry is a single replicated log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// HardState is the state that must survive restarts.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

// Status is a point-in-time view of a node, for tests and metrics.
type Status struct {
	ID          NodeID
	Term        uint64
	Role        Role
	Leader      NodeID
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// On failure, hints that let the leader skip back over a whole term.
	ConflictIndex uint64
	ConflictTerm  uint64
}

var (
	// ErrNotLeader is returned when an operation needs the leader.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrStopped is returned after the node has been stopped.
	ErrStopped = errors.New("raft: node stopped")
)
