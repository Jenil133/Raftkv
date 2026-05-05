package raft

import "context"

// Transport sends Raft RPCs to peers. Implementations must be safe for
// concurrent use. A returned error means the RPC did not complete.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Handler receives Raft RPCs on behalf of a node. *Node implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Storage persists the hard state and the log. Mutating calls only need to be
// durable once Sync returns; the node calls Sync before acknowledging anything
// that depends on them.
type Storage interface {
	// Load returns the persisted hard state and all log entries in index order.
	Load() (HardState, []Entry, error)
	SaveHardState(hs HardState) error
	// Append adds entries directly after the current last entry.
	Append(entries []Entry) error
	// TruncateFrom removes the entry at index and everything after it.
	TruncateFrom(index uint64) error
	Sync() error
}
