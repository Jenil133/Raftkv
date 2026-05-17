package raft

import "context"

// Transport sends Raft RPCs to peers. Implementations must be safe for
// concurrent use. A returned error means the RPC did not complete.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler receives Raft RPCs on behalf of a node. *Node implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
	HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Storage persists the hard state, the log and the latest snapshot. Mutating
// calls only need to be durable once Sync returns, except SaveSnapshot which is
// durable on return. The node calls Sync before acknowledging anything that
// depends on earlier calls.
type Storage interface {
	// Load returns the persisted state. Entries start right after the snapshot.
	Load() (State, error)
	SaveHardState(hs HardState) error
	// Append adds entries directly after the current last entry (or after the
	// snapshot if the log is empty).
	Append(entries []Entry) error
	// TruncateFrom removes the entry at index and everything after it.
	TruncateFrom(index uint64) error
	// SaveSnapshot durably stores snap and discards log entries up to and
	// including snap.Index. If the log ends before snap.Index it becomes empty
	// and the next Append starts at snap.Index+1.
	SaveSnapshot(snap Snapshot) error
	Sync() error
}
