// Package grpctransport carries Raft RPCs over gRPC.
package grpctransport

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Jenil133/raftkv/proto/raftpb"
	"github.com/Jenil133/raftkv/raft"
)

// Transport is the client side: it dials peers lazily and reuses connections.
type Transport struct {
	addrs map[raft.NodeID]string

	mu      sync.Mutex
	conns   map[raft.NodeID]*grpc.ClientConn
	clients map[raft.NodeID]raftpb.RaftClient
}

// New returns a Transport that reaches each node at addrs[id].
func New(addrs map[raft.NodeID]string) *Transport {
	return &Transport{
		addrs:   addrs,
		conns:   make(map[raft.NodeID]*grpc.ClientConn),
		clients: make(map[raft.NodeID]raftpb.RaftClient),
	}
}

func (t *Transport) client(id raft.NodeID) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[id]; ok {
		return c, nil
	}
	addr, ok := t.addrs[id]
	if !ok {
		return nil, fmt.Errorf("grpctransport: unknown node %d", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := raftpb.NewRaftClient(conn)
	t.conns[id] = conn
	t.clients[id] = c
	return c, nil
}

// Close tears down all peer connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.conns {
		c.Close()
		delete(t.conns, id)
		delete(t.clients, id)
	}
}

// Group returns a raft.Transport that tags every RPC with a Raft group id so a
// single connection set can carry several groups (shards).
func (t *Transport) Group(group uint32) raft.Transport { return &groupTransport{t: t, group: group} }

// RequestVote, AppendEntries and InstallSnapshot make Transport itself the
// transport for group 0.
func (t *Transport) RequestVote(ctx context.Context, to raft.NodeID, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return t.requestVote(ctx, 0, to, a)
}

func (t *Transport) AppendEntries(ctx context.Context, to raft.NodeID, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return t.appendEntries(ctx, 0, to, a)
}

func (t *Transport) InstallSnapshot(ctx context.Context, to raft.NodeID, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return t.installSnapshot(ctx, 0, to, a)
}

type groupTransport struct {
	t     *Transport
	group uint32
}

func (g *groupTransport) RequestVote(ctx context.Context, to raft.NodeID, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return g.t.requestVote(ctx, g.group, to, a)
}

func (g *groupTransport) AppendEntries(ctx context.Context, to raft.NodeID, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return g.t.appendEntries(ctx, g.group, to, a)
}

func (g *groupTransport) InstallSnapshot(ctx context.Context, to raft.NodeID, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return g.t.installSnapshot(ctx, g.group, to, a)
}

func (t *Transport) requestVote(ctx context.Context, group uint32, to raft.NodeID, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	r, err := c.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         a.Term,
		CandidateId:  uint64(a.CandidateID),
		LastLogIndex: a.LastLogIndex,
		LastLogTerm:  a.LastLogTerm,
		PreVote:      a.PreVote,
		Group:        group,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: r.Term, VoteGranted: r.VoteGranted}, nil
}

func (t *Transport) appendEntries(ctx context.Context, group uint32, to raft.NodeID, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	req := &raftpb.AppendEntriesRequest{
		Group:        group,
		Term:         a.Term,
		LeaderId:     uint64(a.LeaderID),
		PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm:  a.PrevLogTerm,
		LeaderCommit: a.LeaderCommit,
		Entries:      make([]*raftpb.Entry, len(a.Entries)),
	}
	for i, e := range a.Entries {
		req.Entries[i] = &raftpb.Entry{Term: e.Term, Index: e.Index, Type: uint32(e.Type), Data: e.Data}
	}
	r, err := c.AppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          r.Term,
		Success:       r.Success,
		ConflictIndex: r.ConflictIndex,
		ConflictTerm:  r.ConflictTerm,
	}, nil
}

func (t *Transport) installSnapshot(ctx context.Context, group uint32, to raft.NodeID, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}
	r, err := c.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Group:             group,
		Term:              a.Term,
		LeaderId:          uint64(a.LeaderID),
		LastIncludedIndex: a.LastIncludedIndex,
		LastIncludedTerm:  a.LastIncludedTerm,
		Offset:            a.Offset,
		Data:              a.Data,
		Done:              a.Done,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: r.Term, NextOffset: r.NextOffset}, nil
}

// Router is the server side: it dispatches incoming Raft RPCs to the handler
// registered for the RPC's group.
type Router struct {
	raftpb.UnimplementedRaftServer
	mu       sync.RWMutex
	handlers map[uint32]raft.Handler
}

// Register creates a Router and exposes it as the Raft service on s.
func Register(s grpc.ServiceRegistrar) *Router {
	r := &Router{handlers: make(map[uint32]raft.Handler)}
	raftpb.RegisterRaftServer(s, r)
	return r
}

// Handle routes RPCs for group to h.
func (r *Router) Handle(group uint32, h raft.Handler) {
	r.mu.Lock()
	r.handlers[group] = h
	r.mu.Unlock()
}

func (r *Router) handler(group uint32) (raft.Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[group]
	if !ok {
		return nil, fmt.Errorf("grpctransport: no raft group %d on this node", group)
	}
	return h, nil
}

func (s *Router) RequestVote(_ context.Context, r *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	h, err := s.handler(r.Group)
	if err != nil {
		return nil, err
	}
	reply, err := h.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         r.Term,
		CandidateID:  raft.NodeID(r.CandidateId),
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
		PreVote:      r.PreVote,
	})
	if err != nil {
		return nil, err
	}
	return &raftpb.RequestVoteResponse{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *Router) AppendEntries(_ context.Context, r *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	h, err := s.handler(r.Group)
	if err != nil {
		return nil, err
	}
	args := &raft.AppendEntriesArgs{
		Term:         r.Term,
		LeaderID:     raft.NodeID(r.LeaderId),
		PrevLogIndex: r.PrevLogIndex,
		PrevLogTerm:  r.PrevLogTerm,
		LeaderCommit: r.LeaderCommit,
		Entries:      make([]raft.Entry, len(r.Entries)),
	}
	for i, e := range r.Entries {
		args.Entries[i] = raft.Entry{Term: e.Term, Index: e.Index, Type: raft.EntryType(e.Type), Data: e.Data}
	}
	reply, err := h.HandleAppendEntries(args)
	if err != nil {
		return nil, err
	}
	return &raftpb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

func (s *Router) InstallSnapshot(_ context.Context, r *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	h, err := s.handler(r.Group)
	if err != nil {
		return nil, err
	}
	reply, err := h.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              r.Term,
		LeaderID:          raft.NodeID(r.LeaderId),
		LastIncludedIndex: r.LastIncludedIndex,
		LastIncludedTerm:  r.LastIncludedTerm,
		Offset:            r.Offset,
		Data:              r.Data,
		Done:              r.Done,
	})
	if err != nil {
		return nil, err
	}
	return &raftpb.InstallSnapshotResponse{Term: reply.Term, NextOffset: reply.NextOffset}, nil
}

// ParseAddrs parses "1=host:port,2=host:port" into a node address map.
func ParseAddrs(s string) (map[raft.NodeID]string, error) {
	out := make(map[raft.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || v == "" {
			return nil, fmt.Errorf("bad address %q, want id=host:port", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(k), 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("bad node id in %q", part)
		}
		out[raft.NodeID(id)] = strings.TrimSpace(v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no addresses given")
	}
	return out, nil
}
