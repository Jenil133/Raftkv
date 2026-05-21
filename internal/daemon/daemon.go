// Package daemon assembles one production node: per-shard WAL storage, gRPC
// transport, Raft groups, KV servers, and the gRPC listener that serves the
// Raft, KV and document APIs.
package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Jenil133/raftkv/doc"
	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/docpb"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/shard"
	"github.com/Jenil133/raftkv/storage"
	"github.com/Jenil133/raftkv/transport/grpctransport"
)

// Options configures a Daemon.
type Options struct {
	ID       raft.NodeID
	Listener net.Listener // pre-bound listener; used instead of Listen if set
	Listen   string
	Peers    map[raft.NodeID]string // every node's address, including this one
	DataDir  string
	// Shards is the number of Raft groups; it must match on every node.
	Shards int
	// SnapshotEvery snapshots each shard after this many applied entries
	// (0 disables compaction).
	SnapshotEvery uint64
	// NoSync disables fsync (benchmarks/tests only).
	NoSync bool

	ElectionTimeoutMin time.Duration
	HeartbeatInterval  time.Duration
	Logger             *slog.Logger
}

// Daemon is a running node.
type Daemon struct {
	Host *shard.Host

	grpc      *grpc.Server
	transport *grpctransport.Transport
	wals      []*storage.WAL
	lis       net.Listener
	docConns  []*grpc.ClientConn
	serveDone chan struct{}
}

// Shard returns the replica of shard g on this node.
func (d *Daemon) Shard(g int) shard.Group { return d.Host.Groups[g] }

// Start boots the node and begins serving.
func Start(o Options) (*Daemon, error) {
	if _, ok := o.Peers[o.ID]; !ok {
		return nil, fmt.Errorf("daemon: node %d missing from peers", o.ID)
	}
	if o.Shards < 1 {
		o.Shards = 1
	}
	lis := o.Listener
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", o.Listen); err != nil {
			return nil, err
		}
	}

	d := &Daemon{lis: lis, serveDone: make(chan struct{})}
	fail := func(err error) (*Daemon, error) {
		for _, g := range d.groups() {
			g.Raft.Stop()
			g.KV.Wait()
		}
		for _, w := range d.wals {
			w.Close()
		}
		if d.transport != nil {
			d.transport.Close()
		}
		lis.Close()
		return nil, err
	}

	ids := make([]raft.NodeID, 0, len(o.Peers))
	for id := range o.Peers {
		ids = append(ids, id)
	}
	d.transport = grpctransport.New(o.Peers)
	d.grpc = grpc.NewServer()
	router := grpctransport.Register(d.grpc)

	groups := make([]shard.Group, o.Shards)
	for g := 0; g < o.Shards; g++ {
		wal, err := storage.OpenWAL(filepath.Join(o.DataDir, fmt.Sprintf("shard%d", g)))
		if err != nil {
			return fail(err)
		}
		wal.NoSync = o.NoSync
		d.wals = append(d.wals, wal)

		node, err := raft.NewNode(raft.Config{
			ID:                 o.ID,
			Peers:              ids,
			Transport:          d.transport.Group(uint32(g)),
			Storage:            wal,
			ElectionTimeoutMin: o.ElectionTimeoutMin,
			HeartbeatInterval:  o.HeartbeatInterval,
			Logger:             o.Logger,
		})
		if err != nil {
			return fail(err)
		}
		var kvOpts []kv.Option
		if o.SnapshotEvery > 0 {
			kvOpts = append(kvOpts, kv.WithSnapshotEvery(o.SnapshotEvery))
		}
		groups[g] = shard.Group{Raft: node, KV: kv.NewServer(node, kvOpts...)}
		router.Handle(uint32(g), node)
	}
	d.Host = shard.NewHost(groups)
	kvpb.RegisterKVServer(d.grpc, d.Host)

	// The document API coordinates across shards, whose leaders may be on
	// other nodes, so it talks to the cluster through an ordinary client.
	eps := make(map[raft.NodeID]kvpb.KVClient, len(o.Peers))
	for id, addr := range o.Peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fail(err)
		}
		d.docConns = append(d.docConns, conn)
		eps[id] = kvpb.NewKVClient(conn)
	}
	docpb.RegisterDocsServer(d.grpc, doc.NewServer(doc.NewStore(shard.NewClient(eps, o.Shards))))

	for _, g := range groups {
		g.Raft.Start()
	}
	go func() {
		defer close(d.serveDone)
		d.grpc.Serve(lis)
	}()
	return d, nil
}

func (d *Daemon) groups() []shard.Group {
	if d.Host == nil {
		return nil
	}
	return d.Host.Groups
}

// Addr is the address the node is listening on.
func (d *Daemon) Addr() string { return d.lis.Addr().String() }

// Stop shuts everything down in dependency order.
func (d *Daemon) Stop() {
	d.grpc.Stop()
	<-d.serveDone
	for _, g := range d.Host.Groups {
		g.Raft.Stop()
	}
	for _, g := range d.Host.Groups {
		g.KV.Wait()
	}
	for _, c := range d.docConns {
		c.Close()
	}
	d.transport.Close()
	for _, w := range d.wals {
		w.Close()
	}
}
