// Package daemon assembles one production node: WAL storage, gRPC transport,
// Raft node, KV server, and the gRPC listener that serves both.
package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
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
	// NoSync disables fsync (benchmarks/tests only).
	NoSync bool

	ElectionTimeoutMin time.Duration
	HeartbeatInterval  time.Duration
	Logger             *slog.Logger
}

// Daemon is a running node.
type Daemon struct {
	Raft *raft.Node
	KV   *kv.Server

	grpc      *grpc.Server
	transport *grpctransport.Transport
	wal       *storage.WAL
	lis       net.Listener
	serveDone chan struct{}
}

// Start boots the node and begins serving.
func Start(o Options) (*Daemon, error) {
	if _, ok := o.Peers[o.ID]; !ok {
		return nil, fmt.Errorf("daemon: node %d missing from peers", o.ID)
	}
	lis := o.Listener
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", o.Listen); err != nil {
			return nil, err
		}
	}
	wal, err := storage.OpenWAL(o.DataDir)
	if err != nil {
		lis.Close()
		return nil, err
	}
	wal.NoSync = o.NoSync

	ids := make([]raft.NodeID, 0, len(o.Peers))
	for id := range o.Peers {
		ids = append(ids, id)
	}
	tr := grpctransport.New(o.Peers)
	node, err := raft.NewNode(raft.Config{
		ID:                 o.ID,
		Peers:              ids,
		Transport:          tr,
		Storage:            wal,
		ElectionTimeoutMin: o.ElectionTimeoutMin,
		HeartbeatInterval:  o.HeartbeatInterval,
		Logger:             o.Logger,
	})
	if err != nil {
		wal.Close()
		lis.Close()
		return nil, err
	}

	srv := kv.NewServer(node)
	gs := grpc.NewServer()
	grpctransport.Register(gs, node)
	kvpb.RegisterKVServer(gs, srv)

	d := &Daemon{
		Raft: node, KV: srv, grpc: gs, transport: tr, wal: wal, lis: lis,
		serveDone: make(chan struct{}),
	}
	node.Start()
	go func() {
		defer close(d.serveDone)
		gs.Serve(lis)
	}()
	return d, nil
}

// Addr is the address the node is listening on.
func (d *Daemon) Addr() string { return d.lis.Addr().String() }

// Stop shuts everything down in dependency order.
func (d *Daemon) Stop() {
	d.grpc.Stop()
	<-d.serveDone
	d.Raft.Stop()
	d.KV.Wait()
	d.transport.Close()
	d.wal.Close()
}
