// Package daemon assembles one production node: per-shard WAL storage, gRPC
// transport, Raft groups, KV servers, and the gRPC listener that serves the
// Raft, KV and document APIs.
package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Jenil133/raftkv/doc"
	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/metrics"
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
	// MetricsAddr, if set, serves /metrics, /healthz and /readyz over HTTP.
	MetricsAddr     string
	MetricsListener net.Listener // pre-bound alternative to MetricsAddr

	ElectionTimeoutMin time.Duration
	HeartbeatInterval  time.Duration
	Logger             *slog.Logger
}

// Daemon is a running node.
type Daemon struct {
	Host    *shard.Host
	Metrics *metrics.Registry

	grpc      *grpc.Server
	transport *grpctransport.Transport
	wals      []*storage.WAL
	lis       net.Listener
	docConns  []*grpc.ClientConn
	serveDone chan struct{}

	httpSrv *http.Server
	httpLis net.Listener
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

	d := &Daemon{lis: lis, serveDone: make(chan struct{}), Metrics: metrics.New(o.ID)}
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
		wal.SyncObserver = d.Metrics.FsyncObserver(g)
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
		kvOpts := []kv.Option{kv.WithObserver(d.Metrics.KVObserver(g))}
		if o.SnapshotEvery > 0 {
			kvOpts = append(kvOpts, kv.WithSnapshotEvery(o.SnapshotEvery))
		}
		groups[g] = shard.Group{Raft: node, KV: kv.NewServer(node, kvOpts...)}
		router.Handle(uint32(g), node)
		d.Metrics.AddRaft(g, node)
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

	if o.MetricsListener != nil || o.MetricsAddr != "" {
		hl := o.MetricsListener
		if hl == nil {
			var err error
			if hl, err = net.Listen("tcp", o.MetricsAddr); err != nil {
				return fail(err)
			}
		}
		d.httpLis = hl
		d.httpSrv = &http.Server{Handler: d.httpHandler(), ReadHeaderTimeout: 5 * time.Second}
		go d.httpSrv.Serve(hl)
	}

	for _, g := range groups {
		g.Raft.Start()
	}
	go func() {
		defer close(d.serveDone)
		d.grpc.Serve(lis)
	}()
	return d, nil
}

func (d *Daemon) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", d.Metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	// Ready once every shard knows its leader, i.e. the node can serve or
	// redirect any request.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		for i, g := range d.Host.Groups {
			if g.Raft.Status().Leader == 0 {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "shard %d has no leader\n", i)
				return
			}
		}
		fmt.Fprintln(w, "ready")
	})
	return mux
}

// MetricsAddr is the address of the HTTP metrics server, if running.
func (d *Daemon) MetricsAddr() string {
	if d.httpLis == nil {
		return ""
	}
	return d.httpLis.Addr().String()
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
	if d.httpSrv != nil {
		d.httpSrv.Close()
	}
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
