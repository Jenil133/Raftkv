package daemon

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
)

// gcluster is a 3-node cluster talking real gRPC over loopback with WALs on disk.
type gcluster struct {
	t     *testing.T
	dir   string
	addrs map[raft.NodeID]string
	nodes map[raft.NodeID]*Daemon
}

func newGRPCCluster(t *testing.T, n int) *gcluster {
	t.Helper()
	g := &gcluster{t: t, dir: t.TempDir(), addrs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*Daemon{}}
	lis := map[raft.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := raft.NodeID(i)
		lis[id] = l
		g.addrs[id] = l.Addr().String()
	}
	for id, l := range lis {
		g.start(id, l)
	}
	t.Cleanup(func() {
		for id := range g.nodes {
			g.stop(id)
		}
	})
	return g
}

func (g *gcluster) start(id raft.NodeID, l net.Listener) {
	d, err := Start(Options{
		ID:                 id,
		Listener:           l,
		Listen:             g.addrs[id],
		Peers:              g.addrs,
		DataDir:            filepath.Join(g.dir, fmt.Sprintf("node%d", id)),
		NoSync:             true,
		ElectionTimeoutMin: 150 * time.Millisecond,
		HeartbeatInterval:  25 * time.Millisecond,
	})
	if err != nil {
		g.t.Fatal(err)
	}
	g.nodes[id] = d
}

func (g *gcluster) stop(id raft.NodeID) {
	if d := g.nodes[id]; d != nil {
		d.Stop()
		delete(g.nodes, id)
	}
}

func (g *gcluster) restart(id raft.NodeID) {
	l, err := net.Listen("tcp", g.addrs[id])
	if err != nil {
		g.t.Fatal(err)
	}
	g.start(id, l)
}

func (g *gcluster) client() *kv.Client {
	eps := map[raft.NodeID]kvpb.KVClient{}
	for id, addr := range g.addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			g.t.Fatal(err)
		}
		g.t.Cleanup(func() { conn.Close() })
		eps[id] = kvpb.NewKVClient(conn)
	}
	cl := kv.NewClient(eps)
	cl.AttemptTimeout = time.Second
	return cl
}

func TestGRPCClusterEndToEnd(t *testing.T) {
	g := newGRPCCluster(t, 3)
	cl := g.client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 20; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	v, ok, err := cl.Get(ctx, "k7")
	if err != nil || !ok || string(v) != "v7" {
		t.Fatalf("get k7 = %q %v %v", v, ok, err)
	}
	swapped, _, err := cl.CAS(ctx, "k7", []byte("v7"), false, []byte("new"))
	if err != nil || !swapped {
		t.Fatalf("cas: %v %v", swapped, err)
	}
	if existed, err := cl.Delete(ctx, "k8"); err != nil || !existed {
		t.Fatalf("delete: %v %v", existed, err)
	}
	if _, ok, _ := cl.Get(ctx, "k8"); ok {
		t.Fatal("k8 still present")
	}

	// Kill the leader node; the cluster keeps serving.
	var leader raft.NodeID
	for id, d := range g.nodes {
		if d.Raft.Status().Role == raft.Leader {
			leader = id
		}
	}
	g.stop(leader)
	if err := cl.Put(ctx, "after-failover", []byte("ok")); err != nil {
		t.Fatalf("put after failover: %v", err)
	}

	// Restart it from its WAL; it must catch up and see everything.
	g.restart(leader)
	deadline := time.Now().Add(10 * time.Second)
	for {
		d := g.nodes[leader].KV.Store().Dump()
		if d["after-failover"] == "ok" && d["k7"] == "new" && len(d) == 20 { // 20 - k8 + after-failover
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: %v", d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
