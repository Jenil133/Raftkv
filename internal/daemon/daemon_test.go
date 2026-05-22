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

	"github.com/Jenil133/raftkv/proto/docpb"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/shard"
)

// gcluster is a 3-node cluster talking real gRPC over loopback with WALs on disk.
type gcluster struct {
	t     *testing.T
	dir   string
	addrs map[raft.NodeID]string
	nodes map[raft.NodeID]*Daemon

	shards    int
	snapEvery uint64
}

func newGRPCCluster(t *testing.T, n, shards int, snapEvery uint64) *gcluster {
	t.Helper()
	g := &gcluster{
		t: t, dir: t.TempDir(), shards: shards, snapEvery: snapEvery,
		addrs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*Daemon{},
	}
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
		Shards:             g.shards,
		SnapshotEvery:      g.snapEvery,
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

func (g *gcluster) conns() map[raft.NodeID]*grpc.ClientConn {
	out := map[raft.NodeID]*grpc.ClientConn{}
	for id, addr := range g.addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			g.t.Fatal(err)
		}
		g.t.Cleanup(func() { conn.Close() })
		out[id] = conn
	}
	return out
}

func (g *gcluster) client() *shard.Client {
	eps := map[raft.NodeID]kvpb.KVClient{}
	for id, conn := range g.conns() {
		eps[id] = kvpb.NewKVClient(conn)
	}
	cl := shard.NewClient(eps, g.shards)
	for _, s := range cl.Shards() {
		s.AttemptTimeout = time.Second
	}
	return cl
}

// docClient talks to the document service of one node.
func (g *gcluster) docClient(id raft.NodeID) docpb.DocsClient {
	return docpb.NewDocsClient(g.conns()[id])
}

func TestGRPCClusterEndToEnd(t *testing.T) {
	g := newGRPCCluster(t, 3, 1, 0)
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
		if d.Shard(0).Raft.Status().Role == raft.Leader {
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
		d := g.nodes[leader].Shard(0).KV.Store().Dump()
		if d["after-failover"] == "ok" && d["k7"] == "new" && len(d) == 20 { // 20 - k8 + after-failover
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: %v", d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestGRPCShardsSnapshotsAndDocs(t *testing.T) {
	g := newGRPCCluster(t, 3, 3, 15)
	cl := g.client()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Plain KV across shards.
	for i := 0; i < 90; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Document service on a node, which coordinates across shards itself.
	docs := g.docClient(1)
	put, err := docs.Put(ctx, &docpb.PutRequest{Collection: "users", Id: "ann", Json: []byte(`{"name":"Ann","n":1}`)})
	if err != nil || put.Code != docpb.Code_OK || put.Doc.Version != 1 {
		t.Fatalf("doc put: %+v %v", put, err)
	}
	patch, err := docs.Patch(ctx, &docpb.PatchRequest{Collection: "users", Id: "ann", Patch: []byte(`{"n":null,"city":"Oslo"}`)})
	if err != nil || patch.Code != docpb.Code_OK || patch.Doc.Version != 2 {
		t.Fatalf("doc patch: %+v %v", patch, err)
	}
	// Read through a different node.
	got, err := g.docClient(2).Get(ctx, &docpb.GetRequest{Collection: "users", Id: "ann"})
	if err != nil || got.Code != docpb.Code_OK || string(got.Doc.Json) != `{"city":"Oslo","name":"Ann"}` {
		t.Fatalf("doc get: %+v %v", got, err)
	}
	conflict, _ := docs.Put(ctx, &docpb.PutRequest{
		Collection: "users", Id: "ann", Json: []byte(`{}`), HasIfVersion: true, IfVersion: 1,
	})
	if conflict.Code != docpb.Code_VERSION_CONFLICT {
		t.Fatalf("expected version conflict, got %v", conflict.Code)
	}
	for i := 0; i < 10; i++ {
		docs.Put(ctx, &docpb.PutRequest{Collection: "pets", Id: fmt.Sprintf("p%02d", i), Json: []byte(`{"legs":4}`)})
	}
	scan, err := docs.Scan(ctx, &docpb.ScanRequest{Collection: "pets", Limit: 4})
	if err != nil || scan.Code != docpb.Code_OK || len(scan.Docs) != 4 || scan.Docs[0].Id != "p00" || scan.Docs[3].Id != "p03" {
		t.Fatalf("doc scan: %+v %v", scan, err)
	}
	if r, _ := docs.Get(ctx, &docpb.GetRequest{Collection: "users", Id: "ghost"}); r.Code != docpb.Code_NOT_FOUND {
		t.Fatalf("expected not found, got %v", r.Code)
	}

	// Snapshots must have kicked in on every shard of every node.
	for id, d := range g.nodes {
		for s := 0; s < 3; s++ {
			if d.Shard(s).Raft.Stats().SnapshotsTaken == 0 {
				t.Fatalf("node %d shard %d never snapshotted", id, s)
			}
		}
	}

	// Restart a node: it comes back from snapshot + WAL and serves everything.
	g.stop(2)
	g.restart(2)
	deadline := time.Now().Add(15 * time.Second)
	for {
		total := 0
		for s := 0; s < 3; s++ {
			total += len(g.nodes[2].Shard(s).KV.Store().Dump())
		}
		if total >= 90+1+10 { // 90 kv keys + users/ann + 10 pets
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node holds %d keys", total)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if r, err := g.docClient(2).Get(ctx, &docpb.GetRequest{Collection: "users", Id: "ann"}); err != nil || r.Code != docpb.Code_OK {
		t.Fatalf("doc get after restart: %+v %v", r, err)
	}
}
