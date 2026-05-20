package raft_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/raftkv/internal/cluster"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/storage"
)

func putN(t *testing.T, c *cluster.Cluster, prefix string, from, to int) {
	t.Helper()
	cl := c.Client()
	ctx := ctxTimeout(t, 60*time.Second)
	for i := from; i < to; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("%s%d", prefix, i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
}

func allKeys(c *cluster.Cluster, id raft.NodeID, prefix string, n int) bool {
	srv := c.Server(id, 0)
	if srv == nil {
		return false
	}
	d := srv.Store().Dump()
	for i := 0; i < n; i++ {
		if d[fmt.Sprintf("%s%d", prefix, i)] != fmt.Sprintf("v%d", i) {
			return false
		}
	}
	return true
}

func TestLogIsCompacted(t *testing.T) {
	c := cluster.New(t, 3, cluster.WithSnapshotEvery(20))
	c.WaitLeader(3 * time.Second)
	putN(t, c, "k", 0, 300)

	c.Eventually(5*time.Second, "all nodes compact their logs", func() bool {
		for _, id := range c.IDs {
			st := c.Node(id, 0).Status()
			if st.SnapshotIndex == 0 || st.LogLength > 60 {
				return false
			}
		}
		return true
	})
	for _, id := range c.IDs {
		if got := c.Node(id, 0).Stats().SnapshotsTaken; got == 0 {
			t.Fatalf("node %d took no snapshots", id)
		}
		if n := c.Member(id).Storages[0].(*storage.Memory).LogLen(); n > 60 {
			t.Fatalf("node %d storage still holds %d entries", id, n)
		}
	}
}

func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := cluster.New(t, 5, cluster.WithSnapshotEvery(25))
	leader := c.WaitLeader(3 * time.Second)
	var lagger raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			lagger = id
			break
		}
	}
	putN(t, c, "a", 0, 20)
	c.Crash(lagger)

	// Far more writes than the snapshot interval: the leader compacts away the
	// entries the lagger is missing.
	putN(t, c, "b", 0, 200)
	c.Eventually(5*time.Second, "leader compacted past the lagger", func() bool {
		l := c.WaitLeader(time.Second, otherThan(c, lagger)...)
		return c.Node(l, 0).Status().SnapshotIndex > 30
	})

	c.Restart(lagger)
	c.Eventually(10*time.Second, "lagger catches up", func() bool {
		return allKeys(c, lagger, "a", 20) && allKeys(c, lagger, "b", 200)
	})

	var sent uint64
	for _, id := range c.IDs {
		if n := c.Node(id, 0); n != nil {
			sent += n.Stats().SnapshotsSent
		}
	}
	if sent == 0 {
		t.Fatal("lagger caught up without any InstallSnapshot being sent")
	}
	if got := c.Node(lagger, 0).Stats().SnapshotsInstalled; got == 0 {
		t.Fatal("lagger never installed a snapshot")
	}
}

func otherThan(c *cluster.Cluster, skip raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range c.IDs {
		if id != skip {
			out = append(out, id)
		}
	}
	return out
}

func TestSnapshotInstallIsChunked(t *testing.T) {
	// Tiny chunks force the multi-RPC path; values are fat so the snapshot is
	// much bigger than one chunk.
	c := cluster.New(t, 3,
		cluster.WithSnapshotEvery(10),
		cluster.WithRaftConfig(func(cfg *raft.Config) { cfg.SnapshotChunkSize = 64 }),
	)
	leader := c.WaitLeader(3 * time.Second)
	lagger := otherThan(c, leader)[0]
	c.Crash(lagger)

	cl := c.Client()
	ctx := ctxTimeout(t, 60*time.Second)
	fat := []byte(strings.Repeat("x", 200))
	for i := 0; i < 60; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), fat); err != nil {
			t.Fatal(err)
		}
	}
	c.Eventually(5*time.Second, "compaction past lagger", func() bool {
		l := c.WaitLeader(time.Second, otherThan(c, lagger)...)
		return c.Node(l, 0).Status().SnapshotIndex > 10
	})
	c.Restart(lagger)
	c.Eventually(15*time.Second, "chunked snapshot installed", func() bool {
		srv := c.Server(lagger, 0)
		return srv != nil && len(srv.Store().Dump()) == 60
	})
	if c.Node(lagger, 0).Stats().SnapshotsInstalled == 0 {
		t.Fatal("expected a snapshot install")
	}
}

func TestRestartRestoresFromSnapshotNotFullLog(t *testing.T) {
	c := cluster.New(t, 3, cluster.WithSnapshotEvery(15))
	c.WaitLeader(3 * time.Second)
	putN(t, c, "k", 0, 120)
	c.Eventually(5*time.Second, "snapshots taken", func() bool {
		for _, id := range c.IDs {
			if c.Node(id, 0).Status().SnapshotIndex == 0 {
				return false
			}
		}
		return true
	})
	snapIdx := c.Node(c.IDs[0], 0).Status().SnapshotIndex

	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	c.WaitLeader(5 * time.Second)
	for _, id := range c.IDs {
		st := c.Node(id, 0).Status()
		if st.SnapshotIndex == 0 {
			t.Fatalf("node %d restarted without its snapshot", id)
		}
		if st.LogLength > 80 {
			t.Fatalf("node %d replays %d entries after restart (snapshot at %d)", id, st.LogLength, snapIdx)
		}
	}
	c.Eventually(5*time.Second, "state restored on every node", func() bool {
		for _, id := range c.IDs {
			if !allKeys(c, id, "k", 120) {
				return false
			}
		}
		return true
	})
	// Still writable and consistent after restart.
	putN(t, c, "after", 0, 10)
}

func TestExactlyOnceSurvivesSnapshot(t *testing.T) {
	c := cluster.New(t, 3, cluster.WithSnapshotEvery(5))
	c.WaitLeader(3 * time.Second)
	cl := c.Client()
	ctx := ctxTimeout(t, 30*time.Second)
	// A CAS that must apply exactly once even though sessions get snapshotted
	// and restored across restarts.
	if swapped, _, err := cl.CAS(ctx, "ctr", nil, true, []byte("1")); err != nil || !swapped {
		t.Fatalf("cas: %v %v", swapped, err)
	}
	putN(t, c, "filler", 0, 50)
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	c.WaitLeader(5 * time.Second)
	v, ok, err := cl.Get(ctx, "ctr")
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("ctr = %q %v %v", v, ok, err)
	}
}

func TestSnapshotsUnderChurn(t *testing.T) {
	c := cluster.New(t, 5, cluster.WithSnapshotEvery(10))
	c.WaitLeader(3 * time.Second)
	done := make(chan struct{})
	errc := make(chan error, 1)
	go func() {
		defer close(done)
		cl := c.Client()
		ctx := ctxTimeout(t, 90*time.Second)
		for i := 0; i < 150; i++ {
			if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
				errc <- err
				return
			}
		}
	}()
	// Crash and revive nodes round-robin while writes stream in.
	for i := 0; ; i++ {
		select {
		case <-done:
			goto finished
		case <-time.After(120 * time.Millisecond):
		}
		id := c.IDs[i%len(c.IDs)]
		c.Crash(id)
		time.Sleep(150 * time.Millisecond)
		c.Restart(id)
	}
finished:
	select {
	case err := <-errc:
		t.Fatal(err)
	default:
	}
	c.Eventually(15*time.Second, "every node converges", func() bool {
		for _, id := range c.IDs {
			if !allKeys(c, id, "k", 150) {
				return false
			}
		}
		return true
	})
}
