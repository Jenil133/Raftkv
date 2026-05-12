package raft_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Jenil133/raftkv/internal/cluster"
	"github.com/Jenil133/raftkv/raft"
)

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestElectsSingleLeader(t *testing.T) {
	c := cluster.New(t, 5)
	leader := c.WaitLeader(3 * time.Second)

	// Leadership should be stable with no faults.
	term := c.Member(leader).Raft.Status().Term
	time.Sleep(500 * time.Millisecond)
	if got := c.Leaders(); len(got) != 1 || got[0] != leader {
		t.Fatalf("leaders changed: %v, want [%d]", got, leader)
	}
	if got := c.Member(leader).Raft.Status().Term; got != term {
		t.Fatalf("term moved %d -> %d without faults", term, got)
	}
	for _, id := range c.IDs {
		if st := c.Member(id).Raft.Status(); st.Leader != leader {
			t.Fatalf("node %d thinks leader is %d, want %d", id, st.Leader, leader)
		}
	}
}

func TestReelectionAfterLeaderCrash(t *testing.T) {
	c := cluster.New(t, 5)
	old := c.WaitLeader(3 * time.Second)
	oldTerm := c.Member(old).Raft.Status().Term

	c.Crash(old)
	var rest []raft.NodeID
	for _, id := range c.IDs {
		if id != old {
			rest = append(rest, id)
		}
	}
	next := c.WaitLeader(3*time.Second, rest...)
	if next == old {
		t.Fatalf("crashed node still leader")
	}
	if term := c.Member(next).Raft.Status().Term; term <= oldTerm {
		t.Fatalf("new term %d not above old term %d", term, oldTerm)
	}
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	c := cluster.New(t, 5)
	leader := c.WaitLeader(3 * time.Second)

	// Kill the leader and two more: 2 of 5 left, no quorum possible.
	killed := []raft.NodeID{leader}
	for _, id := range c.IDs {
		if len(killed) == 3 {
			break
		}
		if id != leader {
			killed = append(killed, id)
		}
	}
	for _, id := range killed {
		c.Crash(id)
	}
	time.Sleep(time.Second)
	if got := c.Leaders(); len(got) != 0 {
		t.Fatalf("leader %v elected without quorum", got)
	}

	// Restore quorum: a leader must emerge.
	c.Restart(killed[1])
	c.WaitLeader(5 * time.Second)
}

func TestReplicationAndAgreement(t *testing.T) {
	c := cluster.New(t, 5)
	c.WaitLeader(3 * time.Second)
	cl := c.Client()
	ctx := ctxTimeout(t, 20*time.Second)

	for i := 0; i < 50; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	c.Eventually(5*time.Second, "all replicas converge", func() bool {
		for _, id := range c.IDs {
			if len(c.Member(id).KV.Store().Dump()) != 50 {
				return false
			}
		}
		return true
	})
	want := c.Member(c.IDs[0]).KV.Store().Dump()
	for _, id := range c.IDs[1:] {
		got := c.Member(id).KV.Store().Dump()
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("node %d key %s = %q, want %q", id, k, got[k], v)
			}
		}
	}
}

func TestLogsIdenticalAcrossNodes(t *testing.T) {
	c := cluster.New(t, 3)
	c.WaitLeader(3 * time.Second)
	cl := c.Client()
	ctx := ctxTimeout(t, 10*time.Second)
	for i := 0; i < 20; i++ {
		if err := cl.Put(ctx, "k", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	c.Eventually(3*time.Second, "commit indexes equal", func() bool {
		first := c.Member(c.IDs[0]).Raft.Status()
		for _, id := range c.IDs[1:] {
			st := c.Member(id).Raft.Status()
			if st.CommitIndex != first.CommitIndex || st.LastApplied != first.LastApplied {
				return false
			}
		}
		return first.CommitIndex >= 21 // 20 puts + leader no-op
	})
}

func TestFollowerCatchesUpAfterRestart(t *testing.T) {
	c := cluster.New(t, 5)
	leader := c.WaitLeader(3 * time.Second)
	var follower raft.NodeID
	for _, id := range c.IDs {
		if id != leader {
			follower = id
			break
		}
	}
	c.Crash(follower)

	cl := c.Client()
	ctx := ctxTimeout(t, 20*time.Second)
	for i := 0; i < 30; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	c.Restart(follower)
	c.Eventually(5*time.Second, "restarted follower catches up", func() bool {
		return len(c.Member(follower).KV.Store().Dump()) == 30
	})
}

func TestPersistenceAcrossFullRestart(t *testing.T) {
	c := cluster.New(t, 3)
	c.WaitLeader(3 * time.Second)
	cl := c.Client()
	ctx := ctxTimeout(t, 20*time.Second)
	for i := 0; i < 10; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		c.Restart(id)
	}
	c.WaitLeader(5 * time.Second)
	for i := 0; i < 10; i++ {
		v, ok, err := cl.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil || !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("after restart k%d = %q ok=%v err=%v", i, v, ok, err)
		}
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := cluster.New(t, 1)
	c.WaitLeader(2 * time.Second)
	cl := c.Client()
	ctx := ctxTimeout(t, 5*time.Second)
	if err := cl.Put(ctx, "a", []byte("b")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := cl.Get(ctx, "a")
	if err != nil || !ok || string(v) != "b" {
		t.Fatalf("got %q %v %v", v, ok, err)
	}
}
