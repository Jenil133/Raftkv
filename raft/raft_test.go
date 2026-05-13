package raft_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Jenil133/raftkv/internal/cluster"
	"github.com/Jenil133/raftkv/proto/kvpb"
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

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := cluster.New(t, 5)
	old := c.WaitLeader(3 * time.Second)
	cl := c.Client()
	ctx := ctxTimeout(t, 30*time.Second)
	if err := cl.Put(ctx, "before", []byte("1")); err != nil {
		t.Fatal(err)
	}

	// Strand the leader with one follower (2 of 5).
	var buddy raft.NodeID
	var majority []raft.NodeID
	for _, id := range c.IDs {
		switch {
		case id == old:
		case buddy == 0:
			buddy = id
		default:
			majority = append(majority, id)
		}
	}
	c.Net.Partition([]raft.NodeID{old, buddy}, majority)

	// The stranded leader accepts a proposal but can never commit it.
	oldNode := c.Member(old).Raft
	junk, err := proto.Marshal(&kvpb.Command{Op: kvpb.Op_PUT, Key: "junk", Value: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	idx, _, proposed := oldNode.Propose(junk)

	// Majority elects its own leader and makes progress.
	c.WaitLeader(5*time.Second, majority...)
	mc := c.Client()
	if err := mc.Put(ctx, "during", []byte("2")); err != nil {
		t.Fatalf("majority could not commit: %v", err)
	}
	if st := oldNode.Status(); proposed && st.CommitIndex >= idx {
		t.Fatalf("stranded leader committed index %d in a minority (commit=%d)", idx, st.CommitIndex)
	}

	// Heal: old leader steps down, its uncommitted junk is discarded.
	c.Net.Heal()
	c.Eventually(5*time.Second, "single leader after heal", func() bool {
		return len(c.Leaders()) == 1
	})
	c.Eventually(5*time.Second, "all nodes converge", func() bool {
		for _, id := range c.IDs {
			d := c.Member(id).KV.Store().Dump()
			if d["before"] != "1" || d["during"] != "2" {
				return false
			}
			if _, ok := d["junk"]; ok {
				t.Errorf("node %d applied uncommitted entry", id)
			}
		}
		return true
	})
}

func TestLinearizableReadSeesLatestWrite(t *testing.T) {
	c := cluster.New(t, 5)
	c.WaitLeader(3 * time.Second)
	writer, reader := c.Client(), c.Client()
	ctx := ctxTimeout(t, 20*time.Second)
	for i := 0; i < 30; i++ {
		val := []byte(fmt.Sprintf("%d", i))
		if err := writer.Put(ctx, "x", val); err != nil {
			t.Fatal(err)
		}
		got, ok, err := reader.Get(ctx, "x")
		if err != nil || !ok || string(got) != string(val) {
			t.Fatalf("iter %d: read %q ok=%v err=%v, want %q", i, got, ok, err, val)
		}
	}
}

func TestStaleLeaderCannotServeReads(t *testing.T) {
	c := cluster.New(t, 5)
	old := c.WaitLeader(3 * time.Second)
	c.Net.Isolate(old)
	// Isolated old leader must fail ReadIndex rather than return stale data.
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	if _, err := c.Member(old).Raft.ReadIndex(ctx); err == nil {
		t.Fatal("isolated leader served a read index")
	}
}

func TestConcurrentClientsWithLeaderChurn(t *testing.T) {
	c := cluster.New(t, 5)
	c.WaitLeader(3 * time.Second)
	ctx := ctxTimeout(t, 60*time.Second)

	const clients, perClient = 6, 25
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			cl := c.Client()
			for i := 0; i < perClient; i++ {
				key := fmt.Sprintf("c%d-%d", ci, i)
				if err := cl.Put(ctx, key, []byte(key)); err != nil {
					errs <- fmt.Errorf("client %d put %d: %w", ci, i, err)
					return
				}
			}
		}(ci)
	}

	// Meanwhile kill and revive the leader a couple of times.
	for round := 0; round < 2; round++ {
		time.Sleep(150 * time.Millisecond)
		if ls := c.Leaders(); len(ls) > 0 {
			c.Crash(ls[0])
			time.Sleep(300 * time.Millisecond)
			c.Restart(ls[0])
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	total := clients * perClient
	c.Eventually(10*time.Second, "every replica has every key", func() bool {
		for _, id := range c.IDs {
			if len(c.Member(id).KV.Store().Dump()) != total {
				return false
			}
		}
		return true
	})
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

func TestFlakyNetworkStillConverges(t *testing.T) {
	c := cluster.New(t, 5)
	c.WaitLeader(3 * time.Second)
	c.Net.SetDropRate(0.2)
	c.Net.SetDelay(0, 10*time.Millisecond)
	cl := c.Client()
	ctx := ctxTimeout(t, 60*time.Second)
	for i := 0; i < 30; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	c.Net.SetDropRate(0)
	c.Eventually(10*time.Second, "converge", func() bool {
		for _, id := range c.IDs {
			if len(c.Member(id).KV.Store().Dump()) != 30 {
				return false
			}
		}
		return true
	})
}
