package shard_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/Jenil133/raftkv/internal/cluster"
	"github.com/Jenil133/raftkv/raft"
	"github.com/Jenil133/raftkv/shard"
)

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestMapIsDeterministicAndBalanced(t *testing.T) {
	m := shard.Map{N: 4}
	counts := make([]int, 4)
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("user:%d", i)
		s := m.For(k)
		if s != m.For(k) {
			t.Fatal("non-deterministic")
		}
		counts[s]++
	}
	for s, c := range counts {
		if c < 4000 || c > 6000 { // ideal 5000
			t.Fatalf("shard %d got %d of 20000 keys: %v", s, c, counts)
		}
	}
	if (shard.Map{N: 1}).For("x") != 0 || (shard.Map{}).For("x") != 0 {
		t.Fatal("single shard must always be 0")
	}
}

func TestKeysLandOnlyOnTheirShard(t *testing.T) {
	c := cluster.New(t, 3, cluster.WithShards(4))
	c.WaitAllLeaders(5 * time.Second)
	cl := c.ShardClient()
	ctx := ctxTimeout(t, 60*time.Second)

	const n = 200
	for i := 0; i < n; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	m := shard.Map{N: 4}
	c.Eventually(10*time.Second, "replicas converge per shard", func() bool {
		total := 0
		for g := 0; g < 4; g++ {
			var first map[string]string
			for _, id := range c.IDs {
				d := c.Server(id, g).Store().Dump()
				for k := range d {
					if m.For(k) != g {
						t.Fatalf("key %s found on shard %d, belongs to %d", k, g, m.For(k))
					}
				}
				if first == nil {
					first = d
				} else if len(d) != len(first) {
					return false
				}
			}
			total += len(first)
		}
		return total == n
	})
	for g := 0; g < 4; g++ {
		if len(c.Server(c.IDs[0], g).Store().Dump()) == 0 {
			t.Fatalf("shard %d got no keys", g)
		}
	}
	for i := 0; i < n; i++ {
		v, ok, err := cl.Get(ctx, fmt.Sprintf("key%d", i))
		if err != nil || !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get key%d = %q %v %v", i, v, ok, err)
		}
	}
}

func TestShardsFailOverIndependently(t *testing.T) {
	c := cluster.New(t, 5, cluster.WithShards(3))
	c.WaitAllLeaders(5 * time.Second)
	cl := c.ShardClient()
	ctx := ctxTimeout(t, 60*time.Second)

	// Kill whoever leads shard 0; every shard must keep serving afterwards.
	victim := c.WaitLeaderOf(0, time.Second)
	c.Crash(victim)
	var up []raft.NodeID
	for _, id := range c.IDs {
		if id != victim {
			up = append(up, id)
		}
	}
	for g := 0; g < 3; g++ {
		c.WaitLeaderOf(g, 5*time.Second, up...)
	}
	for i := 0; i < 60; i++ {
		if err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			t.Fatalf("put %d with node %d down: %v", i, victim, err)
		}
	}
	c.Restart(victim)
	c.Eventually(10*time.Second, "restarted node catches up on all shards", func() bool {
		total := 0
		for g := 0; g < 3; g++ {
			total += len(c.Server(victim, g).Store().Dump())
		}
		return total == 60
	})
}

func TestScanMergesShardsInOrderAcrossPages(t *testing.T) {
	c := cluster.New(t, 3, cluster.WithShards(4))
	c.WaitAllLeaders(5 * time.Second)
	cl := c.ShardClient()
	ctx := ctxTimeout(t, 60*time.Second)

	var want []string
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("item/%03d", i)
		want = append(want, k)
		if err := cl.Put(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	// Noise outside the prefix.
	for i := 0; i < 20; i++ {
		cl.Put(ctx, fmt.Sprintf("other/%d", i), []byte("y"))
	}
	sort.Strings(want)

	var got []string
	after := ""
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("pagination does not terminate")
		}
		pairs, err := cl.Scan(ctx, "item/", after, 30)
		if err != nil {
			t.Fatal(err)
		}
		if len(pairs) == 0 {
			break
		}
		if len(pairs) > 30 {
			t.Fatalf("page of %d exceeds limit", len(pairs))
		}
		for _, p := range pairs {
			got = append(got, p.Key)
		}
		after = pairs[len(pairs)-1].Key
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("scan order/content mismatch:\n got %v\nwant %v", got[:min(5, len(got))], want[:5])
	}
}

func TestShardsWithSnapshots(t *testing.T) {
	c := cluster.New(t, 3, cluster.WithShards(3), cluster.WithSnapshotEvery(10))
	c.WaitAllLeaders(5 * time.Second)
	cl := c.ShardClient()
	ctx := ctxTimeout(t, 60*time.Second)
	for i := 0; i < 150; i++ {
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
	c.WaitAllLeaders(5 * time.Second)
	for i := 0; i < 150; i++ {
		v, ok, err := cl.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil || !ok || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("k%d = %q %v %v", i, v, ok, err)
		}
	}
}
