// Package shard spreads the key space over several independent Raft groups.
// Every node hosts one replica of every group; each group elects its own
// leader, so write load spreads across the cluster.
package shard

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/Jenil133/raftkv/kv"
	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
)

// Map assigns keys to shards by hash. N must be the same on every node and
// client; it is fixed for the life of a cluster.
type Map struct{ N int }

// For returns the shard owning key.
func (m Map) For(key string) int {
	if m.N <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(m.N))
}

// Group is one shard's replica on this node.
type Group struct {
	Raft *raft.Node
	KV   *kv.Server
}

// Host is a node's set of shard replicas. It implements kvpb.KVServer by
// routing each request to the replica owning the key.
type Host struct {
	kvpb.UnimplementedKVServer
	Map    Map
	Groups []Group // indexed by shard
}

// NewHost wraps the per-shard groups (len(groups) is the shard count).
func NewHost(groups []Group) *Host {
	return &Host{Map: Map{N: len(groups)}, Groups: groups}
}

func (h *Host) group(key string) *kv.Server { return h.Groups[h.Map.For(key)].KV }

func (h *Host) Put(ctx context.Context, r *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	return h.group(r.Key).Put(ctx, r)
}

func (h *Host) Get(ctx context.Context, r *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	return h.group(r.Key).Get(ctx, r)
}

func (h *Host) Delete(ctx context.Context, r *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	return h.group(r.Key).Delete(ctx, r)
}

func (h *Host) CAS(ctx context.Context, r *kvpb.CASRequest) (*kvpb.CASResponse, error) {
	return h.group(r.Key).CAS(ctx, r)
}

func (h *Host) Scan(ctx context.Context, r *kvpb.ScanRequest) (*kvpb.ScanResponse, error) {
	if int(r.Shard) >= len(h.Groups) {
		return &kvpb.ScanResponse{Status: kvpb.Status_ERROR, Error: fmt.Sprintf("no shard %d", r.Shard)}, nil
	}
	return h.Groups[r.Shard].KV.Scan(ctx, r)
}

// Client routes operations to the right shard and keeps one leader cache per
// shard. Like kv.Client, a Client runs one write at a time per shard.
type Client struct {
	m      Map
	shards []*kv.Client
}

// NewClient returns a client for a cluster running shards shards, reachable
// through the given node endpoints (each endpoint must be a Host or gRPC
// connection to one).
func NewClient(endpoints map[raft.NodeID]kvpb.KVClient, shards int) *Client {
	if shards < 1 {
		shards = 1
	}
	c := &Client{m: Map{N: shards}, shards: make([]*kv.Client, shards)}
	for i := range c.shards {
		c.shards[i] = kv.NewClient(endpoints)
		c.shards[i].Shard = uint32(i)
	}
	return c
}

// Shards exposes the per-shard clients, e.g. to tune timeouts.
func (c *Client) Shards() []*kv.Client { return c.shards }

// ShardFor returns the shard index owning key.
func (c *Client) ShardFor(key string) int { return c.m.For(key) }

func (c *Client) forKey(key string) *kv.Client { return c.shards[c.m.For(key)] }

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	return c.forKey(key).Put(ctx, key, value)
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.forKey(key).Get(ctx, key)
}

func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	return c.forKey(key).Delete(ctx, key)
}

func (c *Client) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error) {
	return c.forKey(key).CAS(ctx, key, expected, expectAbsent, value)
}

func (c *Client) CASDelete(ctx context.Context, key string, expected []byte) (bool, []byte, error) {
	return c.forKey(key).CASDelete(ctx, key, expected)
}

// Scan merges a prefix scan across every shard: up to limit pairs with key >
// after, in global key order.
func (c *Client) Scan(ctx context.Context, prefix, after string, limit int) ([]*kvpb.Pair, error) {
	type res struct {
		pairs []*kvpb.Pair
		err   error
	}
	out := make([]res, len(c.shards))
	done := make(chan int, len(c.shards))
	for i, sc := range c.shards {
		go func(i int, sc *kv.Client) {
			// Each shard may hold all of the first `limit` keys, so ask for that many.
			out[i].pairs, out[i].err = sc.Scan(ctx, prefix, after, limit)
			done <- i
		}(i, sc)
	}
	for range c.shards {
		<-done
	}
	var all []*kvpb.Pair
	for _, r := range out {
		if r.err != nil {
			return nil, r.err
		}
		all = append(all, r.pairs...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}
