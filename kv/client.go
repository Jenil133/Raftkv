package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Jenil133/raftkv/proto/kvpb"
	"github.com/Jenil133/raftkv/raft"
)

// Client talks to a cluster, following leader hints and retrying. Writes carry
// a (client id, sequence) pair that the state machine uses to apply each write
// exactly once even when a retry races with the original. A Client runs one
// write at a time; use several Clients for concurrency.
type Client struct {
	endpoints map[raft.NodeID]kvpb.KVClient
	order     []raft.NodeID
	id        uint64
	// AttemptTimeout bounds a single RPC attempt.
	AttemptTimeout time.Duration

	writeMu sync.Mutex // serialises writes so seq order matches issue order
	mu      sync.Mutex
	seq     uint64
	leader  raft.NodeID
	next    int
}

// NewClient returns a client over the given node endpoints.
func NewClient(endpoints map[raft.NodeID]kvpb.KVClient) *Client {
	if len(endpoints) == 0 {
		panic("kv: NewClient needs at least one endpoint")
	}
	order := make([]raft.NodeID, 0, len(endpoints))
	for id := range endpoints {
		order = append(order, id)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	id := binary.LittleEndian.Uint64(b[:])
	if id == 0 {
		id = 1
	}
	return &Client{
		endpoints:      endpoints,
		order:          order,
		id:             id,
		AttemptTimeout: 2 * time.Second,
	}
}

// ID is this client's session identifier.
func (c *Client) ID() uint64 { return c.id }

func (c *Client) nextSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return c.seq
}

// target picks the node to try next.
func (c *Client) target() raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != 0 {
		return c.leader
	}
	id := c.order[c.next%len(c.order)]
	c.next++
	return id
}

func (c *Client) setLeader(id raft.NodeID) {
	c.mu.Lock()
	c.leader = id
	c.mu.Unlock()
}

// outcome is what one RPC attempt reports back to the retry loop.
type outcome struct {
	status kvpb.Status
	hint   uint64
	errMsg string
}

// do retries call until it succeeds, ctx ends, or a non-retryable error.
func (c *Client) do(ctx context.Context, call func(ctx context.Context, ep kvpb.KVClient) (outcome, error)) error {
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("%w (last error: %v)", err, lastErr)
			}
			return err
		}
		id := c.target()
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		out, err := call(actx, c.endpoints[id])
		cancel()
		switch {
		case err != nil:
			lastErr = err
			c.setLeader(0)
		case out.status == kvpb.Status_OK:
			c.setLeader(id)
			return nil
		case out.status == kvpb.Status_NOT_LEADER:
			lastErr = &NotLeaderError{Hint: raft.NodeID(out.hint)}
			if _, ok := c.endpoints[raft.NodeID(out.hint)]; ok && out.hint != 0 {
				c.setLeader(raft.NodeID(out.hint))
			} else {
				c.setLeader(0)
			}
		default:
			lastErr = fmt.Errorf("node %d: %s: %s", id, out.status, out.errMsg)
			c.setLeader(0)
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Put stores value under key.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	req := &kvpb.PutRequest{Key: key, Value: value, ClientId: c.id, Seq: c.nextSeq()}
	return c.do(ctx, func(ctx context.Context, ep kvpb.KVClient) (outcome, error) {
		r, err := ep.Put(ctx, req)
		if err != nil {
			return outcome{}, err
		}
		return outcome{r.Status, r.LeaderHint, r.Error}, nil
	})
}

// Get returns the value under key; found is false if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (value []byte, found bool, err error) {
	req := &kvpb.GetRequest{Key: key}
	err = c.do(ctx, func(ctx context.Context, ep kvpb.KVClient) (outcome, error) {
		r, err := ep.Get(ctx, req)
		if err != nil {
			return outcome{}, err
		}
		value, found = r.Value, r.Found
		return outcome{r.Status, r.LeaderHint, r.Error}, nil
	})
	return value, found, err
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (existed bool, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	req := &kvpb.DeleteRequest{Key: key, ClientId: c.id, Seq: c.nextSeq()}
	err = c.do(ctx, func(ctx context.Context, ep kvpb.KVClient) (outcome, error) {
		r, err := ep.Delete(ctx, req)
		if err != nil {
			return outcome{}, err
		}
		existed = r.Existed
		return outcome{r.Status, r.LeaderHint, r.Error}, nil
	})
	return existed, err
}

// CAS sets key to value if its current value equals expected. If expectAbsent
// is true it instead requires the key to not exist. It returns whether the swap
// happened and the value the key holds afterwards.
func (c *Client) CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (swapped bool, current []byte, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	req := &kvpb.CASRequest{
		Key: key, Expected: expected, ExpectAbsent: expectAbsent, Value: value,
		ClientId: c.id, Seq: c.nextSeq(),
	}
	err = c.do(ctx, func(ctx context.Context, ep kvpb.KVClient) (outcome, error) {
		r, err := ep.CAS(ctx, req)
		if err != nil {
			return outcome{}, err
		}
		swapped, current = r.Swapped, r.Current
		return outcome{r.Status, r.LeaderHint, r.Error}, nil
	})
	return swapped, current, err
}

// ---- in-process adapter ----

type local struct{ s *Server }

// LocalClient adapts a Server to the kvpb.KVClient interface without gRPC.
func LocalClient(s *Server) kvpb.KVClient { return local{s} }

func (l local) Put(ctx context.Context, in *kvpb.PutRequest, _ ...grpc.CallOption) (*kvpb.PutResponse, error) {
	return l.s.Put(ctx, in)
}
func (l local) Get(ctx context.Context, in *kvpb.GetRequest, _ ...grpc.CallOption) (*kvpb.GetResponse, error) {
	return l.s.Get(ctx, in)
}
func (l local) Delete(ctx context.Context, in *kvpb.DeleteRequest, _ ...grpc.CallOption) (*kvpb.DeleteResponse, error) {
	return l.s.Delete(ctx, in)
}
func (l local) CAS(ctx context.Context, in *kvpb.CASRequest, _ ...grpc.CallOption) (*kvpb.CASResponse, error) {
	return l.s.CAS(ctx, in)
}
