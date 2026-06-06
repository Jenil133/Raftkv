// Package kv implements a replicated key-value state machine on top of Raft.
package kv

import (
	"bytes"
	"maps"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/Jenil133/raftkv/proto/kvpb"
)

// Result is the outcome of applying a command.
type Result struct {
	Found   bool   // key existed (delete) / exists after the op (cas)
	Value   []byte // current value (cas)
	Swapped bool   // cas succeeded
}

type session struct {
	seq    uint64
	result Result
}

// Store is the deterministic state machine. Apply must be called with
// committed commands in log order.
type Store struct {
	mu       sync.RWMutex
	data     map[string][]byte
	sessions map[uint64]session
}

func NewStore() *Store {
	return &Store{
		data:     make(map[string][]byte),
		sessions: make(map[uint64]session),
	}
}

// Apply executes cmd. A command repeating a client's last (client, seq) is not
// executed again and returns the cached result, giving exactly-once semantics
// across client retries.
func (s *Store) Apply(cmd *kvpb.Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cmd.ClientId != 0 {
		if sess, ok := s.sessions[cmd.ClientId]; ok && cmd.Seq <= sess.seq {
			if cmd.Seq == sess.seq {
				return sess.result
			}
			return Result{} // stale duplicate of an op the client already moved past
		}
	}
	res := s.execute(cmd)
	if cmd.ClientId != 0 {
		s.sessions[cmd.ClientId] = session{seq: cmd.Seq, result: res}
	}
	return res
}

func (s *Store) execute(cmd *kvpb.Command) Result {
	switch cmd.Op {
	case kvpb.Op_PUT:
		s.data[cmd.Key] = append([]byte(nil), cmd.Value...)
		return Result{}
	case kvpb.Op_DELETE:
		_, existed := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		return Result{Found: existed}
	case kvpb.Op_CAS:
		cur, exists := s.data[cmd.Key]
		var match bool
		if cmd.ExpectAbsent {
			match = !exists
		} else {
			match = exists && bytes.Equal(cur, cmd.Expected)
		}
		if match {
			if cmd.DeleteOnMatch {
				delete(s.data, cmd.Key)
				return Result{Swapped: true}
			}
			s.data[cmd.Key] = append([]byte(nil), cmd.Value...)
			return Result{Found: true, Value: append([]byte(nil), cmd.Value...), Swapped: true}
		}
		return Result{Found: exists, Value: append([]byte(nil), cur...)}
	default:
		return Result{}
	}
}

// Get reads the local value for key.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Dump returns a copy of all data, for tests and debugging.
func (s *Store) Dump() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = string(v)
	}
	return out
}

// image is a point-in-time copy of the store. Values are never mutated in
// place (writes install fresh slices), so a shallow copy of the maps is a
// consistent snapshot that can be encoded off the apply path.
type image struct {
	data     map[string][]byte
	sessions map[uint64]session
}

func (s *Store) image() image {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return image{data: maps.Clone(s.data), sessions: maps.Clone(s.sessions)}
}

func (im image) encode() ([]byte, error) {
	snap := &kvpb.StoreSnapshot{
		Data:     im.data,
		Sessions: make(map[uint64]*kvpb.SessionState, len(im.sessions)),
	}
	for id, sess := range im.sessions {
		snap.Sessions[id] = &kvpb.SessionState{
			Seq: sess.seq, Found: sess.result.Found, Value: sess.result.Value, Swapped: sess.result.Swapped,
		}
	}
	return proto.Marshal(snap)
}

// Snapshot serialises the whole state machine, including the client session
// table so exactly-once guarantees survive compaction.
func (s *Store) Snapshot() ([]byte, error) { return s.image().encode() }

// Restore replaces the state machine with the contents of a snapshot.
func (s *Store) Restore(data []byte) error {
	var snap kvpb.StoreSnapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string][]byte, len(snap.Data))
	for k, v := range snap.Data {
		s.data[k] = append([]byte(nil), v...)
	}
	s.sessions = make(map[uint64]session, len(snap.Sessions))
	for id, st := range snap.Sessions {
		s.sessions[id] = session{seq: st.Seq, result: Result{Found: st.Found, Value: st.Value, Swapped: st.Swapped}}
	}
	return nil
}

// Scan returns up to limit key/value pairs with the given prefix and a key
// strictly greater than after, in key order. limit <= 0 means no limit.
func (s *Store) Scan(prefix, after string, limit int) []*kvpb.Pair {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k := range s.data {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]*kvpb.Pair, len(keys))
	for i, k := range keys {
		out[i] = &kvpb.Pair{Key: k, Value: append([]byte(nil), s.data[k]...)}
	}
	return out
}

// Keys returns all keys in sorted order.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
