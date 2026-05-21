// Package doc layers a schema-less JSON document API over the sharded KV
// store. A document is an arbitrary JSON object addressed by (collection, id);
// each carries a version that increments on every write, enabling optimistic
// concurrency. Every write is a compare-and-swap on the underlying key, so
// concurrent writers never lose updates.
package doc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Jenil133/raftkv/proto/kvpb"
)

var (
	ErrNotFound        = errors.New("doc: not found")
	ErrVersionConflict = errors.New("doc: version conflict")
	ErrInvalid         = errors.New("doc: invalid argument")
)

// KV is the key-value surface the document layer needs; *shard.Client and
// *kv.Client (single shard) both implement it.
type KV interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	CAS(ctx context.Context, key string, expected []byte, expectAbsent bool, value []byte) (bool, []byte, error)
	CASDelete(ctx context.Context, key string, expected []byte) (bool, []byte, error)
	Scan(ctx context.Context, prefix, after string, limit int) ([]*kvpb.Pair, error)
}

// Document is a stored JSON object plus its metadata.
type Document struct {
	Collection string
	ID         string
	Version    uint64
	Data       json.RawMessage
}

// Store reads and writes documents.
type Store struct{ kv KV }

func NewStore(kv KV) *Store { return &Store{kv: kv} }

// envelope is what actually lives under the KV key.
type envelope struct {
	Version uint64          `json:"v"`
	Data    json.RawMessage `json:"d"`
}

const (
	keyPrefix    = "d/"
	defaultLimit = 100
	maxLimit     = 1000
)

func validate(collection, id string) error {
	if collection == "" || strings.Contains(collection, "/") {
		return fmt.Errorf("%w: collection must be non-empty and contain no '/'", ErrInvalid)
	}
	if id == "" {
		return fmt.Errorf("%w: id must be non-empty", ErrInvalid)
	}
	return nil
}

func collectionPrefix(collection string) string { return keyPrefix + collection + "/" }

func docKey(collection, id string) string { return collectionPrefix(collection) + id }

func decodeEnvelope(raw []byte) (envelope, error) {
	var e envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return envelope{}, fmt.Errorf("doc: corrupt stored document: %w", err)
	}
	return e, nil
}

func encodeEnvelope(version uint64, data json.RawMessage) []byte {
	b, _ := json.Marshal(envelope{Version: version, Data: data})
	return b
}

// parseObject checks that data is a single JSON object and returns it in
// compact form.
func parseObject(data []byte) (json.RawMessage, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return nil, fmt.Errorf("%w: document must be a JSON object", ErrInvalid)
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return buf.Bytes(), nil
}

func checkVersion(ifVersion *uint64, cur uint64) error {
	if ifVersion != nil && *ifVersion != cur {
		return fmt.Errorf("%w: have version %d, expected %d", ErrVersionConflict, cur, *ifVersion)
	}
	return nil
}

// Put creates or replaces a document and returns it with its new version. If
// ifVersion is set the write only applies when the current version matches
// (zero means the document must not exist yet).
func (s *Store) Put(ctx context.Context, collection, id string, data []byte, ifVersion *uint64) (Document, error) {
	if err := validate(collection, id); err != nil {
		return Document{}, err
	}
	obj, err := parseObject(data)
	if err != nil {
		return Document{}, err
	}
	key := docKey(collection, id)
	raw, found, err := s.kv.Get(ctx, key)
	if err != nil {
		return Document{}, err
	}
	for {
		var cur uint64
		if found {
			e, err := decodeEnvelope(raw)
			if err != nil {
				return Document{}, err
			}
			cur = e.Version
		}
		if err := checkVersion(ifVersion, cur); err != nil {
			return Document{}, err
		}
		next := encodeEnvelope(cur+1, obj)
		swapped, current, err := s.kv.CAS(ctx, key, raw, !found, next)
		if err != nil {
			return Document{}, err
		}
		if swapped {
			return Document{Collection: collection, ID: id, Version: cur + 1, Data: obj}, nil
		}
		raw, found = current, len(current) > 0 // lost a race: retry on the newer value
	}
}

// Get returns a document or ErrNotFound.
func (s *Store) Get(ctx context.Context, collection, id string) (Document, error) {
	if err := validate(collection, id); err != nil {
		return Document{}, err
	}
	raw, found, err := s.kv.Get(ctx, docKey(collection, id))
	if err != nil {
		return Document{}, err
	}
	if !found {
		return Document{}, ErrNotFound
	}
	e, err := decodeEnvelope(raw)
	if err != nil {
		return Document{}, err
	}
	return Document{Collection: collection, ID: id, Version: e.Version, Data: e.Data}, nil
}

// Delete removes a document, optionally only at a given version.
func (s *Store) Delete(ctx context.Context, collection, id string, ifVersion *uint64) error {
	if err := validate(collection, id); err != nil {
		return err
	}
	key := docKey(collection, id)
	raw, found, err := s.kv.Get(ctx, key)
	if err != nil {
		return err
	}
	for {
		if !found {
			return ErrNotFound
		}
		e, err := decodeEnvelope(raw)
		if err != nil {
			return err
		}
		if err := checkVersion(ifVersion, e.Version); err != nil {
			return err
		}
		deleted, current, err := s.kv.CASDelete(ctx, key, raw)
		if err != nil {
			return err
		}
		if deleted {
			return nil
		}
		raw, found = current, len(current) > 0
	}
}

// Patch applies an RFC 7386 JSON merge patch to an existing document: object
// fields merge recursively and null removes a field. Concurrent patches to
// different fields all take effect.
func (s *Store) Patch(ctx context.Context, collection, id string, patch []byte, ifVersion *uint64) (Document, error) {
	if err := validate(collection, id); err != nil {
		return Document{}, err
	}
	if _, err := parseObject(patch); err != nil {
		return Document{}, fmt.Errorf("%w (patch)", err)
	}
	patchVal, err := decodeAny(patch)
	if err != nil {
		return Document{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	key := docKey(collection, id)
	raw, found, err := s.kv.Get(ctx, key)
	if err != nil {
		return Document{}, err
	}
	for {
		if !found {
			return Document{}, ErrNotFound
		}
		e, err := decodeEnvelope(raw)
		if err != nil {
			return Document{}, err
		}
		if err := checkVersion(ifVersion, e.Version); err != nil {
			return Document{}, err
		}
		target, err := decodeAny(e.Data)
		if err != nil {
			return Document{}, fmt.Errorf("doc: corrupt stored document: %w", err)
		}
		merged, err := json.Marshal(mergePatch(target, patchVal))
		if err != nil {
			return Document{}, err
		}
		swapped, current, err := s.kv.CAS(ctx, key, raw, false, encodeEnvelope(e.Version+1, merged))
		if err != nil {
			return Document{}, err
		}
		if swapped {
			return Document{Collection: collection, ID: id, Version: e.Version + 1, Data: merged}, nil
		}
		raw, found = current, len(current) > 0
	}
}

// Scan lists documents in a collection in id order, starting after afterID.
func (s *Store) Scan(ctx context.Context, collection, afterID string, limit int) ([]Document, error) {
	if collection == "" || strings.Contains(collection, "/") {
		return nil, fmt.Errorf("%w: bad collection", ErrInvalid)
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	prefix := collectionPrefix(collection)
	after := ""
	if afterID != "" {
		after = prefix + afterID
	}
	pairs, err := s.kv.Scan(ctx, prefix, after, limit)
	if err != nil {
		return nil, err
	}
	docs := make([]Document, 0, len(pairs))
	for _, p := range pairs {
		e, err := decodeEnvelope(p.Value)
		if err != nil {
			return nil, err
		}
		docs = append(docs, Document{
			Collection: collection,
			ID:         strings.TrimPrefix(p.Key, prefix),
			Version:    e.Version,
			Data:       e.Data,
		})
	}
	return docs, nil
}

func decodeAny(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber() // keep large integers exact
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// mergePatch implements RFC 7386.
func mergePatch(target, patch any) any {
	pm, ok := patch.(map[string]any)
	if !ok {
		return patch
	}
	tm, ok := target.(map[string]any)
	if !ok {
		tm = map[string]any{}
	}
	for k, v := range pm {
		if v == nil {
			delete(tm, k)
		} else {
			tm[k] = mergePatch(tm[k], v)
		}
	}
	return tm
}
