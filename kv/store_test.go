package kv

import (
	"testing"

	"github.com/Jenil133/raftkv/proto/kvpb"
)

func TestStoreBasicOps(t *testing.T) {
	s := NewStore()
	s.Apply(&kvpb.Command{Op: kvpb.Op_PUT, Key: "a", Value: []byte("1")})
	if v, ok := s.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("get a = %q %v", v, ok)
	}
	if r := s.Apply(&kvpb.Command{Op: kvpb.Op_DELETE, Key: "a"}); !r.Found {
		t.Fatal("delete of existing key reported missing")
	}
	if r := s.Apply(&kvpb.Command{Op: kvpb.Op_DELETE, Key: "a"}); r.Found {
		t.Fatal("delete of missing key reported found")
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("key still present")
	}
}

func TestStoreCAS(t *testing.T) {
	s := NewStore()
	// Create-if-absent.
	r := s.Apply(&kvpb.Command{Op: kvpb.Op_CAS, Key: "k", ExpectAbsent: true, Value: []byte("v1")})
	if !r.Swapped {
		t.Fatal("expect-absent CAS should succeed on empty key")
	}
	r = s.Apply(&kvpb.Command{Op: kvpb.Op_CAS, Key: "k", ExpectAbsent: true, Value: []byte("v2")})
	if r.Swapped || string(r.Value) != "v1" || !r.Found {
		t.Fatalf("expect-absent on existing key: %+v", r)
	}
	r = s.Apply(&kvpb.Command{Op: kvpb.Op_CAS, Key: "k", Expected: []byte("v1"), Value: []byte("v2")})
	if !r.Swapped {
		t.Fatal("matching CAS failed")
	}
	r = s.Apply(&kvpb.Command{Op: kvpb.Op_CAS, Key: "k", Expected: []byte("v1"), Value: []byte("v3")})
	if r.Swapped || string(r.Value) != "v2" {
		t.Fatalf("mismatched CAS: %+v", r)
	}
	r = s.Apply(&kvpb.Command{Op: kvpb.Op_CAS, Key: "missing", Expected: []byte("x"), Value: []byte("y")})
	if r.Swapped || r.Found {
		t.Fatalf("CAS on missing key with expected value: %+v", r)
	}
}

func TestStoreExactlyOnce(t *testing.T) {
	s := NewStore()
	cas := &kvpb.Command{Op: kvpb.Op_CAS, Key: "n", ExpectAbsent: true, Value: []byte("1"), ClientId: 7, Seq: 1}
	first := s.Apply(cas)
	// Another client changes the key, then the original retry lands again.
	s.Apply(&kvpb.Command{Op: kvpb.Op_PUT, Key: "n", Value: []byte("other"), ClientId: 8, Seq: 1})
	again := s.Apply(cas)
	if !first.Swapped || !again.Swapped {
		t.Fatalf("duplicate should return the original result: first=%+v again=%+v", first, again)
	}
	if v, _ := s.Get("n"); string(v) != "other" {
		t.Fatalf("duplicate re-executed and clobbered value: %q", v)
	}

	// A stale (older) seq is ignored, not executed.
	s.Apply(&kvpb.Command{Op: kvpb.Op_PUT, Key: "n", Value: []byte("new"), ClientId: 7, Seq: 2})
	s.Apply(&kvpb.Command{Op: kvpb.Op_PUT, Key: "n", Value: []byte("stale"), ClientId: 7, Seq: 1})
	if v, _ := s.Get("n"); string(v) != "new" {
		t.Fatalf("stale seq executed: %q", v)
	}
}

func TestStoreCopiesValues(t *testing.T) {
	s := NewStore()
	buf := []byte("abc")
	s.Apply(&kvpb.Command{Op: kvpb.Op_PUT, Key: "k", Value: buf})
	buf[0] = 'X'
	v, _ := s.Get("k")
	if string(v) != "abc" {
		t.Fatalf("store aliased caller buffer: %q", v)
	}
	v[0] = 'Y'
	v2, _ := s.Get("k")
	if string(v2) != "abc" {
		t.Fatalf("store aliased returned buffer: %q", v2)
	}
}
