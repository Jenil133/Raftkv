package storage

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Jenil133/raftkv/raft"
)

func entries(term uint64, from, n int) []raft.Entry {
	var out []raft.Entry
	for i := 0; i < n; i++ {
		out = append(out, raft.Entry{Term: term, Index: uint64(from + i), Data: []byte{byte(from + i)}})
	}
	return out
}

func TestMemoryRoundTrip(t *testing.T) {
	m := NewMemory()
	m.SaveHardState(raft.HardState{Term: 3, VotedFor: 2})
	if err := m.Append(entries(1, 1, 5)); err != nil {
		t.Fatal(err)
	}
	m.TruncateFrom(4)
	if err := m.Append(entries(2, 4, 2)); err != nil {
		t.Fatal(err)
	}
	hs, es, _ := m.Load()
	if hs != (raft.HardState{Term: 3, VotedFor: 2}) || len(es) != 5 || es[3].Term != 2 || es[2].Term != 1 {
		t.Fatalf("unexpected state: %+v %+v", hs, es)
	}
	if err := m.Append(entries(1, 99, 1)); err == nil {
		t.Fatal("non-contiguous append accepted")
	}
}

func TestWALReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.SaveHardState(raft.HardState{Term: 1, VotedFor: 1})
	w.Append(entries(1, 1, 5))
	w.SaveHardState(raft.HardState{Term: 2, VotedFor: 0})
	w.TruncateFrom(4)
	w.Append(entries(2, 4, 3))
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	hs, es, _ := w2.Load()
	if hs != (raft.HardState{Term: 2, VotedFor: 0}) {
		t.Fatalf("hard state = %+v", hs)
	}
	want := append(entries(1, 1, 3), entries(2, 4, 3)...)
	if !reflect.DeepEqual(es, want) {
		t.Fatalf("entries = %+v\nwant %+v", es, want)
	}

	// Writes after reopening continue the same log.
	w2.Append(entries(2, 7, 1))
	w2.Sync()
}

func TestWALTornTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.Append(entries(1, 1, 3))
	w.Sync()
	w.Append(entries(1, 4, 1))
	w.Sync()
	w.Close()

	path := filepath.Join(dir, walFileName)
	info, _ := os.Stat(path)
	// Chop into the middle of the last record.
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, es, _ := w2.Load()
	if len(es) != 3 {
		t.Fatalf("got %d entries after torn tail, want 3", len(es))
	}
	// And the file is usable: append lands right after the good prefix.
	if err := w2.Append(entries(2, 4, 1)); err != nil {
		t.Fatal(err)
	}
	w2.Sync()
	w2.Close()

	w3, _ := OpenWAL(dir)
	defer w3.Close()
	_, es, _ = w3.Load()
	if len(es) != 4 || es[3].Term != 2 {
		t.Fatalf("after recovery append: %+v", es)
	}
}

func TestWALCorruptionStopsReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.Append(entries(1, 1, 4))
	w.Sync()
	w.Close()

	path := filepath.Join(dir, walFileName)
	data, _ := os.ReadFile(path)
	data[len(data)-2] ^= 0xff // flip a bit inside the last record
	os.WriteFile(path, data, 0o644)

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	_, es, _ := w2.Load()
	if len(es) != 3 {
		t.Fatalf("got %d entries after corruption, want 3", len(es))
	}
}
