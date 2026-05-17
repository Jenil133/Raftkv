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

func mustLoad(t *testing.T, s raft.Storage) raft.State {
	t.Helper()
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	return st
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
	st := mustLoad(t, m)
	if st.HardState != (raft.HardState{Term: 3, VotedFor: 2}) || len(st.Entries) != 5 ||
		st.Entries[3].Term != 2 || st.Entries[2].Term != 1 {
		t.Fatalf("unexpected state: %+v", st)
	}
	if err := m.Append(entries(1, 99, 1)); err == nil {
		t.Fatal("non-contiguous append accepted")
	}
}

func TestMemorySnapshotCompacts(t *testing.T) {
	m := NewMemory()
	m.Append(entries(1, 1, 10))
	if err := m.SaveSnapshot(raft.Snapshot{Index: 6, Term: 1, Data: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	st := mustLoad(t, m)
	if st.Snapshot.Index != 6 || len(st.Entries) != 4 || st.Entries[0].Index != 7 {
		t.Fatalf("after snapshot: %+v", st)
	}
	if err := m.Append(entries(1, 11, 1)); err != nil {
		t.Fatal(err)
	}
	// Snapshot beyond the log empties it and moves the append point.
	m.SaveSnapshot(raft.Snapshot{Index: 50, Term: 2})
	if st := mustLoad(t, m); len(st.Entries) != 0 {
		t.Fatalf("log not emptied: %+v", st.Entries)
	}
	if err := m.Append(entries(2, 51, 1)); err != nil {
		t.Fatalf("append after snapshot: %v", err)
	}
	if err := m.TruncateFrom(50); err == nil {
		t.Fatal("truncating into snapshot accepted")
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
	st := mustLoad(t, w2)
	if st.HardState != (raft.HardState{Term: 2, VotedFor: 0}) {
		t.Fatalf("hard state = %+v", st.HardState)
	}
	want := append(entries(1, 1, 3), entries(2, 4, 3)...)
	if !reflect.DeepEqual(st.Entries, want) {
		t.Fatalf("entries = %+v\nwant %+v", st.Entries, want)
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
	if st := mustLoad(t, w2); len(st.Entries) != 3 {
		t.Fatalf("got %d entries after torn tail, want 3", len(st.Entries))
	}
	// And the file is usable: append lands right after the good prefix.
	if err := w2.Append(entries(2, 4, 1)); err != nil {
		t.Fatal(err)
	}
	w2.Sync()
	w2.Close()

	w3, _ := OpenWAL(dir)
	defer w3.Close()
	if st := mustLoad(t, w3); len(st.Entries) != 4 || st.Entries[3].Term != 2 {
		t.Fatalf("after recovery append: %+v", st.Entries)
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
	if st := mustLoad(t, w2); len(st.Entries) != 3 {
		t.Fatalf("got %d entries after corruption, want 3", len(st.Entries))
	}
}

func TestWALSnapshotCompactsFileAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	big := make([]byte, 1024)
	var es []raft.Entry
	for i := 1; i <= 100; i++ {
		es = append(es, raft.Entry{Term: 1, Index: uint64(i), Data: big})
	}
	w.SaveHardState(raft.HardState{Term: 4, VotedFor: 3})
	w.Append(es)
	w.Sync()
	before := w.Size()

	if err := w.SaveSnapshot(raft.Snapshot{Index: 90, Term: 1, Data: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	if after := w.Size(); after >= before/5 {
		t.Fatalf("log file did not shrink: %d -> %d", before, after)
	}
	// Keep writing on the rewritten file.
	if err := w.Append([]raft.Entry{{Term: 2, Index: 101, Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	st := mustLoad(t, w2)
	if st.Snapshot.Index != 90 || string(st.Snapshot.Data) != "state" || st.Snapshot.Term != 1 {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	if st.HardState != (raft.HardState{Term: 4, VotedFor: 3}) {
		t.Fatalf("hard state lost across compaction: %+v", st.HardState)
	}
	if len(st.Entries) != 11 || st.Entries[0].Index != 91 || st.Entries[10].Index != 101 {
		t.Fatalf("entries after restart: first=%d len=%d", st.Entries[0].Index, len(st.Entries))
	}
}

func TestWALCrashBetweenSnapshotAndRewrite(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.Append(entries(1, 1, 10))
	w.Sync()
	w.Close()
	// Simulate dying right after the snapshot file landed but before the log
	// was rewritten: the old log still holds entries the snapshot covers.
	if err := writeSnapshotFile(dir, raft.Snapshot{Index: 6, Term: 1, Data: []byte("s")}, false); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	st := mustLoad(t, w2)
	if st.Snapshot.Index != 6 || len(st.Entries) != 4 || st.Entries[0].Index != 7 {
		t.Fatalf("recovery: snapshot=%d entries=%d", st.Snapshot.Index, len(st.Entries))
	}
}

func TestWALSnapshotBeyondLog(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.Append(entries(1, 1, 3))
	w.Sync()
	if err := w.SaveSnapshot(raft.Snapshot{Index: 20, Term: 3, Data: []byte("far")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(entries(3, 21, 2)); err != nil {
		t.Fatal(err)
	}
	w.Sync()
	w.Close()

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	st := mustLoad(t, w2)
	if st.Snapshot.Index != 20 || len(st.Entries) != 2 || st.Entries[0].Index != 21 {
		t.Fatalf("state: snap=%d entries=%+v", st.Snapshot.Index, st.Entries)
	}
}
