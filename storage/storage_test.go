package storage

import (
	"os"
	"reflect"
	"testing"

	"github.com/Jenil133/raftkv/raft"
)

func activeSegment(t *testing.T, dir string) string {
	t.Helper()
	segs, err := listSegments(dir)
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments in %s: %v", dir, err)
	}
	return segs[len(segs)-1].path
}

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
	m.Sync()
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
	m.Sync()
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
	m.Sync()
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

func TestMemoryLosesUnsyncedWritesOnCrash(t *testing.T) {
	m := NewMemory()
	m.SaveHardState(raft.HardState{Term: 1, VotedFor: 1})
	m.Append(entries(1, 1, 3))
	m.Sync()

	// Unsynced: more entries, a new term, and a truncation of synced entries.
	m.Append(entries(1, 4, 2))
	m.SaveHardState(raft.HardState{Term: 2, VotedFor: 2})
	st := mustLoad(t, m) // "crash and restart"
	if st.HardState != (raft.HardState{Term: 1, VotedFor: 1}) || len(st.Entries) != 3 {
		t.Fatalf("unsynced writes survived: %+v", st)
	}

	m.TruncateFrom(2)
	m.Append(entries(2, 2, 2))
	st = mustLoad(t, m) // crash before Sync: the truncation never happened
	if len(st.Entries) != 3 || st.Entries[1].Term != 1 {
		t.Fatalf("unsynced truncation took effect: %+v", st.Entries)
	}

	m.TruncateFrom(2)
	m.Append(entries(2, 2, 2))
	m.Sync()
	st = mustLoad(t, m)
	if len(st.Entries) != 3 || st.Entries[1].Term != 2 || st.Entries[2].Term != 2 {
		t.Fatalf("synced truncate+append lost: %+v", st.Entries)
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

	path := activeSegment(t, dir)
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

	path := activeSegment(t, dir)
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
	w.SegmentSize = 8 << 10
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
	if after := w.Size(); after >= before/4 {
		t.Fatalf("log did not shrink: %d -> %d bytes", before, after)
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
	if err := writeSnapshotFile(dir, raft.Snapshot{Index: 6, Term: 1, Data: []byte("s")}, nil); err != nil {
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

func TestWALRotatesAndReplaysAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.SegmentSize = 2 << 10
	w.SaveHardState(raft.HardState{Term: 7, VotedFor: 2})
	payload := make([]byte, 300)
	var es []raft.Entry
	for i := 1; i <= 60; i++ {
		es = append(es, raft.Entry{Term: 7, Index: uint64(i), Data: payload})
	}
	w.Append(es)
	w.Sync()
	if n := w.Segments(); n < 5 {
		t.Fatalf("expected rotation into several segments, got %d", n)
	}
	w.Close()

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	st := mustLoad(t, w2)
	if st.HardState != (raft.HardState{Term: 7, VotedFor: 2}) || len(st.Entries) != 60 {
		t.Fatalf("replay across segments: hs=%+v entries=%d", st.HardState, len(st.Entries))
	}
}

func TestWALDeletesOnlyCoveredPrefixOfSegments(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.SegmentSize = 2 << 10
	payload := make([]byte, 300)
	var es []raft.Entry
	for i := 1; i <= 40; i++ {
		es = append(es, raft.Entry{Term: 1, Index: uint64(i), Data: payload})
	}
	w.Append(es)
	w.Sync()
	// Truncate back into an old segment and rewrite: the old segment still
	// holds now-dead entries with high indexes.
	w.TruncateFrom(10)
	var es2 []raft.Entry
	for i := 10; i <= 20; i++ {
		es2 = append(es2, raft.Entry{Term: 2, Index: uint64(i), Data: payload})
	}
	w.Append(es2)
	w.Sync()
	// A snapshot at 15 must not delete segments holding dead entries > 15:
	// deleting them while keeping later ones could resurrect nothing, but
	// deleting a later one while keeping an earlier one could revive the
	// truncated entries. Only a covered prefix may go.
	if err := w.SaveSnapshot(raft.Snapshot{Index: 15, Term: 2, Data: []byte("s")}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	st := mustLoad(t, w2)
	if len(st.Entries) != 5 || st.Entries[0].Index != 16 || st.Entries[4].Index != 20 {
		t.Fatalf("entries after compaction: %d, first %+v", len(st.Entries), st.Entries)
	}
	for _, e := range st.Entries {
		if e.Term != 2 {
			t.Fatalf("truncated entry from term 1 came back: %+v", e)
		}
	}
}

func TestWALIgnoresStaleSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.Append(entries(1, 1, 10))
	w.Sync()
	w.SaveSnapshot(raft.Snapshot{Index: 8, Term: 1, Data: []byte("new")})
	w.SaveSnapshot(raft.Snapshot{Index: 5, Term: 1, Data: []byte("old")})
	w.Close()
	w2, _ := OpenWAL(dir)
	defer w2.Close()
	if st := mustLoad(t, w2); st.Snapshot.Index != 8 || string(st.Snapshot.Data) != "new" {
		t.Fatalf("stale snapshot overwrote newer: %+v", st.Snapshot)
	}
}

func TestWALConcurrentSaveSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir)
	w.NoSync = true
	w.Append(entries(1, 1, 50))
	w.Sync()
	done := make(chan struct{})
	for i := 1; i <= 8; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			data := make([]byte, 64<<10)
			for j := range data {
				data[j] = byte(i)
			}
			if err := w.SaveSnapshot(raft.Snapshot{Index: uint64(i * 5), Term: 1, Data: data}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	w.Close()
	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	st := mustLoad(t, w2)
	if st.Snapshot.Index != 40 {
		t.Fatalf("newest snapshot lost: index %d", st.Snapshot.Index)
	}
	for _, b := range st.Snapshot.Data {
		if b != 8 {
			t.Fatal("snapshot data interleaved from concurrent writers")
		}
	}
}
