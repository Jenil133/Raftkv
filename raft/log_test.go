package raft

import "testing"

func buildLog(terms ...uint64) *raftLog {
	l := newRaftLog()
	for i, t := range terms {
		l.append(Entry{Term: t, Index: uint64(i + 1)})
	}
	return l
}

func TestLogBasics(t *testing.T) {
	l := buildLog(1, 1, 2, 2, 2, 3)
	if l.lastIndex() != 6 || l.lastTerm() != 3 || l.firstIndex() != 1 {
		t.Fatalf("bounds wrong: first=%d last=%d lastTerm=%d", l.firstIndex(), l.lastIndex(), l.lastTerm())
	}
	if term, ok := l.term(0); !ok || term != 0 {
		t.Fatalf("term(0) = %d,%v", term, ok)
	}
	if _, ok := l.term(7); ok {
		t.Fatal("term(7) should be out of range")
	}
	if got := l.slice(2, 3); len(got) != 3 || got[0].Index != 2 || got[2].Index != 4 {
		t.Fatalf("slice = %+v", got)
	}
	if got := l.slice(5, 100); len(got) != 2 {
		t.Fatalf("slice tail len = %d", len(got))
	}
	if got := l.slice(7, 1); got != nil {
		t.Fatalf("slice past end = %+v", got)
	}
}

func TestLogTruncate(t *testing.T) {
	l := buildLog(1, 1, 2, 2)
	l.truncateFrom(3)
	if l.lastIndex() != 2 || l.lastTerm() != 1 {
		t.Fatalf("after truncate last=%d term=%d", l.lastIndex(), l.lastTerm())
	}
	l.truncateFrom(10) // no-op
	if l.lastIndex() != 2 {
		t.Fatal("truncate beyond end changed log")
	}
	l.append(Entry{Term: 4, Index: 3})
	if l.lastTerm() != 4 {
		t.Fatal("append after truncate failed")
	}
}

func TestLogTermHelpers(t *testing.T) {
	l := buildLog(1, 1, 2, 2, 2, 3)
	if got := l.firstIndexOfTerm(2, 5); got != 3 {
		t.Fatalf("firstIndexOfTerm = %d, want 3", got)
	}
	if got := l.firstIndexOfTerm(1, 2); got != 1 {
		t.Fatalf("firstIndexOfTerm(1) = %d, want 1", got)
	}
	if got := l.lastIndexOfTerm(2); got != 5 {
		t.Fatalf("lastIndexOfTerm(2) = %d, want 5", got)
	}
	if got := l.lastIndexOfTerm(9); got != 0 {
		t.Fatalf("lastIndexOfTerm(9) = %d, want 0", got)
	}
	if got := l.lastIndexOfTerm(0); got != 0 {
		t.Fatalf("lastIndexOfTerm(0) = %d, want 0", got)
	}
}

func TestLogUpToDate(t *testing.T) {
	l := buildLog(1, 2, 2)
	cases := []struct {
		idx, term uint64
		want      bool
	}{
		{3, 2, true},  // identical
		{4, 2, true},  // longer, same term
		{2, 2, false}, // shorter, same term
		{1, 3, true},  // higher last term wins regardless of length
		{9, 1, false}, // lower last term loses regardless of length
	}
	for _, c := range cases {
		if got := l.isUpToDate(c.idx, c.term); got != c.want {
			t.Errorf("isUpToDate(%d,%d) = %v, want %v", c.idx, c.term, got, c.want)
		}
	}
}
