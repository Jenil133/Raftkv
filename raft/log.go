package raft

// raftLog is the in-memory log. entries[0] is a sentinel holding the term of
// the entry at index offset (zero until snapshots exist), so entry i lives at
// entries[i-offset].
type raftLog struct {
	entries []Entry
	offset  uint64
}

func newRaftLog() *raftLog {
	return &raftLog{entries: []Entry{{}}}
}

func (l *raftLog) firstIndex() uint64 { return l.offset + 1 }

func (l *raftLog) lastIndex() uint64 { return l.offset + uint64(len(l.entries)) - 1 }

func (l *raftLog) lastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// term returns the term of the entry at index i.
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i < l.offset || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.offset].Term, true
}

func (l *raftLog) entry(i uint64) Entry { return l.entries[i-l.offset] }

// slice returns a copy of up to max entries starting at lo.
func (l *raftLog) slice(lo uint64, max int) []Entry {
	if lo > l.lastIndex() || lo < l.firstIndex() {
		return nil
	}
	hi := l.lastIndex()
	if n := hi - lo + 1; n > uint64(max) {
		hi = lo + uint64(max) - 1
	}
	out := make([]Entry, hi-lo+1)
	copy(out, l.entries[lo-l.offset:hi-l.offset+1])
	return out
}

func (l *raftLog) append(es ...Entry) { l.entries = append(l.entries, es...) }

// truncateFrom drops entry i and everything after it.
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.offset {
		panic("raft: truncating below log offset")
	}
	if i > l.lastIndex() {
		return
	}
	l.entries = l.entries[:i-l.offset]
}

// firstIndexOfTerm returns the lowest index in the log with the given term,
// searching backwards from index from.
func (l *raftLog) firstIndexOfTerm(term, from uint64) uint64 {
	i := from
	for i > l.firstIndex() {
		t, _ := l.term(i - 1)
		if t != term {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the highest index holding term, or 0.
func (l *raftLog) lastIndexOfTerm(term uint64) uint64 {
	for i := l.lastIndex(); i > l.offset; i-- {
		t, _ := l.term(i)
		if t == term {
			return i
		}
		if t < term {
			break
		}
	}
	return 0
}

// isUpToDate reports whether a candidate log (lastIndex, lastTerm) is at least
// as up to date as this one.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= l.lastIndex()
}
