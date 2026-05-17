// Package storage provides raft.Storage implementations.
package storage

import (
	"fmt"
	"sync"

	"github.com/Jenil133/raftkv/raft"
)

// Memory is a volatile Storage. Handing the same instance to a new node after
// a simulated crash models a disk that survived the restart.
type Memory struct {
	mu      sync.Mutex
	hs      raft.HardState
	snap    raft.Snapshot
	entries []raft.Entry // entries[i].Index == snap.Index+1+i
}

func NewMemory() *Memory { return &Memory{} }

func (m *Memory) Load() (raft.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]raft.Entry, len(m.entries))
	copy(out, m.entries)
	return raft.State{HardState: m.hs, Snapshot: m.snap, Entries: out}, nil
}

func (m *Memory) SaveHardState(hs raft.HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	return nil
}

func (m *Memory) Append(entries []raft.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		want := m.snap.Index + uint64(len(m.entries)) + 1
		if e.Index != want {
			return fmt.Errorf("storage: append index %d, want %d", e.Index, want)
		}
		m.entries = append(m.entries, e)
	}
	return nil
}

func (m *Memory) TruncateFrom(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index <= m.snap.Index {
		return fmt.Errorf("storage: truncate from %d at or below snapshot %d", index, m.snap.Index)
	}
	if keep := index - m.snap.Index - 1; keep < uint64(len(m.entries)) {
		m.entries = m.entries[:keep]
	}
	return nil
}

func (m *Memory) SaveSnapshot(snap raft.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if snap.Index < m.snap.Index {
		return fmt.Errorf("storage: snapshot %d older than stored %d", snap.Index, m.snap.Index)
	}
	drop := snap.Index - m.snap.Index
	if drop >= uint64(len(m.entries)) {
		m.entries = nil
	} else {
		m.entries = append([]raft.Entry(nil), m.entries[drop:]...)
	}
	m.snap = snap
	return nil
}

func (m *Memory) Sync() error { return nil }

// LogLen reports how many log entries are currently stored (for tests).
func (m *Memory) LogLen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}
