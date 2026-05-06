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
	entries []raft.Entry
}

func NewMemory() *Memory { return &Memory{} }

func (m *Memory) Load() (raft.HardState, []raft.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]raft.Entry, len(m.entries))
	copy(out, m.entries)
	return m.hs, out, nil
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
		want := uint64(len(m.entries)) + 1
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
	if index == 0 {
		return fmt.Errorf("storage: truncate from index 0")
	}
	if index <= uint64(len(m.entries)) {
		m.entries = m.entries[:index-1]
	}
	return nil
}

func (m *Memory) Sync() error { return nil }
