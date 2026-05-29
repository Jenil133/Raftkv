// Package storage provides raft.Storage implementations.
package storage

import (
	"fmt"
	"sync"

	"github.com/Jenil133/raftkv/raft"
)

// Memory is a Storage that lives in memory but models a disk: writes land in
// a volatile view and only become durable on Sync (or SaveSnapshot). Load
// returns the durable state and discards anything unsynced, exactly like a
// process restarting after a crash. Handing the same instance to a new node
// therefore simulates a restart on the same disk, including loss of the
// unsynced tail.
type Memory struct {
	mu sync.Mutex

	// Volatile view: what the running node believes it has written.
	hs      raft.HardState
	snap    raft.Snapshot
	entries []raft.Entry // entries[i].Index == snap.Index+1+i

	// Durable view: what survives a crash.
	dhs      raft.HardState
	dsnap    raft.Snapshot
	dentries []raft.Entry
	// syncedLen is how many leading entries of the volatile view are known to
	// match dentries; truncation below it lowers it.
	syncedLen int
	hsDirty   bool
}

func NewMemory() *Memory { return &Memory{} }

// Load returns the durable state and resets the volatile view to it.
func (m *Memory) Load() (raft.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs, m.snap = m.dhs, m.dsnap
	m.entries = append([]raft.Entry(nil), m.dentries...)
	m.syncedLen = len(m.entries)
	m.hsDirty = false
	out := make([]raft.Entry, len(m.dentries))
	copy(out, m.dentries)
	return raft.State{HardState: m.dhs, Snapshot: m.dsnap, Entries: out}, nil
}

func (m *Memory) SaveHardState(hs raft.HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	m.hsDirty = true
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
		if int(keep) < m.syncedLen {
			m.syncedLen = int(keep)
		}
	}
	return nil
}

// SaveSnapshot is durable on return, like the WAL's atomic snapshot file.
func (m *Memory) SaveSnapshot(snap raft.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if snap.Index < m.snap.Index {
		return fmt.Errorf("storage: snapshot %d older than stored %d", snap.Index, m.snap.Index)
	}
	// Make everything before the snapshot durable too, so the log stays
	// contiguous with it.
	m.syncLocked()
	drop := int(snap.Index - m.snap.Index)
	if drop >= len(m.entries) {
		m.entries = nil
	} else {
		m.entries = append([]raft.Entry(nil), m.entries[drop:]...)
	}
	m.snap = snap
	m.dsnap = snap
	m.dentries = append([]raft.Entry(nil), m.entries...)
	m.syncedLen = len(m.entries)
	return nil
}

func (m *Memory) syncLocked() {
	if m.hsDirty {
		m.dhs = m.hs
		m.hsDirty = false
	}
	if m.syncedLen > len(m.dentries) {
		m.syncedLen = len(m.dentries)
	}
	m.dentries = append(m.dentries[:m.syncedLen], m.entries[m.syncedLen:]...)
	m.syncedLen = len(m.entries)
}

func (m *Memory) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncLocked()
	return nil
}

// LogLen reports how many log entries the running node has stored.
func (m *Memory) LogLen() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}
