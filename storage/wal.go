package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Jenil133/raftkv/raft"
)

const (
	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3

	walFileName  = "raft.wal"
	snapFileName = "snapshot.bin"
	recHeader    = 9 // crc32 (4) + type (1) + payload length (4)
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// WAL is a file-backed Storage. The log and hard state live in an append-only
// file of CRC-protected records; replaying it on open rebuilds them, and a
// torn final record (crash mid-write) is detected and cut off. The snapshot
// lives in its own file, replaced atomically. Saving a snapshot also rewrites
// the log file so the compacted prefix actually frees disk space.
type WAL struct {
	mu    sync.Mutex
	dir   string
	f     *os.File
	w     *bufio.Writer
	dirty bool
	// Group commit: every flush bumps writeSeq; an fsync started after a
	// flush makes it durable and advances syncedSeq. syncMu serialises
	// fsyncs (and file swaps) without blocking appends.
	syncMu    sync.Mutex
	writeSeq  uint64
	syncedSeq uint64
	// SyncObserver, if set, is told how long each fsync took.
	SyncObserver func(time.Duration)
	// state replayed at open, handed out once by Load.
	loaded raft.State
	// NoSync skips fsync; useful for benchmarks and tests.
	NoSync bool
}

// replayState accumulates records while reading a log file.
type replayState struct {
	hs      raft.HardState
	entries []raft.Entry
}

func (r *replayState) apply(typ byte, p []byte) error {
	switch typ {
	case recHardState:
		if len(p) != 16 {
			return fmt.Errorf("wal: bad hard state record")
		}
		r.hs = raft.HardState{
			Term:     binary.LittleEndian.Uint64(p[0:8]),
			VotedFor: raft.NodeID(binary.LittleEndian.Uint64(p[8:16])),
		}
	case recEntry:
		if len(p) < 17 {
			return fmt.Errorf("wal: bad entry record")
		}
		e := raft.Entry{
			Term:  binary.LittleEndian.Uint64(p[0:8]),
			Index: binary.LittleEndian.Uint64(p[8:16]),
			Type:  raft.EntryType(p[16]),
			Data:  append([]byte(nil), p[17:]...),
		}
		if n := len(r.entries); n > 0 && e.Index != r.entries[n-1].Index+1 {
			return fmt.Errorf("wal: entry index %d does not follow %d", e.Index, r.entries[n-1].Index)
		}
		r.entries = append(r.entries, e)
	case recTruncate:
		if len(p) != 8 {
			return fmt.Errorf("wal: bad truncate record")
		}
		idx := binary.LittleEndian.Uint64(p)
		if n := len(r.entries); n > 0 && idx <= r.entries[n-1].Index {
			if first := r.entries[0].Index; idx <= first {
				r.entries = nil
			} else {
				r.entries = r.entries[:idx-first]
			}
		}
	default:
		return fmt.Errorf("wal: unknown record type %d", typ)
	}
	return nil
}

// replayFile reads records from r until EOF or the first corrupt record, and
// returns the byte offset just past the last good record.
func replayFile(r io.Reader) (*replayState, int64, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	st := &replayState{}
	var good int64
	hdr := make([]byte, recHeader)
	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			return st, good, nil // EOF or torn header
		}
		crc := binary.LittleEndian.Uint32(hdr[0:4])
		typ := hdr[4]
		n := binary.LittleEndian.Uint32(hdr[5:9])
		if n > 1<<30 {
			return st, good, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(br, payload); err != nil {
			return st, good, nil
		}
		h := crc32.New(crcTable)
		h.Write(hdr[4:9])
		h.Write(payload)
		if h.Sum32() != crc {
			return st, good, nil
		}
		if err := st.apply(typ, payload); err != nil {
			return nil, 0, err
		}
		good += int64(recHeader) + int64(n)
	}
}

// OpenWAL opens (creating if needed) the WAL in dir and replays it.
func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	snap, err := readSnapshotFile(dir)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, good, err := replayFile(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	// A crash between writing the snapshot and rewriting the log leaves
	// entries the snapshot already covers; drop them.
	entries := st.entries
	for len(entries) > 0 && entries[0].Index <= snap.Index {
		entries = entries[1:]
	}
	if len(entries) > 0 && entries[0].Index != snap.Index+1 {
		f.Close()
		return nil, fmt.Errorf("wal: log starts at %d but snapshot ends at %d", entries[0].Index, snap.Index)
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{
		dir:    dir,
		f:      f,
		w:      bufio.NewWriterSize(f, 1<<20),
		loaded: raft.State{HardState: st.hs, Snapshot: snap, Entries: entries},
	}, nil
}

func encodeRecord(typ byte, payload []byte) []byte {
	buf := make([]byte, recHeader+len(payload))
	buf[4] = typ
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(payload)))
	copy(buf[recHeader:], payload)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], crcTable))
	return buf
}

func (w *WAL) writeRecord(typ byte, payload []byte) error {
	if _, err := w.w.Write(encodeRecord(typ, payload)); err != nil {
		return err
	}
	w.dirty = true
	return nil
}

func hardStatePayload(hs raft.HardState) []byte {
	p := make([]byte, 16)
	binary.LittleEndian.PutUint64(p[0:8], hs.Term)
	binary.LittleEndian.PutUint64(p[8:16], uint64(hs.VotedFor))
	return p
}

func entryPayload(e raft.Entry) []byte {
	p := make([]byte, 17+len(e.Data))
	binary.LittleEndian.PutUint64(p[0:8], e.Term)
	binary.LittleEndian.PutUint64(p[8:16], e.Index)
	p[16] = byte(e.Type)
	copy(p[17:], e.Data)
	return p
}

// Load hands out the state replayed at open time. It is meant to be called
// once, at node startup; the replayed copy is released afterwards.
func (w *WAL) Load() (raft.State, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.loaded
	w.loaded = raft.State{}
	return st, nil
}

func (w *WAL) SaveHardState(hs raft.HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeRecord(recHardState, hardStatePayload(hs))
}

func (w *WAL) Append(entries []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		if err := w.writeRecord(recEntry, entryPayload(e)); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) TruncateFrom(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var p [8]byte
	binary.LittleEndian.PutUint64(p[:], index)
	return w.writeRecord(recTruncate, p[:])
}

// Sync makes every record written so far durable. Concurrent callers share
// fsyncs: whoever finds an fsync in flight waits for it and returns without
// issuing another if that one already covered their writes.
func (w *WAL) Sync() error {
	w.mu.Lock()
	if w.dirty {
		if err := w.w.Flush(); err != nil {
			w.mu.Unlock()
			return err
		}
		w.dirty = false
		w.writeSeq++
	}
	target := w.writeSeq
	done := w.syncedSeq >= target
	w.mu.Unlock()
	if done {
		return nil
	}

	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.mu.Lock()
	if w.syncedSeq >= target {
		w.mu.Unlock()
		return nil
	}
	// Flush anything appended while we queued so this fsync covers it too.
	if w.dirty {
		if err := w.w.Flush(); err != nil {
			w.mu.Unlock()
			return err
		}
		w.dirty = false
		w.writeSeq++
	}
	cover, f := w.writeSeq, w.f
	w.mu.Unlock()

	if !w.NoSync {
		start := time.Now()
		if err := f.Sync(); err != nil {
			return err
		}
		if w.SyncObserver != nil {
			w.SyncObserver(time.Since(start))
		}
	}
	w.mu.Lock()
	if cover > w.syncedSeq {
		w.syncedSeq = cover
	}
	w.mu.Unlock()
	return nil
}

// SaveSnapshot stores snap, then rewrites the log file without the entries it
// covers. If we crash in between, OpenWAL discards the covered entries.
func (w *WAL) SaveSnapshot(snap raft.Snapshot) error {
	w.syncMu.Lock() // no fsync may run on the file we are about to replace
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	w.dirty = false
	w.writeSeq++
	if err := writeSnapshotFile(w.dir, snap, !w.NoSync); err != nil {
		return err
	}

	// Rebuild the live log from disk and write the trimmed version.
	rf, err := os.Open(filepath.Join(w.dir, walFileName))
	if err != nil {
		return err
	}
	st, _, err := replayFile(rf)
	rf.Close()
	if err != nil {
		return err
	}
	entries := st.entries
	for len(entries) > 0 && entries[0].Index <= snap.Index {
		entries = entries[1:]
	}

	tmpPath := filepath.Join(w.dir, walFileName+".tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(tmp, 1<<20)
	bw.Write(encodeRecord(recHardState, hardStatePayload(st.hs)))
	for _, e := range entries {
		bw.Write(encodeRecord(recEntry, entryPayload(e)))
	}
	if err := bw.Flush(); err != nil {
		tmp.Close()
		return err
	}
	if !w.NoSync {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := os.Rename(tmpPath, filepath.Join(w.dir, walFileName)); err != nil {
		tmp.Close()
		return err
	}
	if !w.NoSync {
		syncDir(w.dir)
	}
	w.f.Close()
	if _, err := tmp.Seek(0, io.SeekEnd); err != nil {
		tmp.Close()
		return err
	}
	w.f = tmp
	w.w = bufio.NewWriterSize(tmp, 1<<20)
	w.syncedSeq = w.writeSeq // the rewritten file was fsynced with everything
	return nil
}

// Close flushes and closes the file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// Size returns the current size of the log file in bytes.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.w.Flush()
	fi, err := w.f.Stat()
	if err != nil {
		return 0
	}
	return fi.Size()
}

// ---- snapshot file: crc32 | index | term | data ----

func writeSnapshotFile(dir string, snap raft.Snapshot, sync bool) error {
	buf := make([]byte, 20+len(snap.Data))
	binary.LittleEndian.PutUint64(buf[4:12], snap.Index)
	binary.LittleEndian.PutUint64(buf[12:20], snap.Term)
	copy(buf[20:], snap.Data)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], crcTable))

	tmpPath := filepath.Join(dir, snapFileName+".tmp")
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, snapFileName)); err != nil {
		return err
	}
	if sync {
		syncDir(dir)
	}
	return nil
}

func readSnapshotFile(dir string) (raft.Snapshot, error) {
	buf, err := os.ReadFile(filepath.Join(dir, snapFileName))
	if os.IsNotExist(err) {
		return raft.Snapshot{}, nil
	}
	if err != nil {
		return raft.Snapshot{}, err
	}
	if len(buf) < 20 || crc32.Checksum(buf[4:], crcTable) != binary.LittleEndian.Uint32(buf[0:4]) {
		return raft.Snapshot{}, fmt.Errorf("wal: snapshot file corrupt")
	}
	return raft.Snapshot{
		Index: binary.LittleEndian.Uint64(buf[4:12]),
		Term:  binary.LittleEndian.Uint64(buf[12:20]),
		Data:  buf[20:],
	}, nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
