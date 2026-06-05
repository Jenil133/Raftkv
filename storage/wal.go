package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Jenil133/raftkv/raft"
)

const (
	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3
	// recSnapMark records that a snapshot covers every entry up to its index,
	// so replay drops them and the log may continue at index+1 even when the
	// snapshot jumped past the end of the log.
	recSnapMark byte = 4

	segPrefix    = "wal-"
	segSuffix    = ".log"
	snapFileName = "snapshot.bin"
	recHeader    = 9 // crc32 (4) + type (1) + payload length (4)

	// DefaultSegmentSize is when the WAL rolls over to a new segment file.
	DefaultSegmentSize = 16 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// SyncMode chooses the durability barrier. On Linux both modes issue fsync.
// On macOS, SyncFull uses F_FULLFSYNC, which also flushes the drive's write
// cache (the only power-loss-safe option there), while SyncFsync issues a
// plain fsync(2), which macOS lets the drive cache.
type SyncMode int

const (
	SyncFull SyncMode = iota
	SyncFsync
)

// ParseSyncMode maps "full", "fsync" and "none" to a mode and NoSync flag.
func ParseSyncMode(s string) (mode SyncMode, noSync bool, err error) {
	switch s {
	case "full", "":
		return SyncFull, false, nil
	case "fsync":
		return SyncFsync, false, nil
	case "none":
		return SyncFull, true, nil
	}
	return 0, false, fmt.Errorf("unknown sync mode %q (want full, fsync or none)", s)
}

// segment is one WAL file. Records go to the newest (active) segment; older
// ones are sealed and only ever deleted, a whole prefix at a time, once a
// snapshot covers every entry they hold.
type segment struct {
	seq       uint64
	path      string
	lastIndex uint64   // highest entry index written to it (0 if none)
	f         *os.File // open while it may still need an fsync
}

// WAL is a file-backed Storage. The log and hard state live in append-only
// segment files of CRC-protected records; replaying them on open rebuilds the
// state, and a torn final record (crash mid-write) is detected and cut off.
// The snapshot lives in its own file, replaced atomically. Each segment
// begins with the current hard state, so dropping old segments never loses it.
type WAL struct {
	mu         sync.Mutex
	dir        string
	segs       []*segment // oldest first; last is active
	w          *bufio.Writer
	activeSize int64
	dirty      bool
	hs         raft.HardState
	snapIndex  uint64
	unsynced   []*os.File // sealed files with data not yet fsynced

	// Group commit: every flush bumps writeSeq; an fsync started after a
	// flush makes it durable and advances syncedSeq. syncMu serialises
	// fsyncs and segment deletion without blocking appends.
	syncMu    sync.Mutex
	writeSeq  uint64
	syncedSeq uint64

	loaded raft.State // replayed at open, handed out once by Load

	// NoSync skips fsync; useful for benchmarks and tests.
	NoSync bool
	// Barrier selects how Sync reaches the disk.
	Barrier SyncMode
	// SyncObserver, if set, is told how long each fsync took.
	SyncObserver func(time.Duration)
	// SegmentSize is the size at which a new segment is started.
	SegmentSize int64
}

// replayState accumulates records while reading segment files.
type replayState struct {
	hs      raft.HardState
	entries []raft.Entry
}

func (r *replayState) apply(typ byte, p []byte) (uint64, error) {
	switch typ {
	case recHardState:
		if len(p) != 16 {
			return 0, fmt.Errorf("wal: bad hard state record")
		}
		r.hs = raft.HardState{
			Term:     binary.LittleEndian.Uint64(p[0:8]),
			VotedFor: raft.NodeID(binary.LittleEndian.Uint64(p[8:16])),
		}
	case recEntry:
		if len(p) < 17 {
			return 0, fmt.Errorf("wal: bad entry record")
		}
		e := raft.Entry{
			Term:  binary.LittleEndian.Uint64(p[0:8]),
			Index: binary.LittleEndian.Uint64(p[8:16]),
			Type:  raft.EntryType(p[16]),
			Data:  append([]byte(nil), p[17:]...),
		}
		if n := len(r.entries); n > 0 && e.Index != r.entries[n-1].Index+1 {
			return 0, fmt.Errorf("wal: entry index %d does not follow %d", e.Index, r.entries[n-1].Index)
		}
		r.entries = append(r.entries, e)
		return e.Index, nil
	case recTruncate:
		if len(p) != 8 {
			return 0, fmt.Errorf("wal: bad truncate record")
		}
		idx := binary.LittleEndian.Uint64(p)
		if n := len(r.entries); n > 0 && idx <= r.entries[n-1].Index {
			if first := r.entries[0].Index; idx <= first {
				r.entries = nil
			} else {
				r.entries = r.entries[:idx-first]
			}
		}
	case recSnapMark:
		if len(p) != 8 {
			return 0, fmt.Errorf("wal: bad snapshot mark record")
		}
		idx := binary.LittleEndian.Uint64(p)
		i := 0
		for i < len(r.entries) && r.entries[i].Index <= idx {
			i++
		}
		r.entries = r.entries[i:]
	default:
		return 0, fmt.Errorf("wal: unknown record type %d", typ)
	}
	return 0, nil
}

// replayInto reads records from r into st until EOF or the first corrupt
// record. It returns the offset just past the last good record and the
// highest entry index seen.
func replayInto(st *replayState, r io.Reader) (good int64, lastIndex uint64, err error) {
	br := bufio.NewReaderSize(r, 1<<20)
	hdr := make([]byte, recHeader)
	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			return good, lastIndex, nil // EOF or torn header
		}
		crc := binary.LittleEndian.Uint32(hdr[0:4])
		typ := hdr[4]
		n := binary.LittleEndian.Uint32(hdr[5:9])
		if n > 1<<30 {
			return good, lastIndex, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(br, payload); err != nil {
			return good, lastIndex, nil
		}
		h := crc32.New(crcTable)
		h.Write(hdr[4:9])
		h.Write(payload)
		if h.Sum32() != crc {
			return good, lastIndex, nil
		}
		idx, err := st.apply(typ, payload)
		if err != nil {
			return 0, 0, err
		}
		if idx > lastIndex {
			lastIndex = idx
		}
		good += int64(recHeader) + int64(n)
	}
}

func segName(seq uint64) string { return fmt.Sprintf("%s%016d%s", segPrefix, seq, segSuffix) }

func listSegments(dir string) ([]*segment, error) {
	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []*segment
	for _, de := range names {
		name := de.Name()
		if !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, segPrefix), segSuffix), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, &segment{seq: seq, path: filepath.Join(dir, name)})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	return segs, nil
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
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, snapIndex: snap.Index, SegmentSize: DefaultSegmentSize}
	st := &replayState{}
	for i, sg := range segs {
		f, err := os.Open(sg.path)
		if err != nil {
			return nil, err
		}
		good, last, err := replayInto(st, f)
		info, statErr := f.Stat()
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("wal: %s: %w", sg.path, err)
		}
		if statErr != nil {
			return nil, statErr
		}
		sg.lastIndex = last
		if good < info.Size() {
			if i != len(segs)-1 {
				return nil, fmt.Errorf("wal: sealed segment %s is corrupt at offset %d", sg.path, good)
			}
			// Torn write at the tail of the active segment: cut it off.
			if err := os.Truncate(sg.path, good); err != nil {
				return nil, err
			}
		}
	}
	w.hs = st.hs

	// A crash after saving a snapshot can leave entries it already covers.
	entries := st.entries
	for len(entries) > 0 && entries[0].Index <= snap.Index {
		entries = entries[1:]
	}
	if len(entries) > 0 && entries[0].Index != snap.Index+1 {
		return nil, fmt.Errorf("wal: log starts at %d but snapshot ends at %d", entries[0].Index, snap.Index)
	}
	w.loaded = raft.State{HardState: st.hs, Snapshot: snap, Entries: entries}

	if len(segs) == 0 {
		w.segs = nil
		if err := w.startSegmentLocked(1); err != nil {
			return nil, err
		}
		return w, nil
	}
	active := segs[len(segs)-1]
	f, err := os.OpenFile(active.path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	active.f = f
	w.segs = segs
	w.activeSize = size
	w.w = bufio.NewWriterSize(f, 1<<20)
	return w, nil
}

// startSegmentLocked creates segment seq, makes it active and writes the
// current hard state at its head.
func (w *WAL) startSegmentLocked(seq uint64) error {
	path := filepath.Join(w.dir, segName(seq))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.segs = append(w.segs, &segment{seq: seq, path: path, f: f})
	w.w = bufio.NewWriterSize(f, 1<<20)
	w.activeSize = 0
	if !w.NoSync {
		syncDir(w.dir)
	}
	return w.writeRecordLocked(recHardState, hardStatePayload(w.hs))
}

func (w *WAL) active() *segment { return w.segs[len(w.segs)-1] }

// rotateLocked seals the active segment and starts the next one. The sealed
// file stays open until an fsync has covered it.
func (w *WAL) rotateLocked() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	w.writeSeq++
	w.dirty = false
	old := w.active()
	w.unsynced = append(w.unsynced, old.f)
	return w.startSegmentLocked(old.seq + 1)
}

func encodeRecord(typ byte, payload []byte) []byte {
	buf := make([]byte, recHeader+len(payload))
	buf[4] = typ
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(payload)))
	copy(buf[recHeader:], payload)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], crcTable))
	return buf
}

func (w *WAL) writeRecordLocked(typ byte, payload []byte) error {
	rec := encodeRecord(typ, payload)
	if _, err := w.w.Write(rec); err != nil {
		return err
	}
	w.activeSize += int64(len(rec))
	w.dirty = true
	return nil
}

func (w *WAL) maybeRotateLocked() error {
	if w.activeSize >= w.SegmentSize {
		return w.rotateLocked()
	}
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
	w.hs = hs
	if err := w.writeRecordLocked(recHardState, hardStatePayload(hs)); err != nil {
		return err
	}
	return w.maybeRotateLocked()
}

func (w *WAL) Append(entries []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		if err := w.writeRecordLocked(recEntry, entryPayload(e)); err != nil {
			return err
		}
		if a := w.active(); e.Index > a.lastIndex {
			a.lastIndex = e.Index
		}
		if err := w.maybeRotateLocked(); err != nil {
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
	if err := w.writeRecordLocked(recTruncate, p[:]); err != nil {
		return err
	}
	return w.maybeRotateLocked()
}

func (w *WAL) barrier(f *os.File) error {
	if w.Barrier == SyncFsync {
		return syscall.Fsync(int(f.Fd()))
	}
	return f.Sync()
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
	cover := w.writeSeq
	files := append(w.unsynced, w.active().f)
	w.unsynced = nil
	w.mu.Unlock()

	if !w.NoSync {
		start := time.Now()
		for _, f := range files {
			if err := w.barrier(f); err != nil {
				return err
			}
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

// SaveSnapshot stores snap atomically and deletes the oldest segments whose
// entries it fully covers. Older snapshots than the stored one are ignored.
func (w *WAL) SaveSnapshot(snap raft.Snapshot) error {
	w.mu.Lock()
	stale := snap.Index <= w.snapIndex
	w.mu.Unlock()
	if stale {
		return nil
	}
	if err := writeSnapshotFile(w.dir, snap, !w.NoSync); err != nil {
		return err
	}

	w.syncMu.Lock() // no fsync may be running on a file we close
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if snap.Index > w.snapIndex {
		w.snapIndex = snap.Index
	}
	var p [8]byte
	binary.LittleEndian.PutUint64(p[:], w.snapIndex)
	if err := w.writeRecordLocked(recSnapMark, p[:]); err != nil {
		return err
	}
	for len(w.segs) > 1 && w.segs[0].lastIndex <= w.snapIndex {
		sg := w.segs[0]
		if sg.f != nil {
			// Its data may sit in the page cache unsynced, but the snapshot
			// now covers it, so it no longer matters.
			for i, f := range w.unsynced {
				if f == sg.f {
					w.unsynced = append(w.unsynced[:i], w.unsynced[i+1:]...)
					break
				}
			}
			sg.f.Close()
		}
		if err := os.Remove(sg.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		w.segs = w.segs[1:]
	}
	return nil
}

// Close flushes and closes all files.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.w.Flush()
	for _, sg := range w.segs {
		if sg.f != nil {
			sg.f.Close()
			sg.f = nil
		}
	}
	return err
}

// Size returns the total bytes held in segment files.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.w.Flush()
	var total int64
	for _, sg := range w.segs {
		if fi, err := os.Stat(sg.path); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// Segments reports how many segment files exist (for tests).
func (w *WAL) Segments() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segs)
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
