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

	"github.com/Jenil133/raftkv/raft"
)

const (
	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3

	walFileName = "raft.wal"
	recHeader   = 9 // crc32 (4) + type (1) + payload length (4)
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// WAL is a file-backed Storage: an append-only log of records, each protected
// by a CRC. Replaying the records on open rebuilds the hard state and log; a
// torn final record (crash mid-write) is detected and cut off.
type WAL struct {
	mu    sync.Mutex
	f     *os.File
	w     *bufio.Writer
	dirty bool
	// loaded state, handed out once by Load.
	hs      raft.HardState
	entries []raft.Entry
	// NoSync skips fsync; useful for benchmarks and tests.
	NoSync bool
}

// OpenWAL opens (creating if needed) the WAL in dir and replays it.
func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{f: f}
	good, err := w.replay()
	if err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	w.w = bufio.NewWriterSize(f, 1<<20)
	return w, nil
}

// replay reads records until EOF or the first corrupt record and returns the
// offset of the end of the last good record.
func (w *WAL) replay() (int64, error) {
	r := bufio.NewReaderSize(w.f, 1<<20)
	var good int64
	hdr := make([]byte, recHeader)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return good, nil // EOF or torn header
		}
		crc := binary.LittleEndian.Uint32(hdr[0:4])
		typ := hdr[4]
		n := binary.LittleEndian.Uint32(hdr[5:9])
		if n > 1<<30 {
			return good, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return good, nil
		}
		h := crc32.New(crcTable)
		h.Write(hdr[4:9])
		h.Write(payload)
		if h.Sum32() != crc {
			return good, nil
		}
		if err := w.apply(typ, payload); err != nil {
			return 0, err
		}
		good += int64(recHeader) + int64(n)
	}
}

func (w *WAL) apply(typ byte, p []byte) error {
	switch typ {
	case recHardState:
		if len(p) != 16 {
			return fmt.Errorf("wal: bad hard state record")
		}
		w.hs = raft.HardState{
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
		if e.Index != uint64(len(w.entries))+1 {
			return fmt.Errorf("wal: entry index %d out of order (have %d)", e.Index, len(w.entries))
		}
		w.entries = append(w.entries, e)
	case recTruncate:
		if len(p) != 8 {
			return fmt.Errorf("wal: bad truncate record")
		}
		idx := binary.LittleEndian.Uint64(p)
		if idx >= 1 && idx <= uint64(len(w.entries)) {
			w.entries = w.entries[:idx-1]
		}
	default:
		return fmt.Errorf("wal: unknown record type %d", typ)
	}
	return nil
}

func (w *WAL) writeRecord(typ byte, payload []byte) error {
	var hdr [recHeader]byte
	hdr[4] = typ
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(payload)))
	h := crc32.New(crcTable)
	h.Write(hdr[4:9])
	h.Write(payload)
	binary.LittleEndian.PutUint32(hdr[0:4], h.Sum32())
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(payload); err != nil {
		return err
	}
	w.dirty = true
	return nil
}

func (w *WAL) Load() (raft.HardState, []raft.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]raft.Entry, len(w.entries))
	copy(out, w.entries)
	return w.hs, out, nil
}

func (w *WAL) SaveHardState(hs raft.HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var p [16]byte
	binary.LittleEndian.PutUint64(p[0:8], hs.Term)
	binary.LittleEndian.PutUint64(p[8:16], uint64(hs.VotedFor))
	return w.writeRecord(recHardState, p[:])
}

func (w *WAL) Append(entries []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		p := make([]byte, 17+len(e.Data))
		binary.LittleEndian.PutUint64(p[0:8], e.Term)
		binary.LittleEndian.PutUint64(p[8:16], e.Index)
		p[16] = byte(e.Type)
		copy(p[17:], e.Data)
		if err := w.writeRecord(recEntry, p); err != nil {
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

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.dirty {
		return nil
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	w.dirty = false
	if w.NoSync {
		return nil
	}
	return w.f.Sync()
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
