// Package session is the durable core: an append-only event log per
// session plus a registry. Context is always assembled by scanning the
// tail of the log under a byte budget — the full history is never
// materialized into heap, which is what bounds per-session memory
// (deepseek-harness discussions #13639, #13717).
package session

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Event is one immutable log entry.
type Event struct {
	Seq  int64           `json:"seq"`
	Type string          `json:"type"`
	At   time.Time       `json:"at"`
	Data json.RawMessage `json:"data"`
}

// Log is an append-only JSONL event log. Its in-memory footprint is one
// int64 offset per event (~8 B/event: 100k events ≈ 0.8 MiB) — bounded by
// design, unlike whole-history materialization.
type Log struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	offsets []int64 // byte offset of each event line, index = seq-1... seq starts at 1
	nextSeq int64
	subs    map[chan Event]struct{}
	compact *Compaction // newest session/compaction marker, if any
	onClose []func()
}

// OpenLog opens (or creates) the log at path and rebuilds its offset index.
func OpenLog(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	l := &Log{f: f, path: path, nextSeq: 1, subs: map[chan Event]struct{}{}}
	fi, _ := f.Stat()
	var size int64
	if fi != nil {
		size = fi.Size()
	}
	// ReadBytes keeps raw line endings: offsets, truncation points and the
	// last-line test all operate on real byte positions — a Scanner would
	// strip the '\r' of CRLF endings and misplace the truncation cut.
	r := bufio.NewReaderSize(f, 1<<20)
	var off int64
	for {
		raw, rerr := r.ReadBytes('\n')
		if len(raw) == 0 {
			if rerr != nil && rerr != io.EOF {
				f.Close()
				return nil, fmt.Errorf("session: corrupt log %s: %w", path, rerr)
			}
			break
		}
		line := bytes.TrimRight(raw, "\r\n")
		var ev Event
		jerr := json.Unmarshal(line, &ev)
		switch {
		case jerr == nil && ev.Seq == l.nextSeq:
			l.offsets = append(l.offsets, off)
			l.nextSeq++
			if ev.Type == "session/compaction" {
				var c Compaction
				if json.Unmarshal(ev.Data, &c) == nil {
					l.compact = &c
				}
			}
		case jerr != nil && off+int64(len(raw)) >= size:
			// Unparseable LAST line: a crash mid-append never committed —
			// drop the uncommitted tail so later appends don't leave the
			// garbage embedded in the middle of the log.
			if err := f.Truncate(off); err != nil {
				f.Close()
				return nil, err
			}
			size = off
		default:
			f.Close()
			return nil, fmt.Errorf("session: corrupt log %s at seq %d", path, l.nextSeq)
		}
		off += int64(len(raw))
		if rerr != nil {
			if rerr != io.EOF {
				f.Close()
				return nil, fmt.Errorf("session: corrupt log %s: %w", path, rerr)
			}
			break
		}
	}
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}
	// A valid final event that was never newline-terminated (writer
	// exited between the data and the '\n') is still committed — but
	// appending now would glue the next event onto its line. Terminate
	// it so the log stays well-formed.
	if size > 0 {
		if _, err := f.Seek(-1, 2); err != nil {
			f.Close()
			return nil, err
		}
		var last [1]byte
		if _, err := f.Read(last[:]); err != nil {
			f.Close()
			return nil, err
		}
		if last[0] != '\n' {
			if _, err := f.Write([]byte{'\n'}); err != nil {
				f.Close()
				return nil, err
			}
		}
	}
	return l, nil
}

// reindex rescans the file after an out-of-band rewrite (Store.Fork
// copies bytes behind the handle's back).
func (l *Log) reindex() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.f.Seek(0, 0); err != nil {
		return err
	}
	l.offsets = l.offsets[:0]
	l.nextSeq = 1
	sc := bufio.NewScanner(l.f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	var off int64
	for sc.Scan() {
		l.offsets = append(l.offsets, off)
		off += int64(len(sc.Bytes())) + 1
		l.nextSeq++
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("session: corrupt log %s: %w", l.path, err)
	}
	_, err := l.f.Seek(0, 2)
	return err
}

// Len returns the number of events.
func (l *Log) Len() int64 { return l.nextSeq - 1 }

// CompactedThrough is the seq the newest compaction marker covers (0
// when none exists), kept in memory — O(1), no scan.
func (l *Log) CompactedThrough() int64 {
	if l.compact == nil {
		return 0
	}
	return l.compact.ThroughSeq
}

// NewestCompaction returns the newest compaction marker, or nil.
func (l *Log) NewestCompaction() *Compaction { return l.compact }

// Append writes an event, fsyncs, and notifies subscribers. Delivery
// order is seq order; subscribers that lag drop nothing — they replay
// from ReadFrom instead.
func (l *Log) Append(typ string, data any) (Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ev := Event{Seq: l.nextSeq, Type: typ, At: time.Now().UTC(), Data: raw}
	line, err := json.Marshal(ev)
	if err != nil {
		return Event{}, err
	}
	off, err := l.f.Seek(0, 2)
	if err != nil {
		return Event{}, err
	}
	if _, err := l.f.Write(append(line, '\n')); err != nil {
		return Event{}, err
	}
	if err := l.f.Sync(); err != nil {
		return Event{}, err
	}
	l.offsets = append(l.offsets, off)
	l.nextSeq++
	if typ == "session/compaction" {
		var c Compaction
		if json.Unmarshal(raw, &c) == nil {
			l.compact = &c
		}
	}
	for ch := range l.subs {
		select {
		case ch <- ev:
		default:
		}
	}
	return ev, nil
}

// ReadFrom reads events with seq >= after+1, calling fn per event in
// order, stopping when fn returns false or an error occurs. It reads
// through a private descriptor: a slow client replaying a large log
// must not hold l.mu — appends and other reads would stall for the
// whole scan (the same class of jank upstream reports for big
// sessions).
func (l *Log) ReadFrom(after int64, fn func(Event) bool) error {
	l.mu.Lock()
	if after < 0 {
		after = 0
	}
	if after >= l.nextSeq-1 {
		l.mu.Unlock()
		return nil
	}
	off := l.offsets[after]
	fi, statErr := l.f.Stat()
	l.mu.Unlock()
	if statErr != nil {
		return statErr
	}

	f, err := os.Open(l.path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(off, 0); err != nil {
		return err
	}
	// Bound the scan at the snapshot end: events appended mid-replay
	// belong to the live channel, not to this replay — streaming them
	// twice produced duplicate ids at the replay/live seam.
	r := bufio.NewReaderSize(io.LimitReader(f, fi.Size()-off), 1<<20)
	return readEvents(after, r, fn)
}

func readEvents(after int64, r *bufio.Reader, fn func(Event) bool) error {
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			var ev Event
			if jerr := json.Unmarshal(line, &ev); jerr == nil {
				if !fn(ev) {
					return nil
				}
			} else if !(err != nil && err.Error() == "EOF") {
				// A bad line mid-log is corruption — surface it rather
				// than silently shrinking history. A partial last line
				// (crash mid-append) is tolerated: it was never
				// committed.
				return fmt.Errorf("session: corrupt event after seq %d: %w", after, jerr)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// Tail returns up to n trailing events — O(n), no full scan.
func (l *Log) Tail(n int) ([]Event, error) {
	if n <= 0 {
		return nil, nil
	}
	l.mu.Lock()
	if l.nextSeq <= 1 {
		l.mu.Unlock()
		return nil, nil
	}
	start := l.nextSeq - 1 - int64(n)
	if start < 0 {
		start = 0
	}
	off := l.offsets[start]
	fi, statErr := l.f.Stat()
	l.mu.Unlock()
	if statErr != nil {
		return nil, statErr
	}

	f, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(off, 0); err != nil {
		return nil, err
	}
	var out []Event
	r := bufio.NewReaderSize(io.LimitReader(f, fi.Size()-off), 1<<20)
	err = readEvents(start, r, func(ev Event) bool {
		out = append(out, ev)
		return true
	})
	return out, err
}

// Subscribe returns a live event channel; combine with ReadFrom for
// gap-free streaming (replay then live).
func (l *Log) Subscribe(buf int) (<-chan Event, func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch := make(chan Event, buf)
	l.subs[ch] = struct{}{}
	return ch, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.subs, ch)
		close(ch)
	}
}

// Close flushes, closes the log file, and runs registered cleanups —
// indexes keyed on this log (mailbox, projections) release here rather
// than pinning a closed log forever.
func (l *Log) Close() error {
	l.mu.Lock()
	cbs := l.onClose
	l.onClose = nil
	err := l.f.Close()
	l.mu.Unlock()
	for _, fn := range cbs {
		fn()
	}
	return err
}

// OnClose registers fn to run once when Close is called.
func (l *Log) OnClose(fn func()) {
	l.mu.Lock()
	l.onClose = append(l.onClose, fn)
	l.mu.Unlock()
}
