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

// Len returns the number of events.
func (l *Log) Len() int64 { return l.nextSeq - 1 }

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
	off, _ := l.f.Seek(0, 2)
	if _, err := l.f.Write(append(line, '\n')); err != nil {
		return Event{}, err
	}
	if err := l.f.Sync(); err != nil {
		return Event{}, err
	}
	l.offsets = append(l.offsets, off)
	l.nextSeq++
	for ch := range l.subs {
		select {
		case ch <- ev:
		default:
		}
	}
	return ev, nil
}

// ReadFrom reads events with seq >= after+1, calling fn per event in
// order, stopping when fn returns false or an error occurs.
func (l *Log) ReadFrom(after int64, fn func(Event) bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readFrom(after, fn)
}

func (l *Log) readFrom(after int64, fn func(Event) bool) error {
	if after < 0 {
		after = 0
	}
	if after >= l.nextSeq-1 {
		return nil
	}
	if _, err := l.f.Seek(l.offsets[after], 0); err != nil {
		return err
	}
	r := bufio.NewReaderSize(l.f, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			var ev Event
			if jerr := json.Unmarshal(line, &ev); jerr == nil {
				if !fn(ev) {
					return nil
				}
			}
		}
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
	}
}

// Tail returns up to n trailing events — O(n), no full scan.
func (l *Log) Tail(n int) ([]Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	start := l.nextSeq - 1 - int64(n)
	if start < 0 {
		start = 0
	}
	var out []Event
	err := l.readFrom(start, func(ev Event) bool {
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

// Close flushes and closes the log file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
