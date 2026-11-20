// Package session is the durable core: an append-only event log per
// session plus a registry. Context is always assembled by scanning the
// tail of the log under a byte budget — the full history is never
// materialized into heap, which is what bounds per-session memory
// (deepseek-harness discussions #13639, #13717).
package session

import (
	"bufio"
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
	onClose []func()
}

// OpenLog opens (or creates) the log at path and rebuilds its offset index.
func OpenLog(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	l := &Log{f: f, path: path, nextSeq: 1, subs: map[chan Event]struct{}{}}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	var off int64
	for sc.Scan() {
		l.offsets = append(l.offsets, off)
		off += int64(len(sc.Bytes())) + 1
		l.nextSeq++
	}
	if err := sc.Err(); err != nil {
		f.Close()
		return nil, fmt.Errorf("session: corrupt log %s: %w", path, err)
	}
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
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
