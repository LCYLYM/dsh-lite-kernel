package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"
)

// validID constrains caller-supplied session ids so they can never
// escape the data dir through "../" or separators.
var validID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

// Session is a durable conversation: a log plus metadata.
type Session struct {
	ID        string
	Workspace string
	Log       *Log
	CreatedAt int64
}

// Store owns sessions under a data dir. Creation is atomic: racing
// creators get one session, never two (#13646). Blank-session reuse is
// decided under the same lock that lists sessions, so a pending list
// can't mint orphans.
type Store struct {
	mu    sync.Mutex
	dir   string
	live  map[string]*Session
	seq   int64
	lockf *os.File
}

// NewStore takes an exclusive advisory lock on the dir: two processes
// sharing a store would interleave jsonl appends into corrupted lines
// (the daemon-vs-CLI race upstream never guarded). LOCK_NB so a second
// opener fails loud instead of silently queueing.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	st := &Store{dir: dir, live: map[string]*Session{}}
	// Seed the id counter from what is already on disk: after a restart
	// the in-memory seq restarts at 0, and without this a new session
	// would reuse session-1-1 and append to the old session's log.
	var maxSeq int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		var n int64
		var tail int
		for _, pat := range []string{"session-%d.jsonl", "session-%d-%d.jsonl"} {
			if k, _ := fmt.Sscanf(e.Name(), pat, &n, &tail); k > 0 && n > maxSeq {
				maxSeq = n
			}
		}
	}
	st.seq = maxSeq
	lf, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		return nil, fmt.Errorf("session: store %s already in use: %w", dir, err)
	}
	return &Store{dir: dir, live: map[string]*Session{}, lockf: lf}, nil
}

// Create opens a fresh session.
func (s *Store) Create(workspace string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.create(workspace)
}

func (s *Store) create(workspace string) (*Session, error) {
	s.seq++
	// Time-based component: two `dshk run`s on the same data dir must not
	// mint the same id and silently share a log. The seq is seeded from
	// disk at NewStore, so it stays unique for the life of the data dir.
	id := fmt.Sprintf("session-%d-%d", time.Now().UnixNano(), s.seq)
	path := filepath.Join(s.dir, id+".jsonl")
	lg, err := OpenLog(path)
	if err != nil {
		return nil, err
	}
	// Workspace is durable: it rides in the log so a session reloaded
	// from disk (after restart or eviction) keeps its binding. Not
	// model-visible — BuildContext skips event types it does not know.
	now := time.Now().Unix()
	if _, err := lg.Append("session/meta", map[string]any{
		"workspace": workspace, "created_at": now,
	}); err != nil {
		_ = lg.Close()
		return nil, err
	}
	ses := &Session{ID: id, Workspace: workspace, Log: lg, CreatedAt: now}
	s.live[id] = ses
	return ses, nil
}

// CreateOrReuseBlank returns an existing blank session for workspace if
// one exists — under the store lock, so concurrent and out-of-order
// callers can't mint unbounded blanks (#13624, #13646). A "blank" has no
// user/message event yet.
func (s *Store) CreateOrReuseBlank(workspace string) (*Session, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ses := range s.live {
		if ses.Workspace != workspace {
			continue
		}
		blank := true
		_ = ses.Log.ReadFrom(0, func(ev Event) bool {
			if ev.Type == "user/message" {
				blank = false
				return false
			}
			return true
		})
		if blank {
			return ses, true, nil
		}
	}
	ses, err := s.create(workspace)
	return ses, false, err
}

// ErrNotFound marks a missing (or malformed) session id. Handlers map
// it to 404; every other store error stays a 500.
var ErrNotFound = errors.New("session: no such session")

// Get returns a live session or loads it from disk.
func (s *Store) Get(id string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ses, ok := s.live[id]; ok {
		return ses, nil
	}
	if !validID.MatchString(id) {
		return nil, fmt.Errorf("%w %q", ErrNotFound, id)
	}
	path := filepath.Join(s.dir, id+".jsonl")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w %q", ErrNotFound, id)
	}
	lg, err := OpenLog(path)
	if err != nil {
		return nil, err
	}
	var ws string
	var at int64
	_ = lg.ReadFrom(0, func(ev Event) bool {
		if ev.Type != "session/meta" {
			return true
		}
		var m struct {
			Workspace string `json:"workspace"`
			CreatedAt int64  `json:"created_at"`
		}
		if json.Unmarshal(ev.Data, &m) == nil {
			ws = m.Workspace
			at = m.CreatedAt
		}
		return false
	})
	ses := &Session{ID: id, Workspace: ws, Log: lg, CreatedAt: at}
	// Older logs predate session/meta; the first event's timestamp is a
	// cheap stand-in (one line read, no full replay).
	if ses.CreatedAt == 0 {
		var first Event
		if err := lg.ReadFrom(0, func(ev Event) bool { first = ev; return false }); err == nil && !first.At.IsZero() {
			ses.CreatedAt = first.At.Unix()
		}
	}
	s.live[id] = ses
	return ses, nil
}

// Release evicts a session from the live map and closes its log. The
// session reloads on the next Get — live memory should track working
// sessions, not every session ever touched.
func (s *Store) Release(id string) error {
	s.mu.Lock()
	ses, ok := s.live[id]
	if ok {
		delete(s.live, id)
	}
	s.mu.Unlock()
	if !ok {
		return nil
	}
	return ses.Log.Close()
}

// Close flushes and closes every live session log, then releases the
// store lock; call it on shutdown so no buffered write is lost. The
// store stays usable for List but Get recreates sessions lazily.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, ses := range s.live {
		ses.Log.Close()
		delete(s.live, id)
	}
	if s.lockf == nil {
		return nil
	}
	err := syscall.Flock(int(s.lockf.Fd()), syscall.LOCK_UN)
	cerr := s.lockf.Close()
	s.lockf = nil
	return errors.Join(err, cerr)
}

// List returns live session ids (a lock-held snapshot, never a stale copy).
func (s *Store) List() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.live))
	for id := range s.live {
		ids = append(ids, id)
	}
	return ids
}
