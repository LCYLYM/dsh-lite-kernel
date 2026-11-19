package session

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

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

// Close releases the store lock. Session logs it opened stay owned by
// their callers.
func (s *Store) Close() error {
	if s.lockf == nil {
		return nil
	}
	err := syscall.Flock(int(s.lockf.Fd()), syscall.LOCK_UN)
	cerr := s.lockf.Close()
	s.lockf = nil
	return errors.Join(err, cerr)
}

// Create opens a fresh session.
func (s *Store) Create(workspace string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.create(workspace)
}

func (s *Store) create(workspace string) (*Session, error) {
	s.seq++
	id := fmt.Sprintf("session-%d-%d", s.seq, len(s.live)+1)
	path := filepath.Join(s.dir, id+".jsonl")
	lg, err := OpenLog(path)
	if err != nil {
		return nil, err
	}
	ses := &Session{ID: id, Workspace: workspace, Log: lg}
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

// Get returns a live session or loads it from disk.
func (s *Store) Get(id string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ses, ok := s.live[id]; ok {
		return ses, nil
	}
	path := filepath.Join(s.dir, id+".jsonl")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("session: no such session %q", id)
	}
	lg, err := OpenLog(path)
	if err != nil {
		return nil, err
	}
	ses := &Session{ID: id, Log: lg}
	s.live[id] = ses
	return ses, nil
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
