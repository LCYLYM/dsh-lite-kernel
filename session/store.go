package session

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	mu   sync.Mutex
	dir  string
	live map[string]*Session
	seq  int64
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Store{dir: dir, live: map[string]*Session{}}, nil
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

// List returns every session the store can serve: live ones plus the
// session-*.jsonl files on disk — a persisted session must be
// discoverable before it is first loaded (upstream #13812: disk
// enumeration silently dropped a healthy session).
func (s *Store) List() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	for id := range s.live {
		seen[id] = true
	}
	entries, _ := os.ReadDir(s.dir)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "session-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		seen[strings.TrimSuffix(name, ".jsonl")] = true
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
