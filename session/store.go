package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	// Workspace is durable: it rides in the log so a session reloaded
	// from disk (after restart or eviction) keeps its binding. Not
	// model-visible — BuildContext skips event types it does not know.
	if _, err := lg.Append("session/meta", map[string]any{"workspace": workspace}); err != nil {
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
	var ws string
	_ = lg.ReadFrom(0, func(ev Event) bool {
		if ev.Type != "session/meta" {
			return true
		}
		var m struct {
			Workspace string `json:"workspace"`
		}
		if json.Unmarshal(ev.Data, &m) == nil {
			ws = m.Workspace
		}
		return false
	})
	ses := &Session{ID: id, Workspace: ws, Log: lg}
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
