// Package server exposes the kernel over HTTP + SSE. Two fixes matter
// here:
//
//   - The event stream is resumable: clients pass Last-Event-ID /
//     ?after_seq=N and replay from the durable log, then go live. A
//     dropped connection is retried by reconnect, so a transient failure
//     can never permanently freeze a stream (#13747).
//   - Writes carry a deadline and a dead peer is closed promptly instead
//     of being fed events forever (#13759).
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/agent"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/session"
)

// Server is the HTTP front end.
type Server struct {
	Store    *session.Store
	Provider llm.Provider
	Model    string
	NewLoop  func(ses *session.Session) *agent.Loop

	mu    sync.Mutex
	loops map[string]*agent.Loop
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("GET /v1/sessions", s.listSessions)
	mux.HandleFunc("GET /v1/sessions/{id}", s.getSession)
	mux.HandleFunc("POST /v1/sessions/{id}/messages", s.postMessage)
	mux.HandleFunc("GET /v1/sessions/{id}/events", s.streamEvents)
	mux.HandleFunc("POST /v1/sessions/{id}/notices", s.postNotice)
	mux.HandleFunc("GET /v1/sessions/{id}/tail", s.tailSession)
	mux.HandleFunc("DELETE /v1/sessions/{id}", s.deleteSession)
	return mux
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Workspace string `json:"workspace"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	ses, reused, err := s.Store.CreateOrReuseBlank(body.Workspace)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"id": ses.ID, "reused_blank": reused})
}

func (s *Server) listSessions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{"ids": s.Store.List()})
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"id": ses.ID, "events": ses.Log.Len()})
}

// tailSession returns up to ?n= trailing events (default 50, capped
// at 500) — the cheap way to render a long session without streaming
// its whole log.
func (s *Server) tailSession(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	n := 50
	if v := r.URL.Query().Get("n"); v != "" {
		parsed, perr := strconv.Atoi(v)
		if perr != nil || parsed < 0 || parsed > 500 {
			http.Error(w, "n must be 0-500", http.StatusBadRequest)
			return
		}
		n = parsed
	}
	evs, err := ses.Log.Tail(n)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"events": evs})
}

// deleteSession removes the session's durable log.
func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.Delete(r.PathValue("id")); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// postMessage starts a turn in the background and returns immediately;
// progress is observable on the event stream.
func (s *Server) postMessage(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var body struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Text == "" {
		http.Error(w, "text required", http.StatusBadRequest)
		return
	}
	loop := s.loopFor(ses)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if _, err := loop.Run(ctx, body.Text); err != nil {
			log.Printf("session %s: turn ended: %v", ses.ID, err)
		}
	}()
	writeJSON(w, map[string]any{"queued": true})
}

// postNotice delivers a cross-agent mailbox notice.
func (s *Server) postNotice(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var n agent.Notice
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		http.Error(w, "bad notice", http.StatusBadRequest)
		return
	}
	if n.ID == "" {
		n.ID = fmt.Sprintf("n-%d", time.Now().UnixNano())
	}
	if err := agent.Post(ses.Log, n); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"posted": n.ID})
}

// streamEvents is the resumable event feed: replay from ?after_seq=N
// (or the Last-Event-ID header), then stream live events, then stay
// attached with SSE heartbeats.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var after int64
	if v := r.URL.Query().Get("after_seq"); v != "" {
		after, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		after, _ = strconv.ParseInt(v, 10, 64)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	rc := http.NewResponseController(w)

	ch, unsub := ses.Log.Subscribe(256)
	defer unsub()

	send := func(ev session.Event) bool {
		data, _ := json.Marshal(ev)
		// A write deadline: a peer that stops reading is closed instead of
		// accumulating buffered bytes forever (#13759).
		_ = rc.SetWriteDeadline(time.Now().Add(15 * time.Second))
		if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Type, data); err != nil {
			return false
		}
		fl.Flush()
		return true
	}

	// 1. Replay from the durable log — this is what makes the stream
	//    resumable instead of permanently dead (#13747).
	replayErr := ses.Log.ReadFrom(after, func(ev session.Event) bool {
		return send(ev)
	})
	if replayErr != nil {
		return
	}

	// 2. Live tail + heartbeat.
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok || !send(ev) {
				return
			}
		case <-heartbeat.C:
			_ = rc.SetWriteDeadline(time.Now().Add(15 * time.Second))
			if _, err := fmt.Fprintf(w, ": hb\n\n"); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

func (s *Server) loopFor(ses *session.Session) *agent.Loop {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loops == nil {
		s.loops = map[string]*agent.Loop{}
	}
	if l, ok := s.loops[ses.ID]; ok {
		return l
	}
	l := s.NewLoop(ses)
	s.loops[ses.ID] = l
	return l
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
