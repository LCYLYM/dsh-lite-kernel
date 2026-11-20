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
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/agent"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/session"
)

// Server is the HTTP front end.
type Server struct {
	Store *session.Store
	// EventBuf is the live-subscription buffer per stream (default 256);
	// Heartbeat is the SSE keep-alive interval (default 20s). Both exist
	// so tests can shrink them.
	EventBuf  int
	Heartbeat time.Duration
	Provider  llm.Provider
	Model     string
	NewLoop   func(ses *session.Session) *agent.Loop
	// Token, when set, is required as `Authorization: Bearer <token>` on
	// every request — the daemon serves full session transcripts, so
	// callers must authenticate whenever it listens beyond loopback.
	Token string

	mu      sync.Mutex
	loops   map[string]*agent.Loop
	turnSeq int64
	// turns maps session id → per-turn token → cancel. Each accepted
	// message gets its own entry so overlapping turns all cancel, and a
	// finished turn removes only its own handle.
	turns map[string]map[int64]context.CancelFunc
}

const maxBodyBytes = 1 << 20 // uploads are bounded at 1 MiB

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("GET /v1/sessions", s.listSessions)
	mux.HandleFunc("GET /v1/sessions/{id}", s.getSession)
	mux.HandleFunc("POST /v1/sessions/{id}/messages", s.postMessage)
	mux.HandleFunc("GET /v1/sessions/{id}/events", s.streamEvents)
	mux.HandleFunc("GET /v1/sessions/{id}/history", s.history)
	mux.HandleFunc("POST /v1/sessions/{id}/notices", s.postNotice)
	mux.HandleFunc("GET /v1/sessions/{id}/tail", s.tailSession)
	mux.HandleFunc("GET /v1/sessions/{id}/usage", s.getUsage)
	mux.HandleFunc("DELETE /v1/sessions/{id}", s.deleteSession)
	mux.HandleFunc("POST /v1/sessions/{id}/fork", s.forkSession)
	mux.HandleFunc("POST /v1/sessions/{id}/cancel", s.cancelTurn)
	if s.Token != "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+s.Token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}
	return mux
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Workspace string `json:"workspace"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}
	// The recorded workspace is honored per session by the loop
	// factory — a path that cannot serve as a tool root is a loud
	// 400 at creation, not a session whose tools fail mysteriously
	// later.
	if body.Workspace != "" {
		st, err := os.Stat(body.Workspace)
		if err != nil || !st.IsDir() {
			http.Error(w, "workspace is not an existing directory", http.StatusBadRequest)
			return
		}
	}
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
	writeJSON(w, map[string]any{
		"id":         ses.ID,
		"workspace":  ses.Workspace,
		"created_at": ses.CreatedAt,
		"events":     ses.Log.Len(),
	})
}

// getUsage returns the aggregated token accounting Usage() folds out
// of turn/usage events — the durable total a host bills or budgets on.
func (s *Server) getUsage(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	u, err := session.Usage(ses.Log)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, u)
}

// history pages events without SSE: ?after=<seq>&limit=<n> (default
// 200, cap 500). Each page reads only its slice via the offset index.
func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var after int64
	limit := 200
	if v := r.URL.Query().Get("after"); v != "" {
		parsed, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil || parsed < 0 {
			http.Error(w, "after must be a non-negative seq", http.StatusBadRequest)
			return
		}
		after = parsed
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		parsed, perr := strconv.Atoi(v)
		if perr != nil || parsed <= 0 || parsed > 500 {
			http.Error(w, "limit must be 1-500", http.StatusBadRequest)
			return
		}
		limit = parsed
	}
	var evs []session.Event
	if err := ses.Log.ReadFrom(after, func(ev session.Event) bool {
		if len(evs) >= limit {
			return false
		}
		evs = append(evs, ev)
		return true
	}); err != nil {
		writeErr(w, err)
		return
	}
	var next int64
	if len(evs) == limit {
		next = evs[len(evs)-1].Seq
	}
	writeJSON(w, map[string]any{"events": evs, "next": next})
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

// deleteSession removes the session's durable log. The cached loop is
// dropped either way: after a failed delete the store may have swapped
// in a reopened log, and a loop still bound to the closed one would
// fail every later turn.
func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Deleting under an in-flight turn closed the log mid-turn: the
	// turn failed on its next append and its turn/end never landed.
	// Suspend the loop first — a busy session gets a 409 like a busy
	// postMessage, not a corrupted turn.
	s.mu.Lock()
	l := s.loops[id]
	s.mu.Unlock()
	var unsuspend func()
	if l != nil {
		var ok bool
		if unsuspend, ok = l.Suspend(); !ok {
			http.Error(w, "turn in progress", http.StatusConflict)
			return
		}
	}
	err := s.Store.Delete(id)
	s.mu.Lock()
	delete(s.loops, id)
	s.mu.Unlock()
	if unsuspend != nil {
		unsuspend()
	}
	if err != nil {
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
	if err := decodeJSON(w, r, &body); err != nil || body.Text == "" {
		http.Error(w, "text required", http.StatusBadRequest)
		return
	}
	loop := s.loopFor(ses)
	if loop.Busy() {
		http.Error(w, "turn already in progress", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	// Register before the goroutine starts: a cancel issued between
	// acceptance and goroutine scheduling must still find the handle.
	s.mu.Lock()
	if s.turns == nil {
		s.turns = map[string]map[int64]context.CancelFunc{}
	}
	if s.turns[ses.ID] == nil {
		s.turns[ses.ID] = map[int64]context.CancelFunc{}
	}
	s.turnSeq++
	tok := s.turnSeq
	s.turns[ses.ID][tok] = cancel
	s.mu.Unlock()
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.turns[ses.ID], tok)
			if len(s.turns[ses.ID]) == 0 {
				delete(s.turns, ses.ID)
			}
			s.mu.Unlock()
		}()
		defer cancel()
		defer s.dropLoop(ses.ID, loop)
		if _, err := loop.Run(ctx, body.Text); err != nil {
			log.Printf("session %s: turn ended: %v", ses.ID, err)
		}
	}()
	writeJSON(w, map[string]any{"queued": true})
}

// forkSession clones the session's durable log under a new id.
func (s *Server) forkSession(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Fork(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"id": ses.ID})
}

// cancelTurn aborts the in-flight turn, if any (upstream #13766 asked
// for session/cancel in the SDK protocol). A session with no active
// turn gets 409 — cancelling nothing must not look like success.
func (s *Server) cancelTurn(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	s.mu.Lock()
	handles := s.turns[ses.ID]
	cancels := make([]context.CancelFunc, 0, len(handles))
	for _, c := range handles {
		cancels = append(cancels, c)
	}
	s.mu.Unlock()
	if len(cancels) == 0 {
		http.Error(w, "no active turn", http.StatusConflict)
		return
	}
	for _, c := range cancels {
		c()
	}
	writeJSON(w, map[string]any{"cancelled": len(cancels)})
}

// postNotice delivers a cross-agent mailbox notice.
func (s *Server) postNotice(w http.ResponseWriter, r *http.Request) {
	ses, err := s.Store.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var n agent.Notice
	if err := decodeJSON(w, r, &n); err != nil {
		return
	}
	if n.ID == "" {
		n.ID = fmt.Sprintf("n-%d", time.Now().UnixNano())
	}
	if err := agent.Post(ses.Log, n); err != nil {
		writeErr(w, err)
		return
	}
	// Wake an idle session so the notice is consumed now, not whenever
	// the next user message happens to arrive — unless the poster opted
	// out: a notice reporting an aborted/cancelled outcome must be
	// enqueued for the next turn, not used to re-trigger the model
	// (#13859: a stop notice that wakes the idle parent re-dispatches
	// the very work the user just stopped).
	if n.Wake == nil || *n.Wake {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			l := s.loopFor(ses)
			state, err := l.Resume(ctx)
			if state == agent.Failed && !errors.Is(err, agent.ErrTurnBusy) {
				log.Printf("notice resume %s: %v", ses.ID, err)
			}
			// Only a wake that acquired the turn may release the cached
			// loop: on ErrTurnBusy this is the loop serving the active
			// turn, and evicting it would let the next request build a
			// second Loop with its own turnMu — two turns interleaving
			// one history.
			if !errors.Is(err, agent.ErrTurnBusy) {
				s.dropLoop(ses.ID, l)
			}
		}()
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
		var err error
		if after, err = strconv.ParseInt(v, 10, 64); err != nil || after < 0 {
			http.Error(w, "bad after_seq", http.StatusBadRequest)
			return
		}
	}
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		var err error
		if after, err = strconv.ParseInt(v, 10, 64); err != nil || after < 0 {
			http.Error(w, "bad Last-Event-ID", http.StatusBadRequest)
			return
		}
	}
	// ?types=a,b filters server-side so a client rendering a long
	// session doesn't pay for event kinds it will drop anyway.
	var types map[string]bool
	if v := r.URL.Query().Get("types"); v != "" {
		types = map[string]bool{}
		for _, t := range strings.Split(v, ",") {
			if t = strings.TrimSpace(t); t != "" {
				types[t] = true
			}
		}
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

	bufSize := s.EventBuf
	if bufSize <= 0 {
		bufSize = 256
	}
	ch, unsub := ses.Log.Subscribe(bufSize)
	defer unsub()

	last := after
	sendFailed := false
	send := func(ev session.Event) bool {
		if sendFailed || ev.Seq <= last {
			return !sendFailed
		}
		data, _ := json.Marshal(ev)
		// A write deadline: a peer that stops reading is closed instead of
		// accumulating buffered bytes forever (#13759).
		_ = rc.SetWriteDeadline(time.Now().Add(15 * time.Second))
		if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Type, data); err != nil {
			sendFailed = true
			return false
		}
		fl.Flush()
		last = ev.Seq
		return true
	}
	// fill replays everything after the last sent seq — the subscription
	// buffer drops events on overflow, but they stay durable in the log.
	fill := func() bool {
		if ses.Log.Len() <= last {
			return true
		}
		if err := ses.Log.ReadFrom(last, func(ev session.Event) bool {
			if types != nil && !types[ev.Type] {
				last = ev.Seq // skipped type still advances the cursor
				return true
			}
			return send(ev)
		}); err != nil {
			return false
		}
		return !sendFailed
	}

	// 1. Replay from the durable log — this is what makes the stream
	//    resumable instead of permanently dead (#13747). The replay is
	//    snapshot-bounded, so catch up again for events appended (and
	//    possibly dropped from the channel buffer) mid-replay; repeat
	//    until a pass makes no progress.
	for !sendFailed {
		prev := last
		if err := ses.Log.ReadFrom(last, func(ev session.Event) bool {
			if types != nil && !types[ev.Type] {
				return true
			}
			return send(ev)
		}); err != nil || sendFailed {
			return
		}
		if last <= prev {
			break // nothing sent — the log has not moved past the cursor
		}
	}

	// 2. Live tail + heartbeat.
	hb := s.Heartbeat
	if hb <= 0 {
		hb = 20 * time.Second
	}
	heartbeat := time.NewTicker(hb)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ses.Log.Done():
			// Session deleted: stop instead of heartbeating forever.
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if ev.Seq > last+1 && !fill() {
				return
			}
			if types == nil || types[ev.Type] {
				if !send(ev) {
					return
				}
			} else if ev.Seq > last {
				last = ev.Seq // skipped type still advances the cursor
			}
		case <-heartbeat.C:
			// Tail events can be dropped with nothing newer to trigger
			// the live-path fill — catch up on the heartbeat too.
			if !fill() {
				return
			}
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
	status := http.StatusInternalServerError
	if errors.Is(err, session.ErrNotFound) {
		status = http.StatusNotFound
	}
	http.Error(w, err.Error(), status)
}

// dropLoop forgets a session's loop once its turn ends — the loop is
// cheap to rebuild from the log, so holding every past loop in the map
// would grow server memory with session count, not with live turns.
// dropLoop compare-deletes: an unconditional map delete would evict a
// replacement loop already serving a newer turn.
func (s *Server) dropLoop(id string, l *agent.Loop) {
	s.mu.Lock()
	if s.loops[id] == l {
		delete(s.loops, id)
	}
	s.mu.Unlock()
}

// decodeJSON reads a request body with a hard cap so a fat body cannot
// grow the heap without bound; decoding failures surface as 400 instead
// of silently defaulting the request.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	return json.NewDecoder(r.Body).Decode(v)
}
