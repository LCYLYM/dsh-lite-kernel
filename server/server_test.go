package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/agent"
	"github.com/LCYLYM/dsh-lite-kernel/kernel"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/session"
	"github.com/LCYLYM/dsh-lite-kernel/tools"
)

func newTestServer(t *testing.T) (*httptest.Server, *session.Store) {
	t.Helper()
	st, err := session.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{
		Store: st, Provider: &llm.EchoProvider{}, Model: "echo",
		NewLoop: func(ses *session.Session) *agent.Loop {
			root := kernel.NewRoot("root")
			sc, _ := root.Child("a")
			reg := agent.NewRegistry(sc, time.Second)
			reg.Register(tools.Echo{})
			return &agent.Loop{
				Model: "echo", Provider: &llm.EchoProvider{}, Registry: reg, Log: ses.Log,
				Budget: session.DefaultBudget(), Guard: agent.DefaultGuard(),
				Retry: llm.Policy{Retries: 0},
			}
		},
	}
	return httptest.NewServer(srv.Handler()), st
}

func post(t *testing.T, url, body string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: %d", url, resp.StatusCode)
	}
}

// The stream is resumable: reconnect with ?after_seq= and get every
// missed event — a transient drop never permanently freezes the feed
// (#13747).
func TestStreamResumeFromSeq(t *testing.T) {
	ts, st := newTestServer(t)
	defer ts.Close()
	post(t, ts.URL+"/v1/sessions", `{"workspace":"ws"}`)
	id := st.List()[0]
	post(t, ts.URL+"/v1/sessions/"+id+"/messages", `{"text":"hi"}`)
	time.Sleep(300 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("%s/v1/sessions/%s/events?after_seq=0", ts.URL, id))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	seen := 0
	var lastSeq int
	sc := bufio.NewScanner(resp.Body)
	deadline := time.After(3 * time.Second)
	done := make(chan struct{})
	go func() {
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "id: ") {
				fmt.Sscanf(line, "id: %d", &lastSeq)
				seen++
			}
			if seen >= 6 {
				break
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-deadline:
		t.Fatalf("only saw %d events", seen)
	}
	if lastSeq < 6 {
		t.Fatalf("lastSeq=%d", lastSeq)
	}

	// Reconnect mid-stream with after_seq — replay must skip already-seen
	// events and continue.
	resp2, err := http.Get(fmt.Sprintf("%s/v1/sessions/%s/events?after_seq=%d", ts.URL, id, lastSeq))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	time.Sleep(200 * time.Millisecond)
	post(t, ts.URL+"/v1/sessions/"+id+"/messages", `{"text":"again"}`)
	got := make(chan string, 1)
	go func() {
		s2 := bufio.NewScanner(resp2.Body)
		for s2.Scan() {
			if strings.HasPrefix(s2.Text(), "event: user/message") {
				got <- "user/message"
				return
			}
		}
	}()
	select {
	case <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("no live event after resume")
	}
}

// A non-numeric after_seq must not silently replay the whole log.
func TestRejectsBadAfterSeq(t *testing.T) {
	ts, st := newTestServer(t)
	defer ts.Close()
	ses, err := st.Create("ws")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(ts.URL + "/v1/sessions/" + ses.ID + "/events?after_seq=abc")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestRejectsMalformedAndOversizedBodies(t *testing.T) {
	ts, _ := newTestServer(t)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/sessions", "application/json", strings.NewReader("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed body: status %d", resp.StatusCode)
	}

	big := strings.NewReader(`{"workspace":"` + strings.Repeat("x", 2<<20) + `"}`)
	resp, err = http.Post(ts.URL+"/v1/sessions", "application/json", big)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized body: status %d", resp.StatusCode)
	}
}

func TestGetMissingSession404(t *testing.T) {
	ts, _ := newTestServer(t)
	defer ts.Close()
	for _, path := range []string{"/v1/sessions/nope", "/v1/sessions/..%2F..%2Fetc%2Fpasswd"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: %d, want 404", path, resp.StatusCode)
		}
	}
}

func TestStreamGapFillAfterBufferOverflow(t *testing.T) {
	st, err := session.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Store: st, EventBuf: 1, Heartbeat: 50 * time.Millisecond,
		Provider: &llm.EchoProvider{}, Model: "echo",
		NewLoop: func(ses *session.Session) *agent.Loop { return nil }}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	ses, err := st.Create("")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET",
		ts.URL+"/v1/sessions/"+ses.ID+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// let the handler subscribe before the burst
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 300; i++ {
		ses.Log.Append("assistant/message", map[string]any{"content": i})
	}
	// read until seq 300 arrives — buffer drops must be refilled
	sc := bufio.NewScanner(resp.Body)
	last := int64(0)
	deadline := time.Now().Add(10 * time.Second)
	for last < 300 && time.Now().Before(deadline) {
		if !sc.Scan() {
			t.Fatalf("stream ended early: %v", sc.Err())
		}
		line := sc.Text()
		if strings.HasPrefix(line, "id: ") {
			var n int64
			fmt.Sscanf(line, "id: %d", &n)
			if n != last+1 {
				t.Fatalf("gap in delivered seqs: got %d after %d", n, last)
			}
			last = n
		}
	}
	if last != 300 {
		t.Fatalf("only %d/300 events delivered", last)
	}
}

type holdProvider struct{ gate chan struct{} }

func (p *holdProvider) Complete(ctx context.Context, r *llm.Request) (*llm.Response, error) {
	<-p.gate
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}, FinishReason: "stop"}, nil
}

func TestPostMessageBusyConflict(t *testing.T) {
	st, err := session.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	srv := &Server{
		Store: st, Provider: &llm.EchoProvider{}, Model: "echo",
		NewLoop: func(ses *session.Session) *agent.Loop {
			root := kernel.NewRoot("root")
			sc, _ := root.Child("a")
			reg := agent.NewRegistry(sc, time.Second)
			return &agent.Loop{
				Model: "echo", Provider: &holdProvider{gate: gate}, Registry: reg, Log: ses.Log,
				Budget: session.DefaultBudget(), Guard: agent.DefaultGuard(),
				Retry: llm.Policy{Retries: 0},
			}
		},
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	res, err := http.Post(ts.URL+"/v1/sessions", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		ID string `json:"id"`
	}
	json.NewDecoder(res.Body).Decode(&created)
	res.Body.Close()

	post := func() int {
		r, err := http.Post(ts.URL+"/v1/sessions/"+created.ID+"/messages",
			"application/json", strings.NewReader(`{"text":"hi"}`))
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.StatusCode
	}
	if code := post(); code != 200 {
		t.Fatalf("first post: %d", code)
	}
	time.Sleep(100 * time.Millisecond) // let the turn reach Complete
	if code := post(); code != http.StatusConflict {
		t.Fatalf("busy post: %d, want 409", code)
	}
	close(gate)
	// Poll rather than sleep: the turn teardown is a goroutine
	// handoff that can outlast any fixed delay on a loaded runner.
	deadline := time.Now().Add(5 * time.Second)
	for {
		code := post()
		if code == 200 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("post after turn end: still %d after 5s", code)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// A delete must not close the log under an in-flight turn — the busy
// session answers 409 like a busy postMessage; after the turn ends the
// delete succeeds.
func TestDeleteBusyConflict(t *testing.T) {
	st, err := session.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	srv := &Server{
		Store: st, Provider: &llm.EchoProvider{}, Model: "echo",
		NewLoop: func(ses *session.Session) *agent.Loop {
			root := kernel.NewRoot("root")
			sc, _ := root.Child("a")
			reg := agent.NewRegistry(sc, time.Second)
			return &agent.Loop{
				Model: "echo", Provider: &holdProvider{gate: gate}, Registry: reg, Log: ses.Log,
				Budget: session.DefaultBudget(), Guard: agent.DefaultGuard(),
				Retry: llm.Policy{Retries: 0},
			}
		},
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	res, err := http.Post(ts.URL+"/v1/sessions", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		ID string `json:"id"`
	}
	json.NewDecoder(res.Body).Decode(&created)
	res.Body.Close()

	post(t, ts.URL+"/v1/sessions/"+created.ID+"/messages", `{"text":"hi"}`)
	time.Sleep(100 * time.Millisecond) // let the turn reach Complete

	del := func() int {
		req, _ := http.NewRequest("DELETE", ts.URL+"/v1/sessions/"+created.ID, nil)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		return r.StatusCode
	}
	if code := del(); code != http.StatusConflict {
		t.Fatalf("delete during turn: %d, want 409", code)
	}
	close(gate)
	deadline := time.Now().Add(5 * time.Second)
	for {
		code := del()
		if code == http.StatusNoContent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("delete after turn end: still %d after 5s", code)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// Events appended during replay must not be delivered twice: the live
// channel and the log replay both see them.
func TestStreamNoDuplicateAfterReplay(t *testing.T) {
	ts, st := newTestServer(t)
	defer ts.Close()
	ses, err := st.Create("ws")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		ses.Log.Append("user/message", map[string]any{"role": "user", "content": fmt.Sprintf("m%d", i)})
	}

	req, _ := http.NewRequest("GET", ts.URL+"/v1/sessions/"+ses.ID+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Append more while the replay is in flight; each seq must appear once.
	for i := 10; i < 14; i++ {
		ses.Log.Append("user/message", map[string]any{"role": "user", "content": fmt.Sprintf("m%d", i)})
	}

	seen := map[int64]int{}
	sc := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(seen) < 14 {
		if !sc.Scan() {
			break
		}
		line := sc.Text()
		if !strings.HasPrefix(line, "id: ") {
			continue
		}
		var n int64
		fmt.Sscanf(line, "id: %d", &n)
		seen[n]++
	}
	for seq, c := range seen {
		if c != 1 {
			t.Fatalf("seq %d delivered %d times", seq, c)
		}
	}
	if len(seen) != 14 {
		t.Fatalf("saw %d events, want 14", len(seen))
	}
}

// A notice posted to an idle session triggers a wake-driven turn whose
// first drain injects it — it must not wait for the next user message.
func TestNoticeWakesIdleSession(t *testing.T) {
	ts, st := newTestServer(t)
	defer ts.Close()
	post(t, ts.URL+"/v1/sessions", `{"workspace":"ws"}`)
	id := st.List()[0]

	post(t, ts.URL+"/v1/sessions/"+id+"/notices",
		`{"id":"n-wake","from":"child-1","kind":"done","payload":{"ok":true}}`)

	var inject bool
	for i := 0; i < 50; i++ {
		ses, _ := st.Get(id)
		inject = false
		ses.Log.ReadFrom(0, func(ev session.Event) bool {
			if ev.Type == "mailbox/inject" {
				inject = true
			}
			return !inject
		})
		if inject {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !inject {
		t.Fatal("notice never consumed by a wake-driven turn")
	}
}

// wake=false enqueues without waking: an idle session must not start a
// turn on a notice that reports an aborted outcome (#13859).
func TestNoticeWakeFalseDoesNotWake(t *testing.T) {
	ts, st := newTestServer(t)
	defer ts.Close()
	post(t, ts.URL+"/v1/sessions", `{"workspace":"ws"}`)
	id := st.List()[0]

	post(t, ts.URL+"/v1/sessions/"+id+"/notices",
		`{"id":"n-nowake","from":"child-1","kind":"aborted","wake":false,"payload":{}}`)

	ses, _ := st.Get(id)
	time.Sleep(300 * time.Millisecond)
	inject := false
	ses.Log.ReadFrom(0, func(ev session.Event) bool {
		if ev.Type == "mailbox/inject" {
			inject = true
		}
		return !inject
	})
	if inject {
		t.Fatal("wake=false notice woke the idle session")
	}
	// The notice still delivers — on the next real turn, not a wake.
	post(t, ts.URL+"/v1/sessions/"+id+"/messages", `{"text":"hi"}`)
	for i := 0; i < 50; i++ {
		inject = false
		ses.Log.ReadFrom(0, func(ev session.Event) bool {
			if ev.Type == "mailbox/inject" {
				inject = true
			}
			return !inject
		})
		if inject {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !inject {
		t.Fatal("wake=false notice never delivered on the next turn")
	}
}

func TestUsageEndpoint(t *testing.T) {
	ts, st := newTestServer(t)
	defer ts.Close()
	ses, err := st.Create(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(ts.URL + "/v1/sessions/" + ses.ID + "/usage")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var u map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
		t.Fatal(err)
	}
	if _, ok := u["input_tokens"]; !ok {
		t.Fatalf("missing input_tokens in %v", u)
	}
	resp2, err := http.Get(ts.URL + "/v1/sessions/nope/usage")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("missing session: status %d", resp2.StatusCode)
	}
}
