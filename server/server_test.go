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
	time.Sleep(100 * time.Millisecond)
	if code := post(); code != 200 {
		t.Fatalf("post after turn end: %d", code)
	}
}

// A notice posted to an idle session wakes it: Resume drives a turn
// that drains the mailbox into the next provider request — the child
// completion is acted on now, not when the user next speaks.
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
