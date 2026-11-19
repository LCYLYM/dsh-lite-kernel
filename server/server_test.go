package server

import (
	"bufio"
	"context"
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
