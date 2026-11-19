package agent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/kernel"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/session"
	"github.com/LCYLYM/dsh-lite-kernel/tools"
)

func newLoop(t *testing.T, prov llm.Provider) (*Loop, *session.Log) {
	t.Helper()
	dir := t.TempDir()
	lg, err := session.OpenLog(filepath.Join(dir, "s.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	root := kernel.NewRoot("root")
	sc, err := root.Child("agent")
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry(sc, 5*time.Second)
	reg.Register(tools.Echo{})
	return &Loop{
		Model: "test", Provider: prov, Registry: reg, Log: lg,
		Budget: session.DefaultBudget(), Guard: DefaultGuard(), Retry: llm.DefaultPolicy(),
	}, lg
}

func TestLoopCompletesEchoTurn(t *testing.T) {
	loop, lg := newLoop(t, &llm.EchoProvider{})
	state, err := loop.Run(context.Background(), "hello")
	if err != nil || state != Completed {
		t.Fatalf("state=%s err=%v", state, err)
	}
	// Tool call + result must be in the log (model-visible ⇔ logged).
	var kinds []string
	lg.ReadFrom(0, func(ev session.Event) bool {
		kinds = append(kinds, ev.Type)
		return true
	})
	want := []string{"user/message", "assistant/message", "tool/call", "tool/result", "assistant/message", "turn/end"}
	if len(kinds) != len(want) {
		t.Fatalf("kinds=%v", kinds)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("kinds=%v", kinds)
		}
	}
}

// spamProvider replays the repetition-collapse bug (#13659).
type spamProvider struct{}

func (spamProvider) Complete(_ context.Context, _ *llm.Request) (*llm.Response, error) {
	return &llm.Response{
		Message:      llm.Message{Role: "assistant", Content: strings.Repeat("做。", 3000)},
		FinishReason: "stop",
	}, nil
}

func TestGuardAbortsRepetitionLoop(t *testing.T) {
	loop, _ := newLoop(t, spamProvider{})
	state, err := loop.Run(context.Background(), "go")
	if state != Aborted || err == nil {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

// flakyProvider fails retryably N times then succeeds.
type flakyProvider struct{ left int }

func (f *flakyProvider) Complete(_ context.Context, _ *llm.Request) (*llm.Response, error) {
	if f.left > 0 {
		f.left--
		return nil, &llm.CallError{Kind: llm.KindTransient, Message: "ECONNRESET"}
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}, FinishReason: "stop"}, nil
}

func TestTransientRetryThenComplete(t *testing.T) {
	loop, _ := newLoop(t, &flakyProvider{left: 3})
	state, err := loop.Run(context.Background(), "hi")
	if state != Completed || err != nil {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

// deadProvider always fails retryably — the loop must land in Waiting,
// never silent-idle (#13621).
type deadProvider struct{}

func (deadProvider) Complete(_ context.Context, _ *llm.Request) (*llm.Response, error) {
	return nil, &llm.CallError{Kind: llm.KindTransient, Message: "transport failed"}
}

func TestRetryBudgetExhaustedGoesWaiting(t *testing.T) {
	loop, lg := newLoop(t, deadProvider{})
	loop.Retry = llm.Policy{Retries: 1, Base: time.Millisecond, Max: 2 * time.Millisecond}
	state, _ := loop.Run(context.Background(), "hi")
	if state != Waiting {
		t.Fatalf("state=%s, want Waiting", state)
	}
	var reason string
	lg.ReadFrom(0, func(ev session.Event) bool {
		if ev.Type == "turn/end" {
			reason = string(ev.Data)
		}
		return true
	})
	if !strings.Contains(reason, "waiting") {
		t.Fatalf("turn/end=%s", reason)
	}
}

// Mailbox: a notice posted while no turn is running survives on disk and
// is delivered once (#13656).
func TestMailboxDurableReplay(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "s.jsonl")
	lg, _ := session.OpenLog(p)
	Post(lg, Notice{ID: "n1", From: "child", Kind: "completed", Payload: []byte(`{"ok":true}`)})
	lg.Close()

	lg2, _ := session.OpenLog(p)
	pend, _ := Pending(lg2)
	if len(pend) != 1 || pend[0].ID != "n1" {
		t.Fatalf("pending=%v", pend)
	}
	Delivered(lg2, "n1")
	pend, _ = Pending(lg2)
	if len(pend) != 0 {
		t.Fatal("notice not acked")
	}
}

// meterProvider reports token usage like a real API.
type meterProvider struct{ calls int }

func (m *meterProvider) Complete(_ context.Context, _ *llm.Request) (*llm.Response, error) {
	m.calls++
	return &llm.Response{
		Message:      llm.Message{Role: "assistant", Content: "ok"},
		FinishReason: "stop",
		Usage:        llm.Usage{InputTokens: 100, OutputTokens: 50},
	}, nil
}

// A session whose spend exceeds MaxSessionTokens aborts with a logged
// budget reason — the durable total, not an in-memory counter.
func TestSessionTokenBudgetAborts(t *testing.T) {
	prov := &meterProvider{}
	loop, lg := newLoop(t, prov)
	loop.Guard.MaxSessionTokens = 120 // first call spends 150
	// The turn that crossed the cap still completes its paid work…
	state, err := loop.Run(context.Background(), "hi")
	if state != Completed || err != nil {
		t.Fatalf("state=%s err=%v", state, err)
	}
	u := session.Usage(lg)
	if u.InputTokens != 100 || u.OutputTokens != 50 || u.Steps != 1 {
		t.Fatalf("usage=%v", u)
	}
	// …but the next turn aborts WITHOUT billing the provider.
	if state, _ := loop.Run(context.Background(), "again"); state != Aborted {
		t.Fatalf("second run state=%s", state)
	}
	if prov.calls != 1 {
		t.Fatalf("provider called %d times after budget exhausted", prov.calls)
	}
}
