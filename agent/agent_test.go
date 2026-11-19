package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

// slowProvider holds the first call until released.
type slowProvider struct{ gate chan struct{} }

func (p *slowProvider) Complete(ctx context.Context, r *llm.Request) (*llm.Response, error) {
	select {
	case <-p.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}, FinishReason: "stop"}, nil
}

func TestConcurrentRunRejected(t *testing.T) {
	gate := make(chan struct{})
	loop, _ := newLoop(t, &slowProvider{gate: gate})
	go loop.Run(context.Background(), "first")
	time.Sleep(50 * time.Millisecond)
	state, err := loop.Run(context.Background(), "second")
	if !errors.Is(err, ErrTurnBusy) || state != Failed {
		t.Fatalf("second run: state=%s err=%v", state, err)
	}
	close(gate)
}

// capProvider records the message list it is asked to complete.
type capProvider struct{ msgs []llm.Message }

func (p *capProvider) Complete(ctx context.Context, r *llm.Request) (*llm.Response, error) {
	p.msgs = append([]llm.Message(nil), r.Messages...)
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}, FinishReason: "stop"}, nil
}

func TestMailboxNoticesReachNextRequest(t *testing.T) {
	prov := &capProvider{}
	loop, lg := newLoop(t, prov)
	if err := Post(lg, Notice{ID: "n1", From: "child-1", Kind: "done",
		Payload: json.RawMessage(`{"result":42}`)}); err != nil {
		t.Fatal(err)
	}
	state, err := loop.Run(context.Background(), "hello")
	if err != nil || state != Completed {
		t.Fatalf("state=%s err=%v", state, err)
	}
	found := false
	for _, m := range prov.msgs {
		if s, ok := m.Content.(string); ok && strings.Contains(s, "mailbox notice from child-1") && strings.Contains(s, `"result":42`) {
			found = true
		}
	}
	if !found {
		t.Fatal("notice never reached the model request")
	}
	if p, _ := Pending(lg); len(p) != 0 {
		t.Fatal("notice left pending")
	}
	// replay on resume injects nothing new — delivered is durable
	msgs2, _ := session.BuildContext(lg, loop.Budget)
	seen := 0
	for _, m := range msgs2 {
		if s, ok := m.Content.(string); ok && strings.Contains(s, "mailbox notice") {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("expected exactly one injected notice, got %d", seen)
	}
}

func TestDrainCrashBeforeAckReinjectsInvisible(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.jsonl")
	lg, _ := session.OpenLog(path)
	Post(lg, Notice{ID: "n1", From: "c", Kind: "done", Payload: json.RawMessage(`1`)})
	if err := Drain(lg); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after inject but before ack: strip the delivered
	// events by rewriting the log without them, then reopen.
	f, _ := os.ReadFile(path)
	kept := []byte{}
	for _, line := range bytes.Split(f, []byte("\n")) {
		if len(line) > 0 && !bytes.Contains(line, []byte("delivered")) {
			kept = append(append(kept, line...), '\n')
		}
	}
	os.WriteFile(path, kept, 0o600)
	lg2, _ := session.OpenLog(path)
	if err := Drain(lg2); err != nil {
		t.Fatal(err)
	}
	// notice is injected again, but BuildContext dedups by id
	msgs, _ := session.BuildContext(lg2, session.DefaultBudget())
	seen := 0
	for _, m := range msgs {
		if s, ok := m.Content.(string); ok && strings.Contains(s, "mailbox notice from c") {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("notice rendered %d times after reinject", seen)
	}
}

func TestDrainBatchCapLeavesRestPending(t *testing.T) {
	dir := t.TempDir()
	lg, _ := session.OpenLog(filepath.Join(dir, "s.jsonl"))
	for i := 0; i < maxDrainBatch+3; i++ {
		Post(lg, Notice{ID: fmt.Sprintf("n%d", i), From: "c", Kind: "k", Payload: json.RawMessage(`1`)})
	}
	if err := Drain(lg); err != nil {
		t.Fatal(err)
	}
	mb := mailboxFor(lg)
	if len(mb.pending) != 3 {
		t.Fatalf("expected 3 pending after capped drain, got %d", len(mb.pending))
	}
	if err := Drain(lg); err != nil {
		t.Fatal(err)
	}
	if len(mb.pending) != 0 {
		t.Fatal("remaining notices not drained")
	}
}

func TestMailboxIndexReleasedOnClose(t *testing.T) {
	dir := t.TempDir()
	lg, _ := session.OpenLog(filepath.Join(dir, "s.jsonl"))
	if err := Drain(lg); err != nil {
		t.Fatal(err)
	}
	if _, ok := mailboxes.Load(lg); !ok {
		t.Fatal("index missing after drain")
	}
	lg.Close()
	if _, ok := mailboxes.Load(lg); ok {
		t.Fatal("index rooted a closed log")
	}
}

func TestIDlessNoticesAllRendered(t *testing.T) {
	dir := t.TempDir()
	lg, _ := session.OpenLog(filepath.Join(dir, "s.jsonl"))
	// Two injections carrying ID-less notices — both must render.
	lg.Append("mailbox/inject", map[string]any{"notices": []map[string]any{
		{"from": "a", "kind": "k", "payload": json.RawMessage(`1`)},
		{"from": "b", "kind": "k", "payload": json.RawMessage(`2`)},
	}})
	msgs, _ := session.BuildContext(lg, session.DefaultBudget())
	found := 0
	for _, m := range msgs {
		if s, ok := m.Content.(string); ok {
			found += strings.Count(s, "[mailbox notice")
		}
	}
	if found != 2 {
		t.Fatalf("id-less notices collapsed: %d rendered", found)
	}
}
