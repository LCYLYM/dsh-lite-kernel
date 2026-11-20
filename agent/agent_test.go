package agent

import (
	"context"
	"encoding/json"
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

// A "stop" response with no text and no tool calls is not a completed
// step (upstream #13822): retry through the policy, and if it stays
// empty the turn goes Waiting — never a silent "completed".
type emptyProvider struct{ left int }

func (e *emptyProvider) Complete(_ context.Context, req *llm.Request) (*llm.Response, error) {
	if e.left > 0 {
		e.left--
		return &llm.Response{Message: llm.Message{Role: "assistant"}, FinishReason: "stop"}, nil
	}
	if last := req.Messages[len(req.Messages)-1]; last.Role == "tool" {
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: "done"}, FinishReason: "stop"}, nil
	}
	args, _ := json.Marshal(map[string]string{"text": "x"})
	return &llm.Response{
		Message:      llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{{ID: "c1", Name: "echo", Args: args}}},
		FinishReason: "tool_calls",
	}, nil
}

func TestEmptyResponseIsRetried(t *testing.T) {
	loop, lg := newLoop(t, &emptyProvider{left: 2})
	loop.Retry = llm.Policy{Retries: 4, Base: time.Millisecond}
	state, err := loop.Run(context.Background(), "hi")
	if state != Completed || err != nil {
		t.Fatalf("state=%s err=%v", state, err)
	}
	// No empty assistant message may reach the log.
	var empty bool
	_ = lg.ReadFrom(0, func(ev session.Event) bool {
		if ev.Type != "assistant/message" {
			return true
		}
		var m struct {
			Content any `json:"content"`
		}
		if json.Unmarshal(ev.Data, &m) == nil && m.Content == nil {
			empty = true
		}
		return true
	})
	if empty {
		t.Fatal("empty assistant message was logged")
	}
}

func TestPersistentEmptyResponseGoesWaiting(t *testing.T) {
	loop, _ := newLoop(t, &emptyProvider{left: 100})
	loop.Retry = llm.Policy{Retries: 2, Base: time.Millisecond}
	state, err := loop.Run(context.Background(), "hi")
	if state != Waiting || err == nil {
		t.Fatalf("state=%s err=%v", state, err)
	}
}
