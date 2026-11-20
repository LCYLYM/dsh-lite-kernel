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
	// No empty assistant message (no content and no tool calls) may reach the log.
	var empty bool
	_ = lg.ReadFrom(0, func(ev session.Event) bool {
		if ev.Type != "assistant/message" {
			return true
		}
		var m struct {
			Content   any             `json:"content"`
			ToolCalls json.RawMessage `json:"tool_calls"`
		}
		if json.Unmarshal(ev.Data, &m) == nil && m.Content == nil && len(m.ToolCalls) == 0 {
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

// Reasoning-only content blocks (upstream #13822's actual shape) count
// as an empty response too — JSON-serializing the blocks must not pass
// the blank check.
type reasoningProvider struct{ called int }

func (r *reasoningProvider) Complete(_ context.Context, req *llm.Request) (*llm.Response, error) {
	r.called++
	if last := req.Messages[len(req.Messages)-1]; last.Role == "tool" {
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: "done"}, FinishReason: "stop"}, nil
	}
	if r.called == 1 {
		return &llm.Response{
			Message:      llm.Message{Role: "assistant", Content: []any{map[string]any{"type": "reasoning", "reasoning": "draft"}}},
			FinishReason: "stop",
		}, nil
	}
	args, _ := json.Marshal(map[string]string{"text": "x"})
	return &llm.Response{
		Message:      llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{{ID: "c1", Name: "echo", Args: args}}},
		FinishReason: "tool_calls",
	}, nil
}

func TestReasoningOnlyResponseIsRetried(t *testing.T) {
	loop, _ := newLoop(t, &reasoningProvider{})
	loop.Retry = llm.Policy{Retries: 2, Base: time.Millisecond}
	state, err := loop.Run(context.Background(), "hi")
	if state != Completed || err != nil {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

// A same-named tool in a child scope shadows the parent's: Call resolves
// the shadowing one, and Specs advertises it only once.
func TestRegistryChildShadowsParentTool(t *testing.T) {
	root := kernel.NewRoot("root")
	parent := NewRegistry(root, 0)
	parent.Register(tools.Echo{})

	child, err := root.Child("agent")
	if err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry(child, 0)
	reg.Register(tools.WriteFile{W: tools.Workspace{Root: t.TempDir()}})

	specs := reg.Specs()
	names := map[string]int{}
	for _, s := range specs {
		names[s.Name]++
	}
	if names["echo"] != 1 || names["write_file"] != 1 {
		t.Fatalf("unexpected specs: %v", names)
	}

	// Register a shadowing echo in the child: still one spec, resolved child-first.
	root2 := kernel.NewRoot("root2")
	p2 := NewRegistry(root2, 0)
	p2.Register(tools.Echo{})
	c2, _ := root2.Child("agent")
	reg2 := NewRegistry(c2, 0)
	reg2.Register(tools.Echo{})
	specs2 := reg2.Specs()
	count := 0
	for _, s := range specs2 {
		if s.Name == "echo" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("shadowed tool advertised %d times", count)
	}
}

// bigTool returns an oversized result.
type bigTool struct{}

func (bigTool) Name() string        { return "big" }
func (bigTool) Description() string { return "returns a huge payload" }
func (bigTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{}}`)
}
func (bigTool) Call(ctx context.Context, args json.RawMessage) (any, error) {
	return strings.Repeat("x", 200<<10), nil
}

// bigProvider asks for bigTool once, then finishes.
type bigProvider struct{ calls int }

func (p *bigProvider) Complete(ctx context.Context, r *llm.Request) (*llm.Response, error) {
	p.calls++
	if p.calls == 1 {
		return &llm.Response{Message: llm.Message{Role: "assistant",
			ToolCalls: []llm.ToolCall{{ID: "t1", Name: "big"}}}, FinishReason: "tool_calls"}, nil
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "done"}, FinishReason: "stop"}, nil
}

func TestToolResultCappedBeforeLog(t *testing.T) {
	prov := &bigProvider{}
	loop, lg := newLoop(t, prov)
	loop.Registry.Register(bigTool{})
	loop.Guard.MaxToolResultChars = 1024
	state, err := loop.Run(context.Background(), "go")
	if err != nil || state != Completed {
		t.Fatalf("state=%s err=%v", state, err)
	}
	var result map[string]any
	lg.ReadFrom(0, func(ev session.Event) bool {
		if ev.Type == "tool/result" {
			json.Unmarshal(ev.Data, &result)
		}
		return true
	})
	inner, _ := result["content"].(map[string]any)
	if inner["truncated"] != true {
		t.Fatalf("result not marked truncated: %v", result)
	}
	if got := len(inner["content"].(string)); got != 1024 {
		t.Fatalf("cap wrong: %d", got)
	}
}

// gateProvider blocks until the gate closes or ctx dies.
type gateProvider struct{ gate chan struct{} }

func (p *gateProvider) Complete(ctx context.Context, r *llm.Request) (*llm.Response, error) {
	select {
	case <-p.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ok"}, FinishReason: "stop"}, nil
}

func TestCallTimeoutClassifiedRetryable(t *testing.T) {
	gate := make(chan struct{})
	loop, _ := newLoop(t, &gateProvider{gate: gate})
	loop.Guard.CallTimeout = 1
	loop.Retry = llm.Policy{Retries: 0}
	defer close(gate)
	state, err := loop.Run(context.Background(), "hi")
	if state != Waiting || !strings.Contains(err.Error(), "exceeded 1s") {
		t.Fatalf("state=%s err=%v — timeout should be a retryable waiting turn", state, err)
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
	// The step that crossed the cap aborts before tool side effects.
	state, err := loop.Run(context.Background(), "hi")
	if state != Aborted || err == nil {
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
