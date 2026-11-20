// Package agent is the minimal agent loop: model → tools → guard, all
// state in the session log, nothing hidden in process memory.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/session"
)

// TurnState is how a turn ended.
type TurnState string

const (
	Completed TurnState = "completed"
	Aborted   TurnState = "aborted" // guard stopped a degenerate loop
	Waiting   TurnState = "waiting" // retry budget spent; resumable
	Failed    TurnState = "failed"
)

// Loop runs turns for one session.
type Loop struct {
	Model      string
	Provider   llm.Provider
	Registry   *Registry
	Log        *session.Log
	Budget     session.ContextBudget
	Guard      Guard
	Retry      llm.Policy
	AutoResume bool // re-drive the turn after Waiting (goal runs, #13621)
	ResumeMax  int
	Stream     bool // prefer Streamer providers; deltas land in the log

	turnMu sync.Mutex // one in-flight turn per log — see Run
}

// ErrTurnBusy is returned when a second Run starts while a turn is in
// flight on the same log.
var ErrTurnBusy = errors.New("agent: turn already in progress")

// Busy reports whether a turn is in flight. It is a probe for callers
// that want to reject early; Run still enforces the exclusion itself.
func (l *Loop) Busy() bool {
	if !l.turnMu.TryLock() {
		return true
	}
	l.turnMu.Unlock()
	return false
}

// Run executes one user turn: append the message, step the loop under the
// guard, persist every model-visible event. Two concurrent Runs on the
// same log would interleave events (user B's message landing mid-turn A)
// — the second caller gets ErrTurnBusy, a caller-visible rejection rather
// than a silently corrupted history.
func (l *Loop) Run(ctx context.Context, userText string) (TurnState, error) {
	if !l.turnMu.TryLock() {
		return Failed, ErrTurnBusy
	}
	defer l.turnMu.Unlock()
	if _, err := l.Log.Append("user/message", map[string]any{
		"role": "user", "content": userText,
	}); err != nil {
		return Failed, err
	}
	state, err := l.drive(ctx)
	if state == Waiting && l.AutoResume {
		max := l.ResumeMax
		if max <= 0 {
			max = 3
		}
		for i := 0; i < max && state == Waiting; i++ {
			// Exponential wait before re-driving; the attempt is logged so
			// an idle session is never a silent one (#13621).
			l.Log.Append("session/waiting", map[string]any{
				"resume_in_seconds": 30 * (1 << i), "attempt": i + 1,
			})
			select {
			case <-ctx.Done():
				return Waiting, ctx.Err()
			case <-time.After(time.Duration(30*(1<<i)) * time.Second):
			}
			state, err = l.drive(ctx)
		}
	}
	return state, err
}

// Continue resumes after a finish=length truncation. The truncated
// assistant output is committed with truncated=true, so the next request
// carries the partial output plus a continue hint — never a stateless
// re-run (#13647).
func (l *Loop) drive(ctx context.Context) (TurnState, error) {
	zeroTool := 0
	resumedFromTrunc := false
	for step := 0; step < l.Guard.MaxSteps; step++ {
		// Check spend before billing another call — an exhausted
		// session aborts without contacting the provider (also covers
		// reopened logs, where only durable usage survives).
		if l.Guard.MaxSessionTokens > 0 {
			if u := session.Usage(l.Log); u.InputTokens+u.OutputTokens > l.Guard.MaxSessionTokens {
				l.Log.Append("turn/end", map[string]any{
					"reason": "aborted",
					"detail": fmt.Sprintf("session token budget %d exceeded (%d used)", l.Guard.MaxSessionTokens, u.InputTokens+u.OutputTokens),
				})
				return Aborted, errors.New("agent: session token budget exceeded")
			}
		}
		if err := Drain(l.Log); err != nil {
			return Failed, err
		}
		msgs, err := session.BuildContext(l.Log, l.Budget)
		if err != nil {
			return Failed, err
		}
		// Pressure compaction: when the retained history window alone
		// cannot fit the budget, fold history before it happens again
		// (deterministic — no model call, no route to misconfigure).
		if l.shouldCompact(msgs) {
			l.compact()
			if msgs, err = session.BuildContext(l.Log, l.Budget); err != nil {
				return Failed, err
			}
		}
		var resp *llm.Response
		req := &llm.Request{
			Model:    l.Model,
			Messages: msgs,
			Tools:    l.Registry.Specs(),
		}
		err = l.Retry.Do(ctx, func(c context.Context) error {
			// A stalled provider call must not park the turn until the
			// caller's outer deadline (#13621 frozen turns): bound each
			// attempt, let Retry decide whether to take another.
			if l.Guard.CallTimeout > 0 {
				var cancel context.CancelFunc
				c, cancel = context.WithTimeout(c, time.Duration(l.Guard.CallTimeout)*time.Second)
				defer cancel()
			}
			var e error
			if l.Stream {
				if st, ok := l.Provider.(llm.Streamer); ok {
					resp, e = l.streamStep(c, st, req)
				} else {
					resp, e = l.Provider.Complete(c, req)
				}
			} else {
				resp, e = l.Provider.Complete(c, req)
			}
			// A per-attempt deadline surfaced raw is a classified timeout
			// — retryable, not an opaque turn failure.
			if e != nil && c.Err() == context.DeadlineExceeded && ctx.Err() == nil {
				return &llm.CallError{Kind: llm.KindTimeout,
					Message: fmt.Sprintf("provider call exceeded %ds", l.Guard.CallTimeout)}
			}
			return e
		})
		if err != nil {
			// Retry budget spent → Waiting is an explicit, logged state.
			var ce *llm.CallError
			if llm.AsCallError(err, &ce) && ce.Retryable() {
				l.Log.Append("turn/end", map[string]any{"reason": "waiting", "error": ce.Message})
				return Waiting, err
			}
			l.Log.Append("turn/end", map[string]any{"reason": "error", "error": err.Error()})
			return Failed, err
		}

		text := contentText(resp.Message.Content)
		truncated := resp.FinishReason == "length" || resp.FinishReason == "max_tokens"
		// Usage rides on the message event itself: one atomic append —
		// a crash between the two can never lose usage a reply saved,
		// and accounting costs zero extra events in the log.
		ev := map[string]any{
			"role": "assistant", "content": resp.Message.Content,
			"tool_calls": rawCalls(resp.Message.ToolCalls),
			"finish":     resp.FinishReason, "truncated": truncated,
		}
		if resp.Usage.InputTokens > 0 || resp.Usage.OutputTokens > 0 {
			ev["prompt_tokens"] = resp.Usage.InputTokens
			ev["completion_tokens"] = resp.Usage.OutputTokens
		}
		l.Log.Append("assistant/message", ev)

		// Crossing the cap aborts before tool side effects — this
		// step's tool calls do not run. Same ordering as the guards.
		if l.Guard.MaxSessionTokens > 0 {
			if u := session.Usage(l.Log); u.InputTokens+u.OutputTokens > l.Guard.MaxSessionTokens {
				l.Log.Append("turn/end", map[string]any{
					"reason": "aborted",
					"detail": fmt.Sprintf("session token budget %d exceeded (%d used)", l.Guard.MaxSessionTokens, u.InputTokens+u.OutputTokens),
				})
				return Aborted, errors.New("agent: session token budget exceeded")
			}
		}

		// Guard: repetition / no-progress / caps.
		if abort, why := l.Guard.CheckStep(text, len(resp.Message.ToolCalls)); abort {
			l.Log.Append("turn/end", map[string]any{"reason": "aborted", "detail": why})
			return Aborted, errors.New("agent: " + why)
		}
		if len(resp.Message.ToolCalls) == 0 {
			zeroTool++
			if zeroTool >= l.Guard.MaxZeroToolSteps && !resumedFromTrunc {
				l.Log.Append("turn/end", map[string]any{"reason": "completed", "detail": "no tool calls"})
				return Completed, nil
			}
		} else {
			zeroTool = 0
		}

		for _, tc := range resp.Message.ToolCalls {
			l.Log.Append("tool/call", map[string]any{"id": tc.ID, "name": tc.Name})
			out, terr := l.Registry.Call(ctx, tc)
			if terr != nil {
				out = map[string]any{"error": terr.Error()}
			}
			out = l.capResult(out)
			l.Log.Append("tool/result", map[string]any{
				"tool_call_id": tc.ID, "name": tc.Name, "content": out, "error": terr != nil,
			})
		}

		if truncated {
			resumedFromTrunc = true
			l.Log.Append("user/message", map[string]any{
				"role": "user", "content": "Continue from where the output was cut off; do not restart.",
			})
			continue
		}
		if len(resp.Message.ToolCalls) == 0 {
			l.Log.Append("turn/end", map[string]any{"reason": "completed"})
			return Completed, nil
		}
	}
	l.Log.Append("turn/end", map[string]any{"reason": "aborted", "detail": "MaxSteps exceeded"})
	return Aborted, errors.New("agent: step budget exhausted")
}

// capResult truncates an oversized tool result before it lands in the
// log — a single runaway tool must not dump unbounded bytes into the
// durable stream (and from there into every later context). The marker
// keeps the truncation model-visible.
func (l *Loop) capResult(out any) any {
	max := l.Guard.MaxToolResultChars
	if max <= 0 {
		return out
	}
	b, err := json.Marshal(out)
	if err != nil || utf8.RuneCount(b) <= max {
		return out
	}
	runes := []rune(string(b))
	return map[string]any{
		"truncated":   true,
		"total_chars": utf8.RuneCount(b),
		"content":     string(runes[:max]),
	}
}

// shouldCompact fires when unfolded history exceeds the context scan
// window — the sign that the next request would silently drop old
// events rather than fold them.
func (l *Loop) shouldCompact(msgs []llm.Message) bool {
	window := int64(l.Budget.MaxEvents * 4)
	return l.Log.Len()-l.Log.CompactedThrough() > window
}

func (l *Loop) compact() {
	through := l.Log.Len() - int64(l.Budget.MaxEvents)
	if err := session.Compact(l.Log, through); err != nil {
		l.Log.Append("session/compaction-error", map[string]any{"error": err.Error()})
	}
}

// streamStep runs one model call through Streamer, logging throttled
// assistant/delta events so the live feed shows progress without an
// event per token (bounded log: deltas coalesce to ~4 KiB).
func (l *Loop) streamStep(ctx context.Context, st llm.Streamer, req *llm.Request) (*llm.Response, error) {
	var pending strings.Builder
	flush := func() {
		if pending.Len() == 0 {
			return
		}
		l.Log.Append("assistant/delta", map[string]any{"text": pending.String()})
		pending.Reset()
	}
	resp, err := st.Stream(ctx, req, func(d llm.Delta) error {
		if d.Text != "" {
			pending.WriteString(d.Text)
			if pending.Len() >= 4096 {
				flush()
			}
		}
		if d.ToolName != "" {
			flush()
			l.Log.Append("assistant/tool-intent", map[string]any{"name": d.ToolName, "index": d.CallIndex})
		}
		return ctx.Err()
	})
	flush()
	return resp, err
}

func contentText(c any) string {
	switch v := c.(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func rawCalls(tcs []llm.ToolCall) json.RawMessage {
	if len(tcs) == 0 {
		return nil
	}
	b, _ := json.Marshal(tcs)
	return b
}

// ErrNoProvider is a configuration error: a turn was attempted with no
// model route. Configuration must fail loud at load, not mid-turn
// (#13719) — validating route keys is the caller's job at setup.
var ErrNoProvider = fmt.Errorf("agent: no model provider configured")
