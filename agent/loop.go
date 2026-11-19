// Package agent is the minimal agent loop: model → tools → guard, all
// state in the session log, nothing hidden in process memory.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
}

// Run executes one user turn: append the message, step the loop under the
// guard, persist every model-visible event.
func (l *Loop) Run(ctx context.Context, userText string) (TurnState, error) {
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
		msgs, err := session.BuildContext(l.Log, l.Budget)
		if err != nil {
			return Failed, err
		}
		var resp *llm.Response
		err = l.Retry.Do(ctx, func(c context.Context) error {
			var e error
			resp, e = l.Provider.Complete(c, &llm.Request{
				Model:    l.Model,
				Messages: msgs,
				Tools:    l.Registry.Specs(),
			})
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
