package session

import (
	"encoding/json"

	"github.com/LCYLYM/dsh-lite-kernel/llm"
)

// ContextBudget bounds how much history a model request may carry. The
// upstream failure this prevents: a seeded/long session stuffing its
// entire inherited log into the first request (8 MiB → 413 → permanent
// deadlock, discussion #13717) and unbounded heap growth on resume
// (#13639).
type ContextBudget struct {
	MaxBytes   int // total serialized bytes across returned messages
	MaxEvents  int // hard cap on history length
	SystemText string
}

func DefaultBudget() ContextBudget {
	return ContextBudget{MaxBytes: 512 << 10, MaxEvents: 200}
}

// messageEvent mirrors the payload of log events that carry chat content.
type messageEvent struct {
	Role       string          `json:"role"`
	Content    any             `json:"content"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	Finish     string          `json:"finish,omitempty"`
	Truncated  bool            `json:"truncated,omitempty"`
}

// BuildContext assembles the request's message list by walking the log
// tail backward until the byte budget is spent — cost is O(events kept),
// never O(history size). Model-visible ⇔ logged: every returned message
// came from a logged event, and a truncated assistant turn is carried
// with its partial content plus a continuation hint (fixes the
// stateless-"continue" re-run loop, discussion #13647).
func BuildContext(lg *Log, b ContextBudget) ([]llm.Message, error) {
	var msgs []llm.Message
	used := 0
	count := 0
	// Tail-scan: read the affordable window, iterate backward.
	tail, err := lg.Tail(b.MaxEvents * 4) // read a window wider than we keep
	if err != nil {
		return nil, err
	}
	var rev []llm.Message
	for i := len(tail) - 1; i >= 0 && count < b.MaxEvents && used < b.MaxBytes; i-- {
		ev := tail[i]
		var me messageEvent
		var m llm.Message
		switch ev.Type {
		case "user/message":
			if err := json.Unmarshal(ev.Data, &me); err != nil {
				continue
			}
			// Retry markers are control metadata, never serialized as empty
			// user messages — that poisons strict providers (#13715).
			if me.Role == "" || me.Content == nil {
				continue
			}
			m = llm.Message{Role: "user", Content: me.Content}
		case "assistant/message":
			if err := json.Unmarshal(ev.Data, &me); err != nil {
				continue
			}
			m = llm.Message{Role: "assistant", Content: me.Content}
			if len(me.ToolCalls) > 0 {
				var tcs []llm.ToolCall
				if err := json.Unmarshal(me.ToolCalls, &tcs); err == nil {
					m.ToolCalls = tcs
				}
			}
			if me.Truncated {
				m.Content = appendText(m.Content, "\n[output was truncated at the token limit; continue from where it stopped, do not restart]")
			}
		case "tool/result":
			if err := json.Unmarshal(ev.Data, &me); err != nil {
				continue
			}
			m = llm.Message{Role: "tool", Content: me.Content, ToolCallID: me.ToolCallID}
		default:
			continue
		}
		size := len(ev.Data)
		if used+size > b.MaxBytes && len(rev) > 0 {
			break
		}
		used += size
		count++
		rev = append(rev, m)
	}
	// A turn that crashed between the assistant tool_call and its
	// tool/result leaves a dangling call in the log; strict providers
	// require a result per call and would reject every later request,
	// bricking the session (upstream #13790). Synthesize an
	// interrupted-marker result directly after the call, bounded by a
	// small synthesis reserve; a call that would not fit is dropped
	// from the message instead so the request stays valid.
	results := map[string]bool{}
	for _, m := range rev {
		if m.Role == "tool" {
			results[m.ToolCallID] = true
		}
	}
	// The synthesis reserve is only the budget history did not spend:
	// history + synthesized results together stay within both limits.
	synthBytes, synthCount := 0, 0
	synthByteCap := b.MaxBytes - used
	if synthByteCap > 64<<10 {
		synthByteCap = 64 << 10
	}
	if synthByteCap < 0 {
		synthByteCap = 0
	}
	synthMax := b.MaxEvents - count
	if synthMax < 0 {
		synthMax = 0
	}
	for i := len(rev) - 1; i >= 0; i-- {
		m := rev[i]
		if m.Role != "assistant" || len(m.ToolCalls) == 0 {
			msgs = append(msgs, m)
			continue
		}
		var keptCalls []llm.ToolCall
		var synths []llm.Message
		for _, tc := range m.ToolCalls {
			if results[tc.ID] {
				keptCalls = append(keptCalls, tc)
				continue
			}
			est := len(tc.ID) + 128
			if synthBytes+est > synthByteCap || synthCount+1 > synthMax {
				continue // drop the call rather than exceed the reserve
			}
			synthBytes += est
			synthCount++
			synths = append(synths, llm.Message{
				Role:       "tool",
				ToolCallID: tc.ID,
				Content:    "[tool call was interrupted before it produced a result]",
			})
			keptCalls = append(keptCalls, tc)
		}
		m.ToolCalls = keptCalls
		if len(keptCalls) == 0 && m.Content == nil {
			continue // an empty assistant carries nothing the model needs
		}
		msgs = append(msgs, m)
		msgs = append(msgs, synths...)
	}
	if b.SystemText != "" {
		msgs = append([]llm.Message{{Role: "system", Content: b.SystemText}}, msgs...)
	}
	return msgs, nil
}

func appendText(c any, suffix string) any {
	if s, ok := c.(string); ok {
		return s + suffix
	}
	if c == nil {
		return "[truncated]" + suffix
	}
	return c
}
