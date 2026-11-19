package session

import (
	"encoding/json"
	"fmt"
	"strings"

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
	var rev []llm.Message
	used := 0
	count := 0
	// Tail-scan: read a window wider than we keep, iterate backward. The
	// window widens while it fills with non-rendered control events —
	// mailbox/delivered acknowledgements must never crowd the injected
	// notices they acknowledge out of context (a flooded mailbox would
	// otherwise leave the model blind to input already acked).
	seenNotices := map[string]bool{}
	for window := b.MaxEvents * 4; ; window *= 2 {
		if window > int(lg.Len()) {
			window = int(lg.Len())
		}
		tail, err := lg.Tail(window)
		if err != nil {
			return nil, err
		}
		rev, used, count = rev[:0], 0, 0
		for k := range seenNotices {
			delete(seenNotices, k)
		}
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
			case "mailbox/inject":
				var inj struct {
					Notices []struct {
						ID      string          `json:"id"`
						From    string          `json:"from"`
						Kind    string          `json:"kind"`
						Payload json.RawMessage `json:"payload"`
					} `json:"notices"`
				}
				if err := json.Unmarshal(ev.Data, &inj); err != nil || len(inj.Notices) == 0 {
					continue
				}
				var sb strings.Builder
				for _, n := range inj.Notices {
					// A crash between inject and ack reinjects the notice on
					// resume — render each id once so duplicates stay invisible.
					// ID-less notices are never deduped: "" would merge them all.
					if n.ID != "" {
						if seenNotices[n.ID] {
							continue
						}
						seenNotices[n.ID] = true
					}
					fmt.Fprintf(&sb, "[mailbox notice from %s (%s)] %s\n", n.From, n.Kind, n.Payload)
				}
				if sb.Len() == 0 {
					continue
				}
				m = llm.Message{Role: "user", Content: strings.TrimRight(sb.String(), "\n")}
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
		if len(tail) < window || count >= b.MaxEvents || used >= b.MaxBytes || window >= int(lg.Len()) {
			break
		}
	}
	for i := len(rev) - 1; i >= 0; i-- {
		msgs = append(msgs, rev[i])
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
