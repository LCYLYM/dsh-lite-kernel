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
	used := 0
	count := 0
	// Tail-scan: read the affordable window, iterate backward.
	tail, err := lg.Tail(b.MaxEvents * 4) // read a window wider than we keep
	if err != nil {
		return nil, err
	}
	var rev []llm.Message
	// The newest compaction marker is the history floor: events with
	// seq <= ThroughSeq are folded; the retained tail between ThroughSeq
	// and the marker is ordinary history and must still be kept. The
	// floor comes from the log's in-memory cursor, so the summary is
	// available even when the budget exhausts before the scan reaches
	// the marker.
	floor := lg.CompactedThrough()
	comp := lg.NewestCompaction()
	seenNotices := map[string]bool{}
	for i := len(tail) - 1; i >= 0 && count < b.MaxEvents && used < b.MaxBytes; i-- {
		ev := tail[i]
		if ev.Seq <= floor {
			break
		}
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
		// Charge the wire message, not the log event — logged fields a
		// provider never sees (usage counters, finish markers) must not
		// evict history.
		size := len(ev.Data)
		if w, err := json.Marshal(m); err == nil {
			size = len(w)
		}
		if used+size > b.MaxBytes && len(rev) > 0 {
			break
		}
		used += size
		count++
		rev = append(rev, m)
	}
	for i := len(rev) - 1; i >= 0; i-- {
		msgs = append(msgs, rev[i])
	}
	sys := b.SystemText
	if comp != nil {
		sys += fmt.Sprintf("\n[earlier history compacted: %d events folded at seq %d; first request was %q; tools used so far: %s]",
			comp.Dropped, comp.ThroughSeq, comp.FirstUser, strings.Join(comp.ToolNames, ", "))
	}
	if sys != "" {
		msgs = append([]llm.Message{{Role: "system", Content: sys}}, msgs...)
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
