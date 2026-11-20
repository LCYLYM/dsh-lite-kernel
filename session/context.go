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
	if b.MaxEvents <= 0 {
		// No history fits — the zero window could never widen out of
		// itself, so don't enter the widening loop at all.
		if b.SystemText != "" {
			return []llm.Message{{Role: "system", Content: b.SystemText}}, nil
		}
		return msgs, nil
	}
	var rev []llm.Message
	used := 0
	count := 0
	// The newest compaction marker is the history floor: events with
	// seq <= ThroughSeq are folded; the retained tail between ThroughSeq
	// and the marker is ordinary history and must still be kept. The
	// floor comes from the log's in-memory cursor, so the summary is
	// available even when the budget exhausts before the scan reaches
	// the marker.
	floor := lg.CompactedThrough()
	comp := lg.NewestCompaction()
	seenNotices := map[string]bool{}
	// Tail-scan: read a window wider than we keep, iterate backward. The
	// window widens while it fills with non-rendered control events —
	// mailbox/delivered acknowledgements must never crowd the injected
	// notices they acknowledge out of context (a flooded mailbox would
	// otherwise leave the model blind to input already acked).
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
		var i int
		for i = len(tail) - 1; i >= 0 && count < b.MaxEvents && used < b.MaxBytes; i-- {
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
		// Exhausting the window before the budget filled (cap hit
		// mid-window, i >= 0) means older history could not fit anyway —
		// widening would just load the entire log.
		if i >= 0 || len(tail) < window || window >= int(lg.Len()) {
			break
		}
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
	// rev is newest→oldest; emit oldest→newest, synthesizing a result for
	// every tool call the log never answered (#13790).
	var filtered []llm.Message
	for i := len(rev) - 1; i >= 0; i-- {
		m := rev[i]
		if m.Role != "assistant" || len(m.ToolCalls) == 0 {
			filtered = append(filtered, m)
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
		filtered = append(filtered, m)
		filtered = append(filtered, synths...)
	}
	// A backward scan can cut between an assistant tool_call and its
	// tool/result (the result is newer, so it survives while the call is
	// evicted). An orphaned tool message hard-fails strict providers, so
	// drop results whose call is not in the window.
	ids := map[string]bool{}
	for _, m := range filtered {
		for _, tc := range m.ToolCalls {
			ids[tc.ID] = true
		}
	}
	for _, m := range filtered {
		if m.Role == "tool" && !ids[m.ToolCallID] {
			continue
		}
		msgs = append(msgs, m)
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
