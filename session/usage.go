package session

import (
	"encoding/json"
	"sync"
)

// UsageTotal is the summed token spend of a session, aggregated from
// usage fields on assistant/message events — accounting rides the same
// durable log entry as the reply it paid for (atomic, no extra events).
type UsageTotal struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	Steps        int `json:"steps"` // model calls with reported usage
}

// usageState is the per-log aggregate: rebuilt once on first call,
// then advanced only over events appended since the last scan — the
// same shape as the mailbox index. drive() budgets every step against
// Usage, so a full reread would cost O(steps × history) — on a long
// session that is hundreds of megabytes of IO per turn.
type usageState struct {
	mu      sync.Mutex
	scanned int64
	u       UsageTotal
}

var usages sync.Map // *Log -> *usageState

func usageFor(lg *Log) *usageState {
	st, loaded := usages.LoadOrStore(lg, &usageState{})
	if !loaded {
		// The index must not root a closed log forever.
		lg.OnClose(func() { usages.Delete(lg) })
	}
	return st.(*usageState)
}

// Usage sums the usage fields on assistant/message events. The read
// error propagates without advancing the index: a partial total must
// never pass for a complete accounting (billing, budget checks).
func Usage(lg *Log) (UsageTotal, error) {
	st := usageFor(lg)
	st.mu.Lock()
	defer st.mu.Unlock()
	last := st.scanned
	err := lg.ReadFrom(st.scanned, func(ev Event) bool {
		last = ev.Seq
		if ev.Type != "assistant/message" {
			return true
		}
		var t struct {
			Prompt     int `json:"prompt_tokens"`
			Completion int `json:"completion_tokens"`
		}
		if json.Unmarshal(ev.Data, &t) == nil && (t.Prompt > 0 || t.Completion > 0) {
			st.u.InputTokens += t.Prompt
			st.u.OutputTokens += t.Completion
			st.u.Steps++
		}
		return true
	})
	if err != nil {
		return UsageTotal{}, err
	}
	st.scanned = last
	return st.u, nil
}
