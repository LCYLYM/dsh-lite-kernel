package session

import "encoding/json"

// UsageTotal is the summed token spend of a session, aggregated from
// turn/usage events — accounting stays in the durable log like
// everything else (no side database to lose).
type UsageTotal struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	Steps        int `json:"steps"` // model calls with reported usage
}

// Usage sums turn/usage events — O(log), only over the offset index.
func Usage(lg *Log) UsageTotal {
	var u UsageTotal
	_ = lg.ReadFrom(0, func(ev Event) bool {
		if ev.Type != "turn/usage" {
			return true
		}
		var t struct {
			Prompt     int `json:"prompt_tokens"`
			Completion int `json:"completion_tokens"`
		}
		if json.Unmarshal(ev.Data, &t) == nil {
			u.InputTokens += t.Prompt
			u.OutputTokens += t.Completion
			u.Steps++
		}
		return true
	})
	return u
}
