package session

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Compaction is the bounded-memory answer for sessions that outlive
// their context budget: old events are folded into one
// "session/compaction" event that BuildContext treats as the history
// floor. The log itself is append-only — compaction never rewrites or
// deletes events; the marker just changes what gets read back.
//
// Upstream ties this to a model summarizer whose route key could be
// missing (fails late, masked — discussion #13719) and whose request
// itself could blow context. The default here is deterministic: no model
// call, no configuration to mispoint, always terminates.
type Compaction struct {
	ThroughSeq int64    `json:"through_seq"` // events <= this are folded
	Dropped    int      `json:"dropped"`
	FirstUser  string   `json:"first_user,omitempty"`  // earliest user text, truncated
	ToolNames  []string `json:"tool_names,omitempty"`  // distinct tools used, sorted
	RecentTurn string   `json:"recent_turn,omitempty"` // last turn's ending state
}

// Compact scans the log once and appends a marker folding every
// message/tool event with seq <= throughSeq. Callers pick throughSeq
// (typically "everything before the tail window"); repeated calls are
// monotonic — a marker that wouldn't advance is refused.
func Compact(lg *Log, throughSeq int64) error {
	if throughSeq <= lg.CompactedThrough() {
		return fmt.Errorf("session: compaction through %d does not advance", throughSeq)
	}
	var c Compaction
	c.ThroughSeq = throughSeq
	tools := map[string]bool{}
	err := lg.ReadFrom(0, func(ev Event) bool {
		if ev.Seq > throughSeq {
			return false
		}
		switch ev.Type {
		case "user/message", "assistant/message", "tool/result", "tool/call":
			c.Dropped++
			if ev.Type == "user/message" && c.FirstUser == "" {
				var me messageEvent
				if json.Unmarshal(ev.Data, &me) == nil {
					c.FirstUser = truncate(firstLine(fmt.Sprint(me.Content)), 240)
				}
			}
			if ev.Type == "tool/call" {
				var d struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(ev.Data, &d) == nil && d.Name != "" {
					tools[d.Name] = true
				}
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	for name := range tools {
		c.ToolNames = append(c.ToolNames, name)
	}
	_, err = lg.Append("session/compaction", c)
	return err
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
