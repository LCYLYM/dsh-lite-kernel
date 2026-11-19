package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/LCYLYM/dsh-lite-kernel/session"
)

// planMu serializes plan read-modify-append transactions — the log's
// own lock is released between ReadFrom and Append, so two concurrent
// complete_step calls could otherwise both read the same state and the
// later snapshot would discard the earlier completion.
var planMu sync.Mutex

// PlanStep is one item of the agent's working plan.
type PlanStep struct {
	Text   string `json:"text"`
	Status string `json:"status"` // "pending" | "in_progress" | "done"
}

// PlanTool keeps the agent's task list in the session log — the same
// model-visible ⇔ logged rule as everything else. Upstream keeps this
// as a todo_write tool; here the plan survives restarts and is visible
// on the event feed for free, because the durable store is the log.
type PlanTool struct{ Log *session.Log }

func (t PlanTool) Name() string { return "plan" }
func (t PlanTool) Description() string {
	return "Track multi-step work: replace or update the current plan"
}
func (t PlanTool) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"action":{"type":"string","enum":["read","write","complete_step"]},"steps":{"type":"array","items":{"type":"object","properties":{"text":{"type":"string"},"status":{"type":"string","enum":["pending","in_progress","done"]}},"required":["text"]}},"step":{"type":"integer","description":"index of the step to complete; required for complete_step"}},"required":["action"]}`)
}

// Call implements the plan actions:
//
//	read          — return the current plan
//	write         — replace the plan (logged as plan/state)
//	complete_step — mark step N done and the next pending step
//	                in_progress (also a plan/state append)
func (t PlanTool) Call(_ context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Action string     `json:"action"`
		Steps  []PlanStep `json:"steps"`
		Step   *int       `json:"step"` // required for complete_step
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	planMu.Lock()
	defer planMu.Unlock()
	steps := readPlan(t.Log)
	switch a.Action {
	case "read":
		return map[string]any{"steps": steps}, nil
	case "write":
		for i, s := range a.Steps {
			if strings.TrimSpace(s.Text) == "" {
				return nil, fmt.Errorf("agent: plan step %d is empty", i)
			}
			if s.Status == "" {
				a.Steps[i].Status = "pending"
			}
			switch a.Steps[i].Status {
			case "pending", "in_progress", "done":
			default:
				return nil, fmt.Errorf("agent: plan step %d has bad status %q", i, s.Status)
			}
		}
		steps = a.Steps
	case "complete_step":
		if a.Step == nil {
			return nil, fmt.Errorf("agent: complete_step requires step")
		}
		n := *a.Step
		if n < 0 || n >= len(steps) {
			return nil, fmt.Errorf("agent: plan step %d out of range (%d steps)", n, len(steps))
		}
		steps[n].Status = "done"
		for i := range steps {
			if steps[i].Status == "in_progress" {
				steps[i].Status = "pending"
			}
		}
		// Advance to the next pending step *after* n — completing a later
		// step must not restart an earlier one.
		for i := n + 1; i < len(steps); i++ {
			if steps[i].Status == "pending" {
				steps[i].Status = "in_progress"
				break
			}
		}
	default:
		return nil, fmt.Errorf("agent: unknown plan action %q", a.Action)
	}
	if a.Action != "read" {
		if _, err := t.Log.Append("plan/state", map[string]any{"steps": steps}); err != nil {
			return nil, err
		}
	}
	return map[string]any{"steps": steps}, nil
}

// readPlan returns the newest logged plan — O(tail), newest wins since
// the log is append-only.
func readPlan(lg *session.Log) []PlanStep {
	var out []PlanStep
	_ = lg.ReadFrom(0, func(ev session.Event) bool {
		if ev.Type == "plan/state" {
			var p struct {
				Steps []PlanStep `json:"steps"`
			}
			if json.Unmarshal(ev.Data, &p) == nil {
				out = p.Steps
			}
		}
		return true
	})
	return out
}
