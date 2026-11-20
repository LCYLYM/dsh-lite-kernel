package agent

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/LCYLYM/dsh-lite-kernel/session"
)

func planTool(t *testing.T) (PlanTool, *session.Log, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "s.jsonl")
	lg, err := session.OpenLog(p)
	if err != nil {
		t.Fatal(err)
	}
	return PlanTool{Log: lg}, lg, p
}

func planCall(t *testing.T, pt PlanTool, args map[string]any) []PlanStep {
	t.Helper()
	raw, _ := json.Marshal(args)
	out, err := pt.Call(context.Background(), raw)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	b, _ := json.Marshal(out.(map[string]any)["steps"])
	var steps []PlanStep
	json.Unmarshal(b, &steps)
	return steps
}

func TestPlanWriteReadPersist(t *testing.T) {
	pt, lg, path := planTool(t)
	defer lg.Close()
	planCall(t, pt, map[string]any{"action": "write", "steps": []PlanStep{
		{Text: "scan"}, {Text: "fix"}, {Text: "verify"},
	}})
	steps := planCall(t, pt, map[string]any{"action": "read"})
	if len(steps) != 3 || steps[0].Text != "scan" || steps[0].Status != "pending" {
		t.Fatalf("%v", steps)
	}
	steps = planCall(t, pt, map[string]any{"action": "complete_step", "step": 0})
	if steps[0].Status != "done" || steps[1].Status != "in_progress" {
		t.Fatalf("%v", steps)
	}
	// Plan survives a log reopen — it is durable state, not memory.
	lg.Close()
	lg2, _ := session.OpenLog(path)
	defer lg2.Close()
	pt2 := PlanTool{Log: lg2}
	steps = planCall(t, pt2, map[string]any{"action": "read"})
	if steps[0].Status != "done" {
		t.Fatal("plan lost across reopen")
	}
}

func TestPlanValidation(t *testing.T) {
	pt, lg, _ := planTool(t)
	defer lg.Close()
	raw, _ := json.Marshal(map[string]any{"action": "write", "steps": []PlanStep{{Text: "  "}}})
	if _, err := pt.Call(context.Background(), raw); err == nil {
		t.Fatal("empty step accepted")
	}
	raw, _ = json.Marshal(map[string]any{"action": "write", "steps": []PlanStep{{Text: "x", Status: "bogus"}}})
	if _, err := pt.Call(context.Background(), raw); err == nil {
		t.Fatal("bad status accepted")
	}
	raw, _ = json.Marshal(map[string]any{"action": "complete_step", "step": 9})
	if _, err := pt.Call(context.Background(), raw); err == nil {
		t.Fatal("out-of-range step accepted")
	}
	raw, _ = json.Marshal(map[string]any{"action": "nope"})
	if _, err := pt.Call(context.Background(), raw); err == nil {
		t.Fatal("bad action accepted")
	}
	raw, _ = json.Marshal(map[string]any{"action": "complete_step"})
	if _, err := pt.Call(context.Background(), raw); err == nil {
		t.Fatal("complete_step without step accepted")
	}
}

// Completing a later step must not restart an earlier pending one.
func TestPlanOutOfOrderComplete(t *testing.T) {
	pt, lg, _ := planTool(t)
	defer lg.Close()
	planCall(t, pt, map[string]any{"action": "write", "steps": []PlanStep{
		{Text: "A", Status: "pending"}, {Text: "B", Status: "in_progress"}, {Text: "C", Status: "pending"},
	}})
	steps := planCall(t, pt, map[string]any{"action": "complete_step", "step": 1})
	if steps[0].Status != "pending" || steps[1].Status != "done" || steps[2].Status != "in_progress" {
		t.Fatalf("%v", steps)
	}
}
