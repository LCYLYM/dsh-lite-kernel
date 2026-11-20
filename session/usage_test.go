package session

import (
	"path/filepath"
	"testing"
)

func TestUsageTotals(t *testing.T) {
	lg, _ := OpenLog(filepath.Join(t.TempDir(), "s.jsonl"))
	defer lg.Close()
	lg.Append("user/message", map[string]any{"role": "user", "content": "x"})
	lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "a", "prompt_tokens": 10, "completion_tokens": 5})
	lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "b", "prompt_tokens": 20, "completion_tokens": 7})
	lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "c"})
	u, err := Usage(lg)
	if err != nil {
		t.Fatal(err)
	}
	if u.InputTokens != 30 || u.OutputTokens != 12 || u.Steps != 2 {
		t.Fatalf("%v", u)
	}
}

// The index must not drop late events: a second call sees only what
// was appended in between.
func TestUsageIncremental(t *testing.T) {
	lg, _ := OpenLog(filepath.Join(t.TempDir(), "s.jsonl"))
	defer lg.Close()
	lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "a", "prompt_tokens": 10})
	if _, err := Usage(lg); err != nil {
		t.Fatal(err)
	}
	lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "b", "completion_tokens": 4})
	u, err := Usage(lg)
	if err != nil {
		t.Fatal(err)
	}
	if u.InputTokens != 10 || u.OutputTokens != 4 || u.Steps != 2 {
		t.Fatalf("%v", u)
	}
}

// A reopened log carries its own index — the total recounts from the
// durable events, not from the old log object's cache.
func TestUsageAfterReopen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.jsonl")
	lg, _ := OpenLog(p)
	lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "a", "prompt_tokens": 10})
	u, err := Usage(lg)
	if err != nil || u.InputTokens != 10 {
		t.Fatalf("%v %v", u, err)
	}
	lg.Close()
	lg2, _ := OpenLog(p)
	defer lg2.Close()
	lg2.Append("assistant/message", map[string]any{"role": "assistant", "content": "b", "prompt_tokens": 5})
	u2, err := Usage(lg2)
	if err != nil {
		t.Fatal(err)
	}
	if u2.InputTokens != 15 || u2.Steps != 2 {
		t.Fatalf("%v", u2)
	}
}
