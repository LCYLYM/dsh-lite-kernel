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
