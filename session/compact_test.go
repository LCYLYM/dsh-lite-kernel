package session

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompactFoldsHistory(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "s.jsonl")
	lg, _ := OpenLog(p)
	for i := 0; i < 50; i++ {
		lg.Append("user/message", map[string]any{"role": "user", "content": fmt.Sprintf("msg-%d", i)})
		lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "ok"})
	}
	lg.Append("tool/call", map[string]any{"id": "c", "name": "read_file"})

	if err := Compact(lg, 80); err != nil {
		t.Fatal(err)
	}
	if lg.CompactedThrough() != 80 {
		t.Fatalf("cursor=%d", lg.CompactedThrough())
	}
	// Non-advancing compaction refused.
	if err := Compact(lg, 80); err == nil {
		t.Fatal("expected refusal")
	}

	msgs, err := BuildContext(lg, ContextBudget{MaxBytes: 1 << 20, MaxEvents: 500, SystemText: "sys"})
	if err != nil {
		t.Fatal(err)
	}
	var sys, folded int
	for _, m := range msgs {
		if m.Role == "system" {
			sys++
			if !strings.Contains(m.Content.(string), "compacted") {
				t.Fatalf("system lacks fold note: %v", m.Content)
			}
		}
		if s, ok := m.Content.(string); ok && s == "msg-10" {
			folded++
		}
	}
	if sys != 1 {
		t.Fatalf("sys=%d", sys)
	}
	if folded != 0 {
		t.Fatal("folded history leaked into context")
	}
	// The retained tail between ThroughSeq and the marker is ordinary
	// history — it must still reach the model.
	var kept int
	for _, m := range msgs {
		if s, ok := m.Content.(string); ok && s == "msg-45" {
			kept++
		}
	}
	if kept != 1 {
		t.Fatal("retained tail after ThroughSeq was dropped")
	}
}

// Compaction cursor survives reopen.
func TestCompactCursorPersists(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "s.jsonl")
	lg, _ := OpenLog(p)
	for i := 0; i < 10; i++ {
		lg.Append("user/message", map[string]any{"role": "user", "content": "x"})
	}
	Compact(lg, 5)
	lg.Close()

	lg2, err := OpenLog(p)
	if err != nil {
		t.Fatal(err)
	}
	defer lg2.Close()
	if lg2.CompactedThrough() != 5 {
		t.Fatalf("cursor=%d after reopen", lg2.CompactedThrough())
	}
}
