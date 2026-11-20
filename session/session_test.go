package session

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestLogAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	lg, err := OpenLog(filepath.Join(dir, "s.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		lg.Append("user/message", map[string]any{"role": "user", "content": strings.Repeat("x", 100)})
	}
	lg.Close()

	lg2, err := OpenLog(filepath.Join(dir, "s.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer lg2.Close()
	if lg2.Len() != 100 {
		t.Fatalf("len=%d", lg2.Len())
	}
	tail, _ := lg2.Tail(5)
	if len(tail) != 5 || tail[4].Seq != 100 {
		t.Fatalf("bad tail %v", tail)
	}
	n := 0
	lg2.ReadFrom(50, func(ev Event) bool { n++; return true })
	if n != 50 {
		t.Fatalf("readfrom got %d", n)
	}
}

// Concurrent CreateOrReuseBlank must mint exactly one blank (#13646).
func TestCreateOrReuseBlankRace(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewStore(dir)
	const n = 32
	var wg sync.WaitGroup
	ids := make(chan string, n)
	reused := make(chan bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ses, r, err := st.CreateOrReuseBlank("ws")
			if err != nil {
				t.Error(err)
				return
			}
			ids <- ses.ID
			reused <- r
		}()
	}
	wg.Wait()
	close(ids)
	uniq := map[string]bool{}
	for id := range ids {
		uniq[id] = true
	}
	if len(uniq) != 1 {
		t.Fatalf("minted %d distinct blanks", len(uniq))
	}
}

// Context assembly must stay under the byte budget on a long log
// (#13639, #13717).
func TestBuildContextBounded(t *testing.T) {
	dir := t.TempDir()
	lg, _ := OpenLog(filepath.Join(dir, "s.jsonl"))
	for i := 0; i < 2000; i++ {
		lg.Append("user/message", map[string]any{"role": "user", "content": strings.Repeat("m", 500)})
		lg.Append("assistant/message", map[string]any{"role": "assistant", "content": strings.Repeat("a", 500)})
	}
	b := ContextBudget{MaxBytes: 64 << 10, MaxEvents: 100}
	msgs, err := BuildContext(lg, b)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, m := range msgs {
		if s, ok := m.Content.(string); ok {
			total += len(s)
		}
	}
	if total > b.MaxBytes*2 { // slack for event framing overhead
		t.Fatalf("context not bounded: %d bytes", total)
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].Role != "assistant" {
		t.Fatal("newest event missing")
	}
}

// Empty retry markers must never reach the model request (#13715).
func TestRetryMarkerSkipped(t *testing.T) {
	dir := t.TempDir()
	lg, _ := OpenLog(filepath.Join(dir, "s.jsonl"))
	lg.Append("user/message", map[string]any{"role": "user", "content": "hi"})
	lg.Append("user/message", map[string]any{"role": "user", "content": nil, "source": "retry"})
	lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "ok"})
	msgs, _ := BuildContext(lg, ContextBudget{MaxBytes: 1 << 20, MaxEvents: 100})
	if len(msgs) != 2 {
		t.Fatalf("got %d msgs, want 2 (marker skipped)", len(msgs))
	}
}

func TestReadFromSurfacesCorruption(t *testing.T) {
	dir := t.TempDir()
	lg, err := OpenLog(filepath.Join(dir, "x.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lg.Append("user/message", map[string]any{"role": "user", "content": "a"})
	lg.Close()
	// corrupt a line in the middle, keep a valid tail
	f, _ := os.ReadFile(filepath.Join(dir, "x.jsonl"))
	lines := strings.SplitN(string(f), "\n", 2)
	f2, _ := os.Create(filepath.Join(dir, "x.jsonl"))
	f2.WriteString("not-json\n" + lines[1])
	f2.Close()
	lg2, err := OpenLog(filepath.Join(dir, "x.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer lg2.Close()
	if err := lg2.ReadFrom(0, func(Event) bool { return true }); err == nil ||
		!strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("mid-log corruption not surfaced: %v", err)
	}
	// a partial last line (crash mid-append) is tolerated
	os.WriteFile(filepath.Join(dir, "y.jsonl"), []byte(`{"seq":1,"type":"user/message","data":{},"ts":1}
{"seq":2,"typ`), 0o600)
	lg3, err := OpenLog(filepath.Join(dir, "y.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer lg3.Close()
	n := 0
	if err := lg3.ReadFrom(0, func(Event) bool { n++; return true }); err != nil || n != 1 {
		t.Fatalf("partial tail should be tolerated: %v n=%d", err, n)
	}
}
