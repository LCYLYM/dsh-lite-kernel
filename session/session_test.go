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

// A tool/result whose call was evicted by the budget must not be sent to
// the model: strict providers reject orphaned tool messages.
func TestBuildContextDropsOrphanToolResults(t *testing.T) {
	dir := t.TempDir()
	lg, _ := OpenLog(filepath.Join(dir, "s.jsonl"))
	lg.Append("assistant/message", map[string]any{
		"role": "assistant", "content": "calling",
		"tool_calls": []map[string]any{{"id": "c1", "name": "read_file", "arguments": "{}"}},
	})
	for i := 0; i < 50; i++ {
		lg.Append("user/message", map[string]any{"role": "user", "content": "filler"})
	}
	lg.Append("tool/result", map[string]any{"content": "file bytes", "tool_call_id": "c1"})
	lg.Append("user/message", map[string]any{"role": "user", "content": "now what"})

	msgs, err := BuildContext(lg, ContextBudget{MaxBytes: 1 << 20, MaxEvents: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if m.Role == "tool" {
			t.Fatalf("orphan tool result reached model context: %+v", m)
		}
	}
}

func TestReleaseReloads(t *testing.T) {
	st, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ses, _ := st.Create("ws")
	if err := st.Release(ses.ID); err != nil {
		t.Fatal(err)
	}
	// evicted: List drops it, Get reloads a fresh Session object
	for _, id := range st.List() {
		if id == ses.ID {
			t.Fatal("released session still live")
		}
	}
	ses2, err := st.Get(ses.ID)
	if err != nil || ses2 == ses {
		t.Fatalf("reload: %v same=%v", err, ses2 == ses)
	}
}

func TestOpenLogValidatesSeqs(t *testing.T) {
	dir := t.TempDir()
	good := `{"seq":1,"type":"a","data":{},"ts":"x"}` + "\n" +
		`{"seq":2,"type":"b","data":{},"ts":"x"}` + "\n"
	// partial tail: truncated to committed content, open succeeds
	p := filepath.Join(dir, "t.jsonl")
	os.WriteFile(p, []byte(good+`{"seq":3,"ty`), 0o600)
	lg, err := OpenLog(p)
	if err != nil {
		t.Fatal(err)
	}
	if lg.Len() != 2 {
		t.Fatalf("Len=%d", lg.Len())
	}
	fi, _ := os.Stat(p)
	if fi.Size() != int64(len(good)) {
		t.Fatal("partial tail not truncated")
	}
	// appends continue from seq 3
	if ev, _ := lg.Append("c", nil); ev.Seq != 3 {
		t.Fatalf("next seq %d", ev.Seq)
	}
	lg.Close()
	// corrupt middle line → fail loud
	p2 := filepath.Join(dir, "m.jsonl")
	os.WriteFile(p2, []byte(good+"garbage\n"+`{"seq":3,"type":"c","data":{},"ts":"x"}`+"\n"), 0o600)
	if _, err := OpenLog(p2); err == nil {
		t.Fatal("mid-log corruption accepted")
	}
	// seq gap → fail loud
	p3 := filepath.Join(dir, "g.jsonl")
	os.WriteFile(p3, []byte(good+`{"seq":9,"type":"c","data":{},"ts":"x"}`+"\n"), 0o600)
	if _, err := OpenLog(p3); err == nil {
		t.Fatal("seq gap accepted")
	}
}

// CRLF endings: offsets must count real bytes — a Scanner strips '\r'
// and would cut the truncation inside a committed event's newline.
func TestOpenLogCRLFTail(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "crlf.jsonl")
	os.WriteFile(p, []byte("{\"seq\":1,\"type\":\"a\",\"data\":{},\"ts\":\"x\"}\r\n{\"seq\":"), 0o600)
	lg, err := OpenLog(p)
	if err != nil {
		t.Fatal(err)
	}
	if lg.Len() != 1 {
		t.Fatalf("Len=%d", lg.Len())
	}
	if ev, _ := lg.Append("b", nil); ev.Seq != 2 {
		t.Fatalf("seq %d", ev.Seq)
	}
	lg.Close()
	// reopen must succeed — the append landed after a clean newline
	lg2, err := OpenLog(p)
	if err != nil {
		t.Fatalf("reopen after CRLF recovery: %v", err)
	}
	if lg2.Len() != 2 {
		t.Fatalf("reopened Len=%d", lg2.Len())
	}
}

// A valid final event with no trailing newline is terminated at open —
// otherwise the next append glues onto its line and corrupts the log.
func TestOpenLogUnterminatedFinal(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "u.jsonl")
	os.WriteFile(p, []byte(`{"seq":1,"type":"a","data":{},"ts":"x"}`), 0o600) // no \n
	lg, err := OpenLog(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lg.Append("b", nil); err != nil {
		t.Fatal(err)
	}
	lg.Close()
	lg2, err := OpenLog(p)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if lg2.Len() != 2 {
		t.Fatalf("Len=%d", lg2.Len())
	}
}

func TestReadFromSurfacesCorruption(t *testing.T) {
	dir := t.TempDir()
	lg, err := OpenLog(filepath.Join(dir, "x.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lg.Append("user/message", map[string]any{"role": "user", "content": "a"})
	lg.Append("user/message", map[string]any{"role": "user", "content": "b"})
	lg.Close()
	// corrupt a line in the middle, keep a valid tail
	f, _ := os.ReadFile(filepath.Join(dir, "x.jsonl"))
	lines := strings.SplitN(string(f), "\n", 2)
	f2, _ := os.Create(filepath.Join(dir, "x.jsonl"))
	f2.WriteString("not-json\n" + lines[1])
	f2.Close()
	// open-time validation surfaces mid-log corruption loudly
	if _, err := OpenLog(filepath.Join(dir, "x.jsonl")); err == nil ||
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
