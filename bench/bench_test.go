// Benchmarks backing the README's memory/concurrency claims. Run:
//
//	go test ./bench -bench=. -benchmem
package bench

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LCYLYM/dsh-lite-kernel/agent"
	"github.com/LCYLYM/dsh-lite-kernel/kernel"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/session"
	"github.com/LCYLYM/dsh-lite-kernel/tools"
)

func rss(t testing.TB) uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// Per-session resident cost: store, log index, agent scope.
func BenchmarkPerSessionMemory(b *testing.B) {
	dir := b.TempDir()
	st, _ := session.NewStore(dir)
	base := rss(b)
	const n = 10000
	for i := 0; i < n; i++ {
		ses, err := st.Create("ws")
		if err != nil {
			b.Fatal(err)
		}
		ses.Log.Append("user/message", map[string]any{"role": "user", "content": "hi"})
	}
	delta := rss(b) - base
	b.ReportMetric(float64(delta)/float64(n)/1024, "KiB/session")
}

// Concurrency: how many simultaneous turns a process carries.
func BenchmarkConcurrentTurns(b *testing.B) {
	dir := b.TempDir()
	st, _ := session.NewStore(dir)
	const n = 1000
	done := make(chan struct{}, n)
	base := rss(b)
	b.ResetTimer()
	for i := 0; i < n; i++ {
		ses, err := st.Create("ws")
		if err != nil {
			b.Fatal(err)
		}
		root := kernel.NewRoot("root")
		sc, _ := root.Child("a")
		reg := agent.NewRegistry(sc, 0)
		reg.Register(tools.Echo{})
		loop := &agent.Loop{
			Model: "echo", Provider: &llm.EchoProvider{}, Registry: reg, Log: ses.Log,
			Budget: session.DefaultBudget(), Guard: agent.DefaultGuard(), Retry: llm.Policy{Retries: 0},
		}
		go func() {
			_, _ = loop.Run(context.Background(), "hello")
			done <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
	b.StopTimer()
	delta := rss(b) - base
	b.ReportMetric(float64(delta)/float64(n)/1024, "KiB/turn")
	b.Logf("events/sec capability: %d turns completed", n)
}

// Long-session memory: a session with many events must not blow the heap
// when its context is built (the #13639 regression).
func BenchmarkLongSessionContext(b *testing.B) {
	dir := b.TempDir()
	lg, _ := session.OpenLog(filepath.Join(dir, "s.jsonl"))
	for i := 0; i < 50000; i++ {
		lg.Append("assistant/message", map[string]any{"role": "assistant", "content": string(make([]byte, 200))})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := session.BuildContext(lg, session.DefaultBudget())
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) == 0 {
			b.Fatal("empty context")
		}
		_ = fmt.Sprint(msgs[0].Role)
	}
}

// Reopening a long session: the per-line offset index is O(events) but
// constant-memory — this is the "load a big conversation" path upstream
// users report stalling on.
func BenchmarkOpenLongLog(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "s.jsonl")
	lg, _ := session.OpenLog(path)
	for i := 0; i < 50000; i++ {
		lg.Append("assistant/message", map[string]any{"role": "assistant", "content": string(make([]byte, 200))})
	}
	lg.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := session.OpenLog(path)
		if err != nil {
			b.Fatal(err)
		}
		l.Close()
	}
}

// Full replay of a long session — the SSE catch-up path.
func BenchmarkReplayLongLog(b *testing.B) {
	dir := b.TempDir()
	lg, _ := session.OpenLog(filepath.Join(dir, "s.jsonl"))
	for i := 0; i < 50000; i++ {
		lg.Append("assistant/message", map[string]any{"role": "assistant", "content": string(make([]byte, 200))})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int
		err := lg.ReadFrom(0, func(ev session.Event) bool { n++; return true })
		if err != nil {
			b.Fatal(err)
		}
		if n != 50000 {
			b.Fatalf("replayed %d", n)
		}
	}
}

// The token-budget check runs Usage once per step on every turn; the
// incremental index must keep repeat calls O(new events), not O(history).
func BenchmarkUsageRepeatCalls(b *testing.B) {
	dir := b.TempDir()
	lg, _ := session.OpenLog(filepath.Join(dir, "s.jsonl"))
	for i := 0; i < 50000; i++ {
		lg.Append("assistant/message", map[string]any{"role": "assistant", "content": "x", "prompt_tokens": 10})
	}
	if _, err := session.Usage(lg); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := session.Usage(lg); err != nil {
			b.Fatal(err)
		}
	}
}
