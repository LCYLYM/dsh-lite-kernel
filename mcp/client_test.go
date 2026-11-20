package mcp

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// A stdio "server" that spawns but never answers must fail within the
// init timeout — never block the caller (#13653).
func TestInitializeTimeout(t *testing.T) {
	start := time.Now()
	_, err := Connect(context.Background(), 300*time.Millisecond, "sleep", "60")
	if err == nil {
		t.Fatal("expected initialize to fail on a silent server")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("took %v, should fail fast", elapsed)
	}
}

// A command that cannot spawn is a loud error, not silent absence.
func TestSpawnFailureIsLoud(t *testing.T) {
	_, err := Connect(context.Background(), time.Second, "definitely-not-a-real-binary-xyz")
	if err == nil {
		t.Fatal("expected loud spawn error")
	}
}

// Concurrent calls must each receive their own response: replies are
// routed by id, not by whoever happens to read the pipe first.
func TestConcurrentCallsGetOwnResponses(t *testing.T) {
	dir := t.TempDir()
	srv := dir + "/server.py"
	src := `import sys, json, time
for line in sys.stdin:
    m = json.loads(line)
    if "id" not in m:
        continue
    if m.get("method") == "initialize":
        print(json.dumps({"jsonrpc": "2.0", "id": m["id"], "result": {"ok": 1}}), flush=True)
        continue
    args = (m.get("params") or {}).get("arguments") or {}
    time.sleep(args.get("delay", 0))
    print(json.dumps({"jsonrpc": "2.0", "id": m["id"], "result": {"tag": args.get("tag")}}), flush=True)
`
	if err := os.WriteFile(srv, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := Connect(context.Background(), 200*time.Millisecond, "python3", srv)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// The init ctx must not own the child: calls after the deadline still work.
	time.Sleep(300 * time.Millisecond)

	type res struct {
		tag  string
		body json.RawMessage
		err  error
	}
	var wg sync.WaitGroup
	out := make(chan res, 2)
	for _, r := range []res{{tag: "slow"}, {tag: "fast"}} {
		wg.Add(1)
		go func(tag string, delay float64) {
			defer wg.Done()
			body, err := c.CallTool(context.Background(), 5*time.Second, "t",
				map[string]any{"tag": tag, "delay": delay})
			out <- res{tag: tag, body: body, err: err}
		}(r.tag, map[string]float64{"slow": 0.4}[r.tag])
	}
	wg.Wait()
	close(out)
	for r := range out {
		if r.err != nil {
			t.Fatalf("%s: %v", r.tag, r.err)
		}
		if !strings.Contains(string(r.body), `"`+r.tag+`"`) {
			t.Fatalf("%s got %s", r.tag, r.body)
		}
	}
}
