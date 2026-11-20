package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/kernel"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/mcp"
)

// A full adapter round-trip through a real stdio fake: connect, list,
// name-prefixing, schema passthrough, and a tools/call routed by id.
func TestMCPToolEndToEnd(t *testing.T) {
	srv := filepath.Join(t.TempDir(), "server.py")
	src := `import sys, json
for line in sys.stdin:
    m = json.loads(line)
    if m.get("method") == "initialize":
        print(json.dumps({"jsonrpc":"2.0","id":m["id"],"result":{"capabilities":{}}})); sys.stdout.flush()
    elif m.get("method") == "tools/list":
        print(json.dumps({"jsonrpc":"2.0","id":m["id"],"result":{"tools":[
            {"name":"pong.tool","description":"replies pong","inputSchema":{"type":"object","properties":{"x":{"type":"string"}}}}
        ]}})); sys.stdout.flush()
    elif m.get("method") == "tools/call":
        print(json.dumps({"jsonrpc":"2.0","id":m["id"],"result":{"content":[{"type":"text","text":"pong"}]}})); sys.stdout.flush()
`
	if err := os.WriteFile(srv, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := mcp.Connect(context.Background(), 5*time.Second, "python3", srv)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ts, err := ListTools(c, "fs server", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(ts))
	}
	if ts[0].Name() != "mcp_fs_server_pong_tool" {
		t.Fatalf("bad prefixed name %q", ts[0].Name())
	}
	var sch map[string]any
	if err := json.Unmarshal(ts[0].Schema(), &sch); err != nil || sch["type"] != "object" {
		t.Fatalf("schema not passed through: %s", ts[0].Schema())
	}
	out, err := ts[0].Call(context.Background(), json.RawMessage(`{"x":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := out.(map[string]any)
	if m["content"] == nil {
		t.Fatalf("bad result %v", out)
	}

	root := kernel.NewRoot("t")
	sc, _ := root.Child("a")
	reg := NewRegistry(sc, time.Second)
	if _, err := reg.Register(ts[0]); err != nil {
		t.Fatal(err)
	}
	res, err := reg.Call(context.Background(), llm.ToolCall{Name: ts[0].Name(), Args: json.RawMessage(`{"x":"hi"}`)})
	if err != nil {
		t.Fatal(err)
	}
	m2, _ := res.(map[string]any)
	if m2["content"] == nil {
		t.Fatalf("bad routed result %v", res)
	}
}
