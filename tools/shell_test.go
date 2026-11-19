package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func shellCall(t *testing.T, cmd string) map[string]any {
	t.Helper()
	s := Shell{W: Workspace{Root: t.TempDir()}, Timeout: 3 * time.Second}
	args, _ := json.Marshal(map[string]string{"cmd": cmd})
	out, err := s.Call(context.Background(), args)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	return out.(map[string]any)
}

func TestShellRunsInWorkspace(t *testing.T) {
	res := shellCall(t, "echo hello && pwd")
	if res["exit_code"] != 0 {
		t.Fatalf("%v", res)
	}
	// Inside the sandbox the workspace is /ws.
	out := res["output"].(string)
	if out[:5] != "hello" {
		t.Fatalf("%v", res)
	}
}

// The sandbox must hold where text policies fail: quoted splits,
// flag-based deletion, and absolute-path escapes all hit the namespace,
// not a regex.
func TestShellConfined(t *testing.T) {
	dir := t.TempDir()
	out := t.TempDir()
	secret := filepath.Join(out, "secret.txt")
	os.WriteFile(secret, []byte("s3cret"), 0o644)

	s := Shell{W: Workspace{Root: dir}, Timeout: 3 * time.Second}
	call := func(cmd string) map[string]any {
		args, _ := json.Marshal(map[string]string{"cmd": cmd})
		res, err := s.Call(context.Background(), args)
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		return res.(map[string]any)
	}

	// Host paths are unreachable, not merely blocked by name.
	res := call("cat " + secret)
	if res["exit_code"] == 0 {
		t.Fatalf("read outside workspace succeeded: %v", res)
	}
	res = call("cat /etc/shadow")
	if res["exit_code"] == 0 {
		t.Fatalf("read /etc/shadow succeeded: %v", res)
	}
	res = call("touch /etc/x")
	if res["exit_code"] == 0 {
		t.Fatalf("wrote to /etc: %v", res)
	}
	// cd / lands on the sandbox root — which must not contain host dirs.
	res = call("cd / && ls")
	if res["exit_code"] != 0 {
		t.Fatalf("cd / failed: %v", res)
	}
	for _, d := range []string{"home", "root", "var"} {
		for _, line := range strings.Split(res["output"].(string), "\n") {
			if line == d {
				t.Fatalf("host dir %q visible: %v", d, res)
			}
		}
	}
	// Quote-split words like r""m are just words in a namespace — rm
	// inside the workspace is legitimate and works.
	os.WriteFile(filepath.Join(dir, "junk.txt"), []byte("x"), 0o644)
	res = call("r\"\"m junk.txt")
	if res["exit_code"] != 0 {
		t.Fatalf("in-workspace rm refused: %v", res)
	}
	if _, err := os.Stat(filepath.Join(dir, "junk.txt")); !os.IsNotExist(err) {
		t.Fatal("in-workspace file not removed")
	}
	// find -delete is confined the same way.
	os.WriteFile(filepath.Join(dir, "junk2.txt"), []byte("x"), 0o644)
	res = call("find . -name 'junk2*' -delete")
	if res["exit_code"] != 0 {
		t.Fatalf("in-workspace delete failed: %v", res)
	}
	// Options that look like denied verbs are fine — no text matching.
	res = call("echo rm sudo format")
	if res["exit_code"] != 0 {
		t.Fatalf("false positive: %v", res)
	}
}

func TestShellTimeout(t *testing.T) {
	res := shellCall(t, "sleep 10")
	if res["error"] != "timeout" {
		t.Fatalf("%v", res)
	}
}

func TestShellOutputCap(t *testing.T) {
	s := Shell{W: Workspace{Root: t.TempDir()}, MaxOutput: 100, Timeout: 3 * time.Second}
	args, _ := json.Marshal(map[string]string{"cmd": "yes | head -1000"})
	out, err := s.Call(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	res := out.(map[string]any)
	if res["truncated"] != true || len(res["output"].(string)) > 100 {
		t.Fatalf("%v", res)
	}
}
