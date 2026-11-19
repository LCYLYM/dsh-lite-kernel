package tools

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func shellCall(t *testing.T, cmd string) map[string]any {
	t.Helper()
	s := Shell{W: Workspace{Root: t.TempDir()}, Timeout: 2 * time.Second}
	args, _ := json.Marshal(map[string]string{"cmd": cmd})
	out, err := s.Call(context.Background(), args)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	return out.(map[string]any)
}

func TestShellRunsInWorkspace(t *testing.T) {
	res := shellCall(t, "echo hello")
	if res["exit_code"] != 0 || res["output"] != "hello\n" {
		t.Fatalf("%v", res)
	}
}

// Destructive verbs are refused before exec — the kernel keeps its
// no-delete promise (#13675).
func TestShellDenyList(t *testing.T) {
	s := Shell{W: Workspace{Root: t.TempDir()}}
	for _, cmd := range []string{"rm -rf /", "rm file", "sudo apt update", "dd if=/dev/zero of=/dev/sda", "shutdown -h now"} {
		args, _ := json.Marshal(map[string]string{"cmd": cmd})
		if _, err := s.Call(context.Background(), args); err == nil {
			t.Fatalf("command %q was not refused", cmd)
		}
	}
	// Benign commands containing the verbs inside words are fine.
	args, _ := json.Marshal(map[string]string{"cmd": "echo formatter warmups"})
	if _, err := s.Call(context.Background(), args); err != nil {
		t.Fatalf("false positive: %v", err)
	}
}

func TestShellTimeout(t *testing.T) {
	res := shellCall(t, "sleep 10")
	if res["error"] != "timeout" {
		t.Fatalf("%v", res)
	}
}

func TestShellOutputCap(t *testing.T) {
	s := Shell{W: Workspace{Root: t.TempDir()}, MaxOutput: 100, Timeout: 2 * time.Second}
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
