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
	// AllowUnsandboxed so hosts without user namespaces (CI runners)
	// still exercise the shell; confinement has its own test, which
	// skips there instead of faking a sandbox.
	s := Shell{W: Workspace{Root: t.TempDir()}, Timeout: 3 * time.Second, AllowUnsandboxed: true}
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
	if !sandboxed() {
		t.Skip("user namespaces disabled — no sandbox to confine in")
	}
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
	// Host directories are covered by empty tmpfs mounts — the names
	// may still be visible at /, but their contents are gone.
	res = call("find /home /root /var /media /srv -mindepth 1 2>/dev/null | head -5")
	if strings.TrimSpace(res["output"].(string)) != "" {
		t.Fatalf("host dirs not covered: %v", res)
	}
	// /dev is a fresh tmpfs — only our stub files and empty dirs exist;
	// no host device nodes at all.
	res = call("ls /dev")
	for _, line := range strings.Fields(res["output"].(string)) {
		switch line {
		case "null", "zero", "full", "random", "urandom", "tty", "ptmx", "pts", "shm",
			"fd", "stdin", "stdout", "stderr":
		default:
			t.Fatalf("host device %q exposed: %v", line, res)
		}
	}
	// >/dev/null must work — the classic failure mode of this sandbox.
	res = call("echo out; echo err >&2; echo kept 2>/dev/null")
	if res["exit_code"] != 0 {
		t.Fatalf("2>/dev/null broken: %v", res)
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
	s := Shell{W: Workspace{Root: t.TempDir()}, MaxOutput: 100, Timeout: 3 * time.Second, AllowUnsandboxed: true}
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

// Credential-shaped variables must never reach a model-run command —
// a logged `env` would otherwise exfiltrate the daemon's keys.
func TestShellScrubsDaemonCredentials(t *testing.T) {
	t.Setenv("DSHK_TEST_TOKEN_X", "leak-me")
	t.Setenv("DSHK_TEST_APIKEY", "leak-me-too")
	t.Setenv("DSHK_TEST_HTTPS_PROXY", "http://user:pass@proxy:8080")
	t.Setenv("GIT_AUTHOR_NAME", "kernel-bot")
	t.Setenv("GIT_TOKEN", "leak-me-three")
	t.Setenv("GOPROXY", "https://user:pass@goproxy.corp")
	res := shellCall(t, "env")
	out := res["output"].(string)
	for _, name := range []string{"DSHK_TEST_TOKEN_X", "DSHK_TEST_APIKEY", "DSHK_TEST_HTTPS_PROXY", "GIT_TOKEN"} {
		if strings.Contains(out, name) {
			t.Fatalf("credential %s leaked into shell env: %v", name, out)
		}
	}
	if !strings.Contains(out, "PATH=") {
		t.Fatal("env scrub dropped PATH — toolchain needs it")
	}
	if !strings.Contains(out, "GIT_AUTHOR_NAME=kernel-bot") {
		t.Fatal("env scrub stripped git's own config namespace")
	}
	// GOPROXY is kept only for commands that can reach the network:
	// unsandboxed (host net) or AllowNet. In the default no-net sandbox
	// it is scrubbed like any other credential-bearing proxy var.
	net := !sandboxed()
	if !net {
		if strings.Contains(out, "GOPROXY=") {
			t.Fatal("no-net sandbox kept GOPROXY credentials")
		}
		t.Setenv("DSHK_TEST_TOKEN_X", "leak-me")
		s := Shell{W: Workspace{Root: t.TempDir()}, Timeout: 3 * time.Second, AllowNet: true}
		args, _ := json.Marshal(map[string]string{"cmd": "env"})
		raw, err := s.Call(context.Background(), args)
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		out = raw.(map[string]any)["output"].(string)
	}
	if !strings.Contains(out, "GOPROXY=https://user:pass@goproxy.corp") {
		t.Fatal("env scrub dropped GOPROXY from a networked command")
	}
}

// The sandbox's namespace root starts with CAP_SYS_ADMIN, which could
// umount every tmpfs cover and expose the host — the bounding-set drop
// before exec must make umount fail.
func TestShellCannotUnmountCovers(t *testing.T) {
	if !sandboxed() {
		t.Skip("user namespaces disabled — no sandbox to confine in")
	}
	s := Shell{W: Workspace{Root: t.TempDir()}, Timeout: 3 * time.Second}
	args, _ := json.Marshal(map[string]string{"cmd": "umount /usr 2>&1 || echo UMOUNT_FAILED"})
	out, err := s.Call(context.Background(), args)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	res := out.(map[string]any)
	if !strings.Contains(res["output"].(string), "UMOUNT_FAILED") {
		t.Fatalf("umount succeeded inside the sandbox: %v", res)
	}
}

// A sandboxed shell must not reach the network — model commands have
// no business talking to private services or cloud metadata.
func TestShellNoNetworkByDefault(t *testing.T) {
	if !sandboxed() {
		t.Skip("user namespaces disabled — no sandbox to confine in")
	}
	s := Shell{W: Workspace{Root: t.TempDir()}, Timeout: 3 * time.Second}
	// /proc/net/dev inside a fresh net ns lists only lo.
	args, _ := json.Marshal(map[string]string{"cmd": "cat /proc/net/dev"})
	out, err := s.Call(context.Background(), args)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	res := out.(map[string]any)
	got := res["output"].(string)
	if strings.Contains(got, "eth0") || strings.Contains(got, "ens") || strings.Contains(got, "wlan") {
		t.Fatalf("host network interfaces visible in sandbox: %q", got)
	}
	if !strings.Contains(got, "lo:") {
		t.Fatalf("expected lo-only net ns, got %q", got)
	}
}
