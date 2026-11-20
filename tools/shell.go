package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Shell runs a command confined to the workspace. Confinement is real
// (Linux user+mount+pid namespaces via unshare), not a textual policy:
// the workspace is the only writable path, /tmp is a private tmpfs,
// /dev is reduced to null/zero/urandom, and the rest of the host
// filesystem does not exist inside. Text matching on shell source can
// always be bypassed (r""m) and false-positives (git log --format=);
// a namespace cannot. This keeps the kernel's no-escape promise even
// through arbitrary shell access (#13675).
type Shell struct {
	W         Workspace
	Timeout   time.Duration // per call, default 30s
	MaxOutput int           // bytes kept from stdout+stderr, default 64 KiB
	// AllowUnsandboxed permits running without namespace confinement on
	// hosts where user namespaces are disabled. Off by default — an
	// unsandboxed shell has the process's full filesystem permissions.
	AllowUnsandboxed bool
	// AllowNet keeps the daemon's network inside the sandbox. Off by
	// default the sandbox also isolates the network — a model command
	// must not reach private services or cloud metadata endpoints that
	// Fetch refuses on purpose.
	AllowNet bool
}

// sandboxScript runs inside `unshare -rmpf --kill-child`: instead of
// chroot (escapable by a namespace-root process holding CAP_SYS_CHROOT)
// it keeps toolchain dirs read-only in place and covers every other
// top-level directory with an empty read-only tmpfs — nothing
// host-side stays reachable and there is no shared staging root.
const sandboxScript = `WS="$1"; CMD="$2"
mount --bind "$WS" /mnt || exit 3
for d in bin lib lib32 lib64 libx32 usr etc sbin opt; do
  [ -d "/$d" ] && mount --bind "/$d" "/$d" && mount -o remount,bind,ro "/$d" || :
done
for d in /*; do
  case "$d" in
    /bin|/lib|/lib32|/lib64|/libx32|/usr|/etc|/sbin|/opt|/proc|/mnt|/tmp|/dev) ;;
    *) mount -t tmpfs -o ro,size=1m tmpfs "$d" 2>/dev/null || : ;;
  esac
done
# Fresh /dev: device opens are denied in a userns anyway (the kernel
# checks CAP_MKNOD against the superblock's userns), so neither binds
# nor mknod can give working nodes — plain files cover the common
# uses: >/dev/null sinks (bounded by the 4m tmpfs), reads get EOF.
mount -t tmpfs -o size=4m tmpfs /dev
for d in null zero full random urandom tty ptmx; do touch "/dev/$d"; done
mkdir /dev/shm /dev/pts
# Descriptor links — plain symlinks onto /proc/self/fd so process
# substitution and std-stream paths keep working.
ln -s /proc/self/fd /dev/fd
ln -s /proc/self/fd/0 /dev/stdin
ln -s /proc/self/fd/1 /dev/stdout
ln -s /proc/self/fd/2 /dev/stderr
mount -t tmpfs -o size=4m tmpfs /dev/shm
mount -t tmpfs -o size=64m tmpfs /tmp
# Drop the capabilities that make the confinement escapable before the
# model command execs: the namespace root starts with CAP_SYS_ADMIN and
# could umount every cover above (and CAP_SYS_CHROOT to walk out of any
# chroot-style anchoring). The bounding set persists across exec, so the
# command can never regain mount authority. setpriv is part of
# util-linux like unshare — if it is absent the exec fails loud rather
# than running unconstrained.
exec setpriv --no-new-privs \
  --bounding-set=-sys_admin,-sys_chroot,-sys_module,-sys_rawio,-sys_boot,-mknod,-sys_ptrace,-syslog \
  sh -c 'cd /mnt && exec sh -c "$1"' _ "$CMD"
`

var (
	sandboxOnce sync.Once
	sandboxOK   bool
)

func sandboxed() bool {
	sandboxOnce.Do(func() {
		sandboxOK = exec.Command("unshare", "-rmpf", "--kill-child", "--mount-proc", "true").Run() == nil
	})
	return sandboxOK
}

func (s Shell) Name() string        { return "shell" }
func (s Shell) Description() string { return "Run a shell command confined to the workspace" }
func (s Shell) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"cmd":{"type":"string"},"timeout_seconds":{"type":"integer"}},"required":["cmd"]}`)
}

func (s Shell) Call(ctx context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Cmd            string `json:"cmd"`
		TimeoutSeconds int    `json:"timeout_seconds"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	if a.Cmd == "" {
		return nil, fmt.Errorf("tools: empty command")
	}
	to := s.Timeout
	if to <= 0 {
		to = 30 * time.Second
	}
	if a.TimeoutSeconds > 0 {
		to = time.Duration(a.TimeoutSeconds) * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()

	// Abs first: EvalSymlinks(".") returns "." and a relative bind
	// source would expose whatever the unshare child starts in.
	abs, err := filepath.Abs(s.W.Root)
	if err != nil {
		return nil, fmt.Errorf("tools: workspace %q: %w", s.W.Root, err)
	}
	root := abs
	if r, err := filepath.EvalSymlinks(abs); err == nil {
		root = r
	} else {
		// Nonexistent root: resolve the deepest existing ancestor and
		// rejoin the missing tail, so symlinks in the prefix are still
		// honored.
		tail := ""
		for p := abs; ; {
			prev := p
			p = filepath.Dir(p)
			if p == prev {
				break
			}
			tail = filepath.Join(filepath.Base(prev), tail)
			if r, err := filepath.EvalSymlinks(p); err == nil {
				root = filepath.Join(r, tail)
				break
			}
		}
	}
	var cmd *exec.Cmd
	if sandboxed() {
		argv := []string{"-rmpf", "--kill-child", "--mount-proc"}
		if !s.AllowNet {
			argv = append(argv, "-n") // own network namespace: no connectivity
		}
		argv = append(argv, "sh", "-c", sandboxScript, "dshk-shell", root, a.Cmd)
		cmd = exec.CommandContext(cctx, "unshare", argv...)
		cmd.Dir = "/"
	} else if s.AllowUnsandboxed {
		cmd = exec.CommandContext(cctx, "sh", "-c", a.Cmd)
		cmd.Dir = root
	} else {
		return nil, fmt.Errorf("tools: shell unavailable — user namespaces are disabled and AllowUnsandboxed is false")
	}
	// Grandchildren can outlive the killed shell while holding the
	// output pipes; WaitDelay closes them instead of waiting forever.
	cmd.WaitDelay = 5 * time.Second
	// HOME points at a path that does not exist inside the sandbox —
	// tools that need a writable home (gocache, config) get /tmp. The
	// daemon's credential-shaped variables never enter a model command —
	// a logged `env` must not leak DEEPSEEK_API_KEY / DSHK_TOKEN.
	// A command without network has no use for proxy/mirror
	// credentials — scrub them too.
	net := !sandboxed() || s.AllowNet
	cmd.Env = envWith(envScrubbed(net), map[string]string{"HOME": "/tmp", "TMPDIR": "/tmp"})

	max := s.MaxOutput
	if max <= 0 {
		max = 64 << 10
	}
	var buf limitedBuffer
	buf.limit = max
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err = cmd.Run()
	res := map[string]any{
		"output":    buf.String(),
		"truncated": buf.truncated,
	}
	if cctx.Err() == context.DeadlineExceeded {
		res["error"] = "timeout"
		res["exit_code"] = -1
		return res, nil
	}
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			res["exit_code"] = ee.ExitCode()
			return res, nil
		}
		return nil, err
	}
	res["exit_code"] = 0
	return res, nil
}

// envScrubbed returns the process environment minus credential-shaped
// variables — names carrying KEY, TOKEN, SECRET, PASSWORD, CREDENTIAL,
// AUTH or PROXY (proxy URLs embed user:pass) — so a shell command
// cannot exfiltrate daemon credentials through its logged output.
// Toolchain config that only looks credential-shaped is exempt:
// GIT_AUTHOR_*/GIT_COMMITTER_*/GIT_CONFIG_* are the shell git's own
// settings (stripping them breaks commits, and dropping the keys while
// GIT_CONFIG_COUNT survives corrupts env-based config), and GOPROXY
// selects the module mirror builds fetch through — but only when the
// command can reach the network at all. Any other GIT_* name —
// GIT_TOKEN and friends — stays scrub-checked.
func envScrubbed(net bool) []string {
	var out []string
	for _, kv := range os.Environ() {
		name, val, _ := strings.Cut(kv, "=")
		u := strings.ToUpper(name)
		sensitive := false
		if !benignShellConfig(u, val, net) {
			for _, pat := range []string{"KEY", "TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL", "AUTH", "PROXY"} {
				if strings.Contains(u, pat) {
					sensitive = true
					break
				}
			}
		}
		if !sensitive {
			out = append(out, kv)
		}
	}
	return out
}

func benignShellConfig(u, val string, net bool) bool {
	for _, p := range []string{"GIT_AUTHOR_", "GIT_COMMITTER_", "GIT_CONFIG_"} {
		if strings.HasPrefix(u, p) {
			return true
		}
	}
	if !net {
		// GOPROXY is a comma/pipe-separated list; keep it only
		// when every entry is offline-safe — a local file mirror
		// or a keyword — so a credentialed remote entry can never
		// ride along behind a benign first entry.
		return u == "GOPROXY" && offlineSafeProxyList(val)
	}
	// Go module config selects mirrors and private-module rules;
	// dropping GONOPROXY/GOPRIVATE while keeping GOPROXY sends
	// private dependencies to the mirror and breaks the build.
	for _, p := range []string{"GOPROXY", "GOPRIVATE", "GONOPROXY", "GONOSUMDB", "GOFLAGS", "GOWORK"} {
		if u == p {
			return true
		}
	}
	return false
}

func envWith(base []string, set map[string]string) []string {
	out := base[:0]
	for _, kv := range base {
		k := kv
		for i := 0; i < len(k); i++ {
			if k[i] == '=' {
				k = k[:i]
				break
			}
		}
		if _, ok := set[k]; !ok {
			out = append(out, kv)
		}
	}
	for k, v := range set {
		out = append(out, k+"="+v)
	}
	return out
}

// limitedBuffer caps retained output; excess is discarded, not
// buffered. It must not embed bytes.Buffer: that type implements
// io.ReaderFrom, which io.Copy prefers over Write and would grow
// unboundedly.
type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.buf.Len()+len(p) > b.limit {
		keep := b.limit - b.buf.Len()
		if keep > 0 {
			b.buf.Write(p[:keep])
		}
		b.truncated = true
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *limitedBuffer) String() string { return b.buf.String() }

// offlineSafeProxyList reports whether every GOPROXY entry works
// without the network and cannot carry credentials: file:// mirrors
// (no userinfo) and the off/direct keywords.
func offlineSafeProxyList(v string) bool {
	for _, e := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == '|' }) {
		e = strings.TrimSpace(e)
		if e == "off" || e == "direct" {
			continue
		}
		if !strings.HasPrefix(e, "file://") || strings.Contains(strings.TrimPrefix(e, "file://"), "@") {
			return false
		}
	}
	return v != ""
}
