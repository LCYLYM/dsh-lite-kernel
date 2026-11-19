package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"time"
)

// Shell runs a command inside the workspace with a hard timeout, an
// output cap, and a denylist of destructive verbs. The denylist keeps
// the same promise as the rest of this kernel: the agent cannot erase
// user data (#13675). `rm`/`rmdir`/`unlink`/`dd`/mkfs/format/shutdown/
// sudo are refused outright — file deletion is intentionally impossible
// through this kernel; write_file covers legitimate mutation.
type Shell struct {
	W           Workspace
	Timeout     time.Duration // per call, default 30s
	MaxOutput   int           // bytes kept from stdout+stderr, default 64 KiB
	DenyPattern *regexp.Regexp
}

var defaultDeny = regexp.MustCompile(`(?i)(^|[^a-z])(rm|rmdir|unlink|dd|mkfs|fdisk|format|shutdown|reboot|halt|sudo|su|chmod|chown|passwd|crontab|systemctl|launchctl)\b`)

func (s Shell) deny() *regexp.Regexp {
	if s.DenyPattern != nil {
		return s.DenyPattern
	}
	return defaultDeny
}

func (s Shell) Name() string        { return "shell" }
func (s Shell) Description() string { return "Run a shell command inside the workspace" }
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
	if s.deny().MatchString(a.Cmd) {
		return nil, fmt.Errorf("tools: command refused by destructive-verb policy: %q", a.Cmd)
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

	cmd := exec.CommandContext(cctx, "sh", "-c", a.Cmd)
	cmd.Dir = s.W.Root
	// Grandchildren can outlive the killed shell while holding the
	// output pipes; WaitDelay closes them instead of waiting forever.
	cmd.WaitDelay = 5 * time.Second
	max := s.MaxOutput
	if max <= 0 {
		max = 64 << 10
	}
	var buf limitedBuffer
	buf.limit = max
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
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
