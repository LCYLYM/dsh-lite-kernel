// Package tools holds the built-in tools. File access is confined to the
// session workspace; there is no delete tool at all — the kernel cannot
// erase anything, which is the strongest possible answer to #13675
// (a host process recursively deleting ~86k user files).
package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Workspace confines all file tools to Root.
type Workspace struct{ Root string }

func (w Workspace) resolve(p string) (string, error) {
	rootAbs, err := filepath.Abs(w.Root)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(filepath.Clean(filepath.Join(w.Root, p)))
	if err != nil {
		return "", err
	}
	rootReal, err := realPath(rootAbs)
	if err != nil {
		return "", err
	}
	abs, err = realPath(abs)
	if err != nil {
		return "", err
	}
	if abs != rootReal && !strings.HasPrefix(abs, rootReal+string(os.PathSeparator)) {
		return "", fmt.Errorf("tools: path %q escapes the workspace", p)
	}
	return abs, nil
}

// realPath resolves symlinks along p. EvalSymlinks fails on paths that
// don't exist yet (a new file or a not-yet-created workspace), so walk
// to the deepest existing ancestor, resolve it, and rejoin the tail.
func realPath(p string) (string, error) {
	var tail []string
	for {
		if _, err := os.Lstat(p); err == nil {
			real, rerr := filepath.EvalSymlinks(p)
			if rerr != nil {
				return "", rerr
			}
			for i := len(tail) - 1; i >= 0; i-- {
				real = filepath.Join(real, tail[i])
			}
			return real, nil
		}
		parent := filepath.Dir(p)
		if parent == p {
			return "", fmt.Errorf("tools: cannot resolve %q", p)
		}
		tail = append(tail, filepath.Base(p))
		p = parent
	}
}

// ReadFile reads a file inside the workspace.
type ReadFile struct{ W Workspace }

func (t ReadFile) Name() string        { return "read_file" }
func (t ReadFile) Description() string { return "Read a file inside the workspace" }
func (t ReadFile) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`)
}
func (t ReadFile) Call(_ context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	p, err := t.W.resolve(a.Path)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	if len(b) > 1<<20 {
		return map[string]any{"error": "file exceeds 1 MiB read cap", "bytes": len(b)}, nil
	}
	return string(b), nil
}

// WriteFile writes a file inside the workspace.
type WriteFile struct{ W Workspace }

func (t WriteFile) Name() string        { return "write_file" }
func (t WriteFile) Description() string { return "Write a file inside the workspace" }
func (t WriteFile) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}`)
}
func (t WriteFile) Call(_ context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	p, err := t.W.resolve(a.Path)
	if err != nil {
		return nil, err
	}
	tmp := p + ".tmp"
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(tmp, []byte(a.Content), 0o600); err != nil {
		return nil, err
	}
	return map[string]any{"path": p}, os.Rename(tmp, p)
}

// Echo returns its input — the smoke-test tool.
type Echo struct{}

func (Echo) Name() string        { return "echo" }
func (Echo) Description() string { return "Return the input verbatim" }
func (Echo) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`)
}
func (Echo) Call(_ context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	return a.Text, nil
}
