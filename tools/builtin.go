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
	abs, err := filepath.Abs(filepath.Clean(filepath.Join(w.Root, p)))
	if err != nil {
		return "", err
	}
	rootReal, err := filepath.EvalSymlinks(w.Root)
	if err != nil {
		return "", err
	}
	// EvalSymlinks fails on paths that don't exist yet (a new file being
	// written) — resolve the deepest existing ancestor, which also
	// resolves any symlink in the middle of the path.
	var tail []string
	for {
		if _, err := os.Lstat(abs); err == nil {
			real, rerr := filepath.EvalSymlinks(abs)
			if rerr != nil {
				return "", rerr
			}
			abs = real
			break
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", fmt.Errorf("tools: cannot resolve %q", p)
		}
		tail = append(tail, filepath.Base(abs))
		abs = parent
	}
	for i := len(tail) - 1; i >= 0; i-- {
		abs = filepath.Join(abs, tail[i])
	}
	if abs != rootReal && !strings.HasPrefix(abs, rootReal+string(os.PathSeparator)) {
		return "", fmt.Errorf("tools: path %q escapes the workspace", p)
	}
	return abs, nil
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
