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
	"sync"
)

// Workspace confines all file tools to Root.
type Workspace struct{ Root string }

// fileMu serializes read-modify-write and write ops per resolved path —
// concurrent tool calls must not restore stale snapshots over each
// other's committed results.
var fileMu sync.Map // resolved abs path -> *sync.Mutex

func lockPath(p string) func() {
	mu, _ := fileMu.LoadOrStore(p, &sync.Mutex{})
	m := mu.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

func (w Workspace) resolve(p string) (string, error) {
	clean := filepath.Clean(filepath.Join(w.Root, p))
	abs, err := filepath.Abs(clean)
	if err != nil {
		return "", err
	}
	rootAbs, _ := filepath.Abs(w.Root)
	if abs != rootAbs && !strings.HasPrefix(abs, rootAbs+string(os.PathSeparator)) {
		return "", fmt.Errorf("tools: path %q escapes the workspace", p)
	}
	// Lexical containment isn't enough: a symlink inside the workspace
	// would redirect file ops outside. Resolve the real path of the
	// deepest existing ancestor and re-check containment on it.
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", err
	}
	probe, rest := abs, ""
	for {
		if real, err := filepath.EvalSymlinks(probe); err == nil {
			abs = filepath.Join(real, rest)
			break
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", fmt.Errorf("tools: path %q unresolvable", p)
		}
		rest = filepath.Join(filepath.Base(probe), rest)
		probe = parent
	}
	if abs != rootReal && !strings.HasPrefix(abs, rootReal+string(os.PathSeparator)) {
		return "", fmt.Errorf("tools: path %q escapes the workspace", p)
	}
	return abs, nil
}

// writeAtomic writes data to a unique temp file next to p and renames —
// a failed write can never leave p truncated.
func writeAtomic(p string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), p)
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
	unlock := lockPath(p)
	defer unlock()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	if err := writeAtomic(p, []byte(a.Content), 0o600); err != nil {
		return nil, err
	}
	return map[string]any{"path": p}, nil
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
