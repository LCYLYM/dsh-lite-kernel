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
	clean := filepath.Clean(filepath.Join(w.Root, p))
	abs, err := filepath.Abs(clean)
	if err != nil {
		return "", err
	}
	rootAbs, _ := filepath.Abs(w.Root)
	if abs != rootAbs && !strings.HasPrefix(abs, rootAbs+string(os.PathSeparator)) {
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
	wsRoot, err := filepath.Abs(t.W.Root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	if err := writeAtomic(p, wsRoot, []byte(a.Content)); err != nil {
		return nil, err
	}
	return map[string]any{"path": p}, nil
}

// writeAtomic stages the content in a sibling temp file, fsyncs it,
// renames over the target, and fsyncs every directory between the file
// and wsRoot so freshly created path components survive a host crash
// too. Two writers never share a temp name.
func writeAtomic(path, wsRoot string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		d, err := os.Open(dir)
		if err != nil {
			return fmt.Errorf("tools: sync dir %q: %w", dir, err)
		}
		err = d.Sync()
		d.Close()
		if err != nil {
			return fmt.Errorf("tools: sync dir %q: %w", dir, err)
		}
		if dir == wsRoot {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	return nil
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
