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
	"syscall"
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

// lockWrite locks the lexical write path, plus the real path when the
// rename will land on it — i.e. the leaf is not itself a symlink (an
// ancestor is, or the file doesn't exist). Replacing a link never
// touches its target, so the target's lock — possibly held by a stuck
// reader of a fifo — is skipped in that case.
func lockWrite(lex, real string) func() {
	if fi, err := os.Lstat(lex); (err == nil && fi.Mode()&os.ModeSymlink != 0) || real == lex {
		return lockPath(lex)
	}
	a, b := lex, real
	if a > b {
		a, b = b, a
	}
	u1 := lockPath(a)
	u2 := lockPath(b)
	return func() { u2(); u1() }
}

// resolve returns both forms of p: the lexical absolute path (what
// write_file should create — a symlink there is REPLACED, never
// followed) and the real path with symlinks resolved (what
// read_file/edit_file should touch — an in-workspace link to an
// external file is rejected).
func (w Workspace) resolve(p string) (lex string, real string, err error) {
	clean := filepath.Clean(filepath.Join(w.Root, p))
	abs, err := filepath.Abs(clean)
	if err != nil {
		return "", "", err
	}
	rootAbs, _ := filepath.Abs(w.Root)
	if abs != rootAbs && !strings.HasPrefix(abs, rootAbs+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("tools: path %q escapes the workspace", p)
	}
	lexAbs := abs
	// A nonexistent workspace has no symlinks inside it — lexical
	// containment suffices, and resolve stays side-effect free (a read
	// must not materialize directories).
	if _, err := os.Stat(rootAbs); os.IsNotExist(err) {
		return lexAbs, lexAbs, nil
	}
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", "", err
	}
	probe, rest := abs, ""
	for {
		if realp, err := filepath.EvalSymlinks(probe); err == nil {
			abs = filepath.Join(realp, rest)
			break
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", "", fmt.Errorf("tools: path %q unresolvable", p)
		}
		rest = filepath.Join(filepath.Base(probe), rest)
		probe = parent
	}
	if abs != rootReal && !strings.HasPrefix(abs, rootReal+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("tools: path %q escapes the workspace", p)
	}
	return lexAbs, abs, nil
}

// writeAtomic writes data to a unique temp file next to p and renames —
// a failed write can never leave p truncated, and the temp is removed
// when the rename fails. preserve, when non-nil, keeps the original
// file's uid/gid/mode; without it the file gets mode + caller's ids.
// A chown failure fails the write loudly rather than exposing content
// to a different group with the original's permission bits.
func writeAtomic(p string, data []byte, mode os.FileMode, preserve os.FileInfo) error {
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	cleanup := func(err error) error {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if preserve != nil {
		st, ok := preserve.Sys().(*syscall.Stat_t)
		if !ok {
			return cleanup(fmt.Errorf("tools: cannot stat %q", p))
		}
		if err := tmp.Chown(int(st.Uid), int(st.Gid)); err != nil {
			return cleanup(fmt.Errorf("tools: cannot preserve ownership of %q: %w", p, err))
		}
	}
	if err := tmp.Chmod(mode); err != nil {
		return cleanup(err)
	}
	if _, err := tmp.Write(data); err != nil {
		return cleanup(err)
	}
	if err := tmp.Sync(); err != nil {
		return cleanup(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if d, err := os.Open(filepath.Dir(p)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
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
	_, p, err := t.W.resolve(a.Path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("tools: read_file %q is not a regular file", a.Path)
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
	// resolve rejects symlink escapes on the leaf; the parent needs the
	// same check — a dir symlink could make the lexical rename land
	// outside even when the leaf's target resolves inside.
	p, real, err := t.W.resolve(a.Path)
	if err != nil {
		return nil, err
	}
	if _, _, err := t.W.resolve(filepath.Dir(a.Path)); err != nil {
		return nil, err
	}
	if rootAbs, _ := filepath.Abs(t.W.Root); p == rootAbs {
		return nil, fmt.Errorf("tools: path %q is the workspace root", a.Path)
	}
	unlock := lockWrite(p, real)
	defer unlock()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	if err := writeAtomic(p, []byte(a.Content), 0o600, nil); err != nil {
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
