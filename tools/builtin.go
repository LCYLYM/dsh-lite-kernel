// Package tools holds the built-in tools. File access is confined to the
// session workspace; there is no delete tool at all — the kernel cannot
// erase anything, which is the strongest possible answer to #13675
// (a host process recursively deleting ~86k user files).
package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	// Check size before reading: allocating the file first and
	// rejecting it afterward defeats the point of the cap — a huge
	// file would still grow the heap to its full size.
	// Stat before Open: opening a fifo blocks until a writer appears,
	// so the non-regular check must come first.
	fi, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("tools: %q is not a regular file", a.Path)
	}
	if fi.Size() > 1<<20 {
		return map[string]any{"error": "file exceeds 1 MiB read cap", "bytes": fi.Size()}, nil
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// The +1 byte distinguishes "exactly at cap" from "grew past the
	// stat": a file that expanded or under-reported its size still
	// returns the cap error rather than silent truncation.
	b, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 1<<20 {
		return map[string]any{"error": "file exceeds 1 MiB read cap"}, nil
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
// too. Directory handles are opened before the rename — a dir that
// can't be opened fails before the file changes, so an acknowledged
// failure never masks a successful write. Two writers never share a
// temp name.
func writeAtomic(path, wsRoot string, data []byte) error {
	type dirFD struct {
		path string
		f    *os.File
	}
	var dirs []dirFD
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		d, err := os.Open(dir)
		if err != nil {
			for _, f := range dirs {
				f.f.Close()
			}
			return fmt.Errorf("tools: open dir %q: %w", dir, err)
		}
		dirs = append(dirs, dirFD{dir, d})
		if dir == wsRoot {
			break
		}
		if parent := filepath.Dir(dir); parent == dir {
			break
		}
	}
	defer func() {
		for _, d := range dirs {
			d.f.Close()
		}
	}()
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
	for _, d := range dirs {
		f := d.f
		// The directory may have been replaced between open and rename;
		// the open fd would then sync a stale inode. Reopen the current
		// path when it no longer names the same directory.
		if cur, err := os.Open(d.path); err == nil {
			ci, _ := cur.Stat()
			oi, _ := f.Stat()
			if ci != nil && oi != nil && !os.SameFile(ci, oi) {
				f.Close()
				f = cur
			} else {
				cur.Close()
			}
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("tools: sync dir %q: %w", d.path, err)
		}
		if f != d.f {
			f.Close()
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
