package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// EditFile replaces exact text inside a workspace file. Whole-file
// rewrites were a recurring upstream token-burn complaint — a surgical
// replace lets the model patch one line without resending the file.
//
// `old` must occur exactly once unless `all` is set; an ambiguous edit
// fails loud with the occurrence count instead of silently picking the
// first match.
type EditFile struct{ W Workspace }

func (t EditFile) Name() string        { return "edit_file" }
func (t EditFile) Description() string { return "Replace exact text in a workspace file" }
func (t EditFile) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"old":{"type":"string"},"new":{"type":"string"},"all":{"type":"boolean"}},"required":["path","old","new"]}`)
}

const editMaxFile = 4 << 20 // beyond this, a full write is honest anyway

func (t EditFile) Call(_ context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Path string `json:"path"`
		Old  string `json:"old"`
		New  string `json:"new"`
		All  bool   `json:"all"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	if a.Old == "" {
		return nil, fmt.Errorf("tools: edit_file requires a non-empty 'old'")
	}
	_, p, err := t.W.resolve(a.Path)
	if err != nil {
		return nil, err
	}
	unlock := lockPath(p)
	defer unlock()
	info, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("tools: edit_file %q is a directory", a.Path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("tools: edit_file %q is not a regular file", a.Path)
	}
	if info.Size() > editMaxFile {
		return nil, fmt.Errorf("tools: edit_file %q too large (%d bytes)", a.Path, info.Size())
	}
	src, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	n := strings.Count(string(src), a.Old)
	if n == 0 {
		return nil, fmt.Errorf("tools: edit_file: 'old' not found in %q", a.Path)
	}
	if n > 1 && !a.All {
		return nil, fmt.Errorf("tools: edit_file: 'old' matches %d locations in %q; narrow it or pass all=true", n, a.Path)
	}
	if !a.All {
		n = 1
	}
	out := strings.Replace(string(src), a.Old, a.New, n)
	if err := writeAtomic(p, []byte(out), info.Mode(), info); err != nil {
		return nil, err
	}
	return map[string]any{"replaced": n}, nil
}
