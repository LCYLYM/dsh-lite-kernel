package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ListFiles walks the workspace and returns relative paths. Upstream's
// equivalent readdir loop could dump a huge tree into context in one
// go; entries are capped here and node_modules/.git-style noise is
// skipped by default.
type ListFiles struct {
	W       Workspace
	Max     int  // entry cap; 0 → 500
	ShowAll bool // include dotdirs and VCS/dependency dirs
}

func (t ListFiles) Name() string        { return "list_files" }
func (t ListFiles) Description() string { return "List files inside the workspace (bounded)" }
func (t ListFiles) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}}}`)
}

var skipDirs = map[string]bool{".git": true, "node_modules": true, "vendor": true}

func (t ListFiles) Call(_ context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	_, root, err := t.W.resolve(a.Path)
	if err != nil {
		return nil, err
	}
	max := t.Max
	if max <= 0 {
		max = 500
	}
	wsRoot, _ := filepath.Abs(t.W.Root)
	var out []string
	truncated := false
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable entries are skipped, not fatal
		}
		if len(out) >= max {
			truncated = true
			return filepath.SkipAll
		}
		rel, _ := filepath.Rel(wsRoot, p)
		if rel == "." {
			return nil
		}
		if d.IsDir() && !t.ShowAll {
			for _, seg := range strings.Split(rel, string(os.PathSeparator)) {
				if skipDirs[seg] {
					return filepath.SkipDir
				}
			}
		}
		if d.IsDir() {
			return nil
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"files": out, "truncated": truncated}, nil
}

type grepHit struct {
	File string `json:"file"`
	Line int    `json:"line"`
	Text string `json:"text"`
}

// GrepFile searches workspace files for a regex and returns matching
// lines. The upstream complaint class: shell-based grep was a token
// sponge — an unfiltered recursive grep could flood a whole turn.
// Results are line-capped and files over 1MB are skipped.
type GrepFile struct{ W Workspace }

func (t GrepFile) Name() string        { return "grep" }
func (t GrepFile) Description() string { return "Search workspace files for a regex (bounded)" }
func (t GrepFile) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"pattern":{"type":"string"},"path":{"type":"string"},"max":{"type":"integer"}},"required":["pattern"]}`)
}

func (t GrepFile) Call(ctx context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path"`
		Max     int    `json:"max"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	re, err := regexp.Compile(a.Pattern)
	if err != nil {
		return nil, fmt.Errorf("tools: grep: %w", err)
	}
	_, root, err := t.W.resolve(a.Path)
	if err != nil {
		return nil, err
	}
	max := a.Max
	if max <= 0 {
		max = 100
	}
	wsRoot, _ := filepath.Abs(t.W.Root)
	hits := []grepHit{}
	truncated := false
	if info, _ := os.Stat(root); info != nil && !info.IsDir() {
		if !grepEligible(root) {
			return map[string]any{"matches": hits, "truncated": false}, nil
		}
		var more bool
		hits, more = grepOne(ctx, root, wsRoot, re, max)
		truncated = more
	} else {
		err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(hits) >= max {
				truncated = true
				return filepath.SkipAll
			}
			if !grepEligible(p) {
				return nil // non-regular, symlink, or oversized
			}
			h, more := grepOne(ctx, p, wsRoot, re, max-len(hits))
			hits = append(hits, h...)
			truncated = truncated || more
			return nil
		})
	}
	if err != nil {
		return nil, err
	}
	// A cancelled scan is an incomplete result, not a partial success.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"matches": hits, "truncated": truncated}, nil
}

// grepEligible admits only plain regular files under the size cap —
// symlinks stay un-followed (a workspace link must not leak external
// content) and FIFOs are never opened (os.Open on a pipe blocks until
// a writer connects, hanging the whole turn).
func grepEligible(p string) bool {
	fi, err := os.Lstat(p)
	return err == nil && fi.Mode().IsRegular() && fi.Size() <= 1<<20
}

// grepOne returns hits plus whether the cap dropped additional matches.
func grepOne(ctx context.Context, p, wsRoot string, re *regexp.Regexp, max int) ([]grepHit, bool) {
	f, err := os.Open(p)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	rel, _ := filepath.Rel(wsRoot, p)
	hits := []grepHit{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	ln := 0
	more := false
	for sc.Scan() && ctx.Err() == nil {
		ln++
		line := sc.Text()
		if !re.MatchString(line) {
			continue
		}
		if len(hits) >= max {
			more = true // overflow in this file still counts as truncation
			break
		}
		if len(line) > 500 {
			line = line[:500] + "…"
		}
		hits = append(hits, grepHit{rel, ln, line})
	}
	return hits, more
}
