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
	root, err := t.W.resolve(a.Path)
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

func (t GrepFile) Call(_ context.Context, args json.RawMessage) (any, error) {
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
	root, err := t.W.resolve(a.Path)
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
		hits = grepOne(root, wsRoot, re, max)
	} else {
		err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if len(hits) >= max {
				truncated = true
				return filepath.SkipAll
			}
			if fi, _ := d.Info(); fi != nil && fi.Size() > 1<<20 {
				return nil // grep the needle, not the heap
			}
			hits = append(hits, grepOne(p, wsRoot, re, max-len(hits))...)
			return nil
		})
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"matches": hits, "truncated": truncated}, nil
}

func grepOne(p, wsRoot string, re *regexp.Regexp, max int) []grepHit {
	f, err := os.Open(p)
	if err != nil {
		return nil
	}
	defer f.Close()
	rel, _ := filepath.Rel(wsRoot, p)
	hits := []grepHit{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	ln := 0
	for sc.Scan() && len(hits) < max {
		ln++
		line := sc.Text()
		if re.MatchString(line) {
			if len(line) > 500 {
				line = line[:500] + "…"
			}
			hits = append(hits, grepHit{rel, ln, line})
		}
	}
	return hits
}
