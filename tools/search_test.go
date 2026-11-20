package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestListFilesCaps(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 10; i++ {
		os.WriteFile(filepath.Join(dir, string(rune('a'+i))+".txt"), []byte("x"), 0o644)
	}
	os.Mkdir(filepath.Join(dir, ".git"), 0o755)
	os.WriteFile(filepath.Join(dir, ".git", "config"), []byte("x"), 0o644)
	raw, _ := json.Marshal(map[string]any{"path": "."})
	res, err := ListFiles{W: Workspace{Root: dir}, Max: 3}.Call(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	m := res.(map[string]any)
	if len(m["files"].([]string)) != 3 || m["truncated"] != true {
		t.Fatalf("%v", m)
	}
	res, _ = ListFiles{W: Workspace{Root: dir}}.Call(context.Background(), raw)
	if n := len(res.(map[string]any)["files"].([]string)); n != 10 {
		t.Fatalf(".git should be skipped, got %d files", n)
	}
}

func TestGrepFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.go"), []byte("package a\n\nfunc Hello() {}\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "b.go"), []byte("package b\n\nfunc World() {}\n"), 0o644)
	raw, _ := json.Marshal(map[string]any{"pattern": "func (Hello|World)"})
	res, err := GrepFile{W: Workspace{Root: dir}}.Call(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	hits := res.(map[string]any)["matches"].([]grepHit)
	if len(hits) != 2 {
		t.Fatalf("%v", hits)
	}
	raw, _ = json.Marshal(map[string]any{"pattern": "func", "max": 1})
	res, _ = GrepFile{W: Workspace{Root: dir}}.Call(context.Background(), raw)
	m := res.(map[string]any)
	if len(m["matches"].([]grepHit)) != 1 || m["truncated"] != true {
		t.Fatalf("%v", m)
	}
}

func TestGrepTruncationWithinFile(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("TODO 1\nTODO 2\nTODO 3\n"), 0o644)
	raw, _ := json.Marshal(map[string]any{"pattern": "TODO", "path": "a.txt", "max": 1})
	res, err := GrepFile{W: Workspace{Root: dir}}.Call(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	m := res.(map[string]any)
	if len(m["matches"].([]grepHit)) != 1 || m["truncated"] != true {
		t.Fatalf("single-file overflow must report truncated: %v", m)
	}
	// exactly at limit is not truncation
	raw, _ = json.Marshal(map[string]any{"pattern": "TODO", "path": "a.txt", "max": 3})
	res, _ = GrepFile{W: Workspace{Root: dir}}.Call(context.Background(), raw)
	if res.(map[string]any)["truncated"] != false {
		t.Fatal("at-limit flagged as truncated")
	}
}

func TestGrepSkipsSpecialFiles(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("TODO here\n"), 0o644)
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("TODO secret\n"), 0o644)
	os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "link.txt"))
	os.MkdirAll(filepath.Join(dir, "build"), 0o755)
	syscall.Mkfifo(filepath.Join(dir, "build", "output.pipe"), 0o644)
	big := strings.Repeat("TODO x\n", 1<<18)
	os.WriteFile(filepath.Join(dir, "big.log"), []byte(big), 0o644)

	raw, _ := json.Marshal(map[string]any{"pattern": "TODO"})
	done := make(chan any, 1)
	go func() {
		res, err := GrepFile{W: Workspace{Root: dir}}.Call(context.Background(), raw)
		if err != nil {
			t.Error(err)
		}
		done <- res
	}()
	select {
	case res := <-done:
		hits := res.(map[string]any)["matches"].([]grepHit)
		if len(hits) != 1 || hits[0].File != "a.txt" {
			t.Fatalf("expected only a.txt, got %v", hits)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("grep hung on workspace FIFO")
	}
	// explicit oversized file is skipped, same as in traversal
	raw, _ = json.Marshal(map[string]any{"pattern": "TODO", "path": "big.log"})
	res, _ := GrepFile{W: Workspace{Root: dir}}.Call(context.Background(), raw)
	if len(res.(map[string]any)["matches"].([]grepHit)) != 0 {
		t.Fatal("explicit oversized file searched")
	}
}
