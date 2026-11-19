package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
