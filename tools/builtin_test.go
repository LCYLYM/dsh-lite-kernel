package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// File tools cannot escape the workspace (#13675 class of failure).
func TestWorkspaceEscapeRejected(t *testing.T) {
	dir := t.TempDir()
	w := Workspace{Root: dir}
	rf := ReadFile{W: w}
	for _, p := range []string{"..", "../..", "/etc/passwd", "x/../../etc/passwd"} {
		args, _ := json.Marshal(map[string]string{"path": p})
		if _, err := rf.Call(context.Background(), args); err == nil {
			t.Fatalf("path %q escaped", p)
		}
	}
}

func TestWriteThenRead(t *testing.T) {
	dir := t.TempDir()
	w := Workspace{Root: dir}
	wf := WriteFile{W: w}
	args, _ := json.Marshal(map[string]string{"path": "a/b.txt", "content": "hello"})
	if _, err := wf.Call(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "a", "b.txt"))
	if err != nil || string(b) != "hello" {
		t.Fatalf("got %q err %v", b, err)
	}
}
