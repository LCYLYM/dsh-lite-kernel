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

// A symlink inside the workspace pointing outside is an escape route;
// resolve must follow links, not just check path text.
func TestSymlinkEscapeRejected(t *testing.T) {
	dir := t.TempDir()
	out := t.TempDir()
	secret := filepath.Join(out, "secret.txt")
	os.WriteFile(secret, []byte("s3cret"), 0o600)
	if err := os.Symlink(out, filepath.Join(dir, "link")); err != nil {
		t.Skip("symlinks unsupported")
	}
	w := Workspace{Root: dir}
	rf := ReadFile{W: w}
	args, _ := json.Marshal(map[string]string{"path": "link/secret.txt"})
	if _, err := rf.Call(context.Background(), args); err == nil {
		t.Fatal("symlink escape was not rejected")
	}
	wf := WriteFile{W: w}
	args, _ = json.Marshal(map[string]string{"path": "link/new.txt", "content": "x"})
	if _, err := wf.Call(context.Background(), args); err == nil {
		t.Fatal("write through symlink was not rejected")
	}
	// A symlink pointing *inside* still works.
	os.MkdirAll(filepath.Join(dir, "real"), 0o755)
	os.WriteFile(filepath.Join(dir, "real", "f.txt"), []byte("ok"), 0o644)
	os.Symlink(filepath.Join(dir, "real"), filepath.Join(dir, "inlink"))
	args, _ = json.Marshal(map[string]string{"path": "inlink/f.txt"})
	if _, err := rf.Call(context.Background(), args); err != nil {
		t.Fatalf("inside symlink rejected: %v", err)
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
