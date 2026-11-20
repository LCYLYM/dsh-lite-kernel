package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
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

// An oversized file is rejected from its size metadata — the heap never
// sees its contents.
func TestReadFileOversize(t *testing.T) {
	dir := t.TempDir()
	big := filepath.Join(dir, "big.bin")
	if err := os.WriteFile(big, make([]byte, 2<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := (ReadFile{W: Workspace{Root: dir}}).Call(context.Background(),
		json.RawMessage(`{"path":"big.bin"}`))
	if err != nil {
		t.Fatal(err)
	}
	m, ok := out.(map[string]any)
	if !ok || m["error"] == nil {
		t.Fatalf("expected cap error, got %v", out)
	}
}

func TestReadFileRejectsFifo(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "pipe")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := (ReadFile{W: Workspace{Root: dir}}).Call(context.Background(),
		json.RawMessage(`{"path":"pipe"}`)); err == nil {
		t.Fatal("expected non-regular file error")
	}
}
