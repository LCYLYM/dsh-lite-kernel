package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func editCall(t *testing.T, dir string, a map[string]any) (any, error) {
	t.Helper()
	raw, _ := json.Marshal(a)
	return EditFile{W: Workspace{Root: dir}}.Call(context.Background(), raw)
}

func TestEditFileReplace(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("one two one"), 0o644)
	res, err := editCall(t, dir, map[string]any{"path": "a.txt", "old": "one two", "new": "three"})
	if err != nil {
		t.Fatal(err)
	}
	if res.(map[string]any)["replaced"] != 1 {
		t.Fatalf("%v", res)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "a.txt"))
	if string(got) != "three one" {
		t.Fatalf("%q", got)
	}
}

func TestEditFileAmbiguous(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x x x"), 0o644)
	_, err := editCall(t, dir, map[string]any{"path": "a.txt", "old": "x", "new": "y"})
	if err == nil || !strings.Contains(err.Error(), "3 locations") {
		t.Fatalf("expected ambiguity error, got %v", err)
	}
	res, err := editCall(t, dir, map[string]any{"path": "a.txt", "old": "x", "new": "y", "all": true})
	if err != nil || res.(map[string]any)["replaced"] != 3 {
		t.Fatalf("%v %v", res, err)
	}
}

func TestEditFileGuards(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("abc"), 0o644)
	if _, err := editCall(t, dir, map[string]any{"path": "a.txt", "old": "", "new": "x"}); err == nil {
		t.Fatal("empty old accepted")
	}
	if _, err := editCall(t, dir, map[string]any{"path": "a.txt", "old": "zzz", "new": "x"}); err == nil {
		t.Fatal("missing old accepted")
	}
	if _, err := editCall(t, dir, map[string]any{"path": "../out", "old": "x", "new": "y"}); err == nil {
		t.Fatal("workspace escape accepted")
	}
}

func TestEditFileSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, "ext.txt")
	os.WriteFile(target, []byte("alpha"), 0o644)
	os.Symlink(target, filepath.Join(dir, "link.txt"))
	if _, err := editCall(t, dir, map[string]any{"path": "link.txt", "old": "alpha", "new": "beta"}); err == nil {
		t.Fatal("symlink edit escaped the workspace")
	}
	got, _ := os.ReadFile(target)
	if string(got) != "alpha" {
		t.Fatal("external file modified")
	}
}

func TestEditFileConcurrent(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("alpha beta"), 0o644)
	done := make(chan error, 2)
	go func() {
		_, err := editCall(t, dir, map[string]any{"path": "a.txt", "old": "alpha", "new": "ALPHA"})
		done <- err
	}()
	go func() {
		_, err := editCall(t, dir, map[string]any{"path": "a.txt", "old": "beta", "new": "BETA"})
		done <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	got, _ := os.ReadFile(filepath.Join(dir, "a.txt"))
	if string(got) != "ALPHA BETA" {
		t.Fatalf("lost update: %q", got)
	}
}

func TestWriteFileReplacesLinkNotTarget(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "real.txt"), []byte("original"), 0o644)
	os.Symlink("real.txt", filepath.Join(dir, "link.txt"))
	raw, _ := json.Marshal(map[string]any{"path": "link.txt", "content": "new"})
	if _, err := (WriteFile{W: Workspace{Root: dir}}).Call(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "real.txt"))
	if string(got) != "original" {
		t.Fatalf("write destroyed link target: %q", got)
	}
	if fi, _ := os.Lstat(filepath.Join(dir, "link.txt")); fi.Mode()&os.ModeSymlink != 0 {
		t.Fatal("link should have been replaced by a regular file")
	}
}

func TestWriteFileInitializesMissingWorkspace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fresh", "ws")
	raw, _ := json.Marshal(map[string]any{"path": "a.txt", "content": "x"})
	if _, err := (WriteFile{W: Workspace{Root: dir}}).Call(context.Background(), raw); err != nil {
		t.Fatalf("first write in new workspace: %v", err)
	}
}

func TestWriteFileDirSymlinkOutside(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	os.WriteFile(filepath.Join(outside, "f"), []byte("x"), 0o644)
	os.Symlink(filepath.Join(dir, "inner.txt"), filepath.Join(outside, "f"))
	os.Mkdir(filepath.Join(dir, "d"), 0o755)
	// leaf symlink (dir/f -> ws/inner.txt) living on an external dir
	os.Symlink(outside, filepath.Join(dir, "d", "ext"))
	os.Symlink(filepath.Join(outside, "f"), filepath.Join(dir, "d", "ext2"))
	// dir symlink to outside dir; leaf link -> inside
	os.Symlink(outside, filepath.Join(dir, "dirlink"))
	raw, _ := json.Marshal(map[string]any{"path": "dirlink/f", "content": "new"})
	if _, err := (WriteFile{W: Workspace{Root: dir}}).Call(context.Background(), raw); err == nil {
		t.Fatal("write through external dir link allowed")
	}
}

func TestReadOnMissingWorkspaceLeavesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ghost")
	raw, _ := json.Marshal(map[string]any{"path": "a.txt"})
	_, err := (ReadFile{W: Workspace{Root: dir}}).Call(context.Background(), raw)
	if err == nil {
		t.Fatal("read should fail")
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatal("read materialized the workspace")
	}
}
