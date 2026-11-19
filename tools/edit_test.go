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
