package mcp

import (
	"context"
	"testing"
	"time"
)

// A stdio "server" that spawns but never answers must fail within the
// init timeout — never block the caller (#13653).
func TestInitializeTimeout(t *testing.T) {
	start := time.Now()
	_, err := Connect(context.Background(), 300*time.Millisecond, "sleep", "60")
	if err == nil {
		t.Fatal("expected initialize to fail on a silent server")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("took %v, should fail fast", elapsed)
	}
}

// A command that cannot spawn is a loud error, not silent absence.
func TestSpawnFailureIsLoud(t *testing.T) {
	_, err := Connect(context.Background(), time.Second, "definitely-not-a-real-binary-xyz")
	if err == nil {
		t.Fatal("expected loud spawn error")
	}
}
