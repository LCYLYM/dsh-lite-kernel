package session

import (
	"strings"
	"testing"
)

func TestStoreExclusiveLock(t *testing.T) {
	dir := t.TempDir()
	st, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(dir); err == nil ||
		!strings.Contains(err.Error(), "already in use") {
		t.Fatalf("second opener got %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(dir); err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
}
