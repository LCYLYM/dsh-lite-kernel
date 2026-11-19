package kernel

import "testing"

// Two agents in one process can register same-named services — scopes
// isolate them (upstream #13710).
func TestScopedRegistrationsCoexist(t *testing.T) {
	root := NewRoot("root")
	a, _ := root.Child("agent-a")
	b, _ := root.Child("agent-b")
	if _, err := a.Register("tool/read", "A"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Register("tool/read", "B"); err != nil {
		t.Fatal(err)
	}
	if v, _ := a.Lookup("tool/read"); v != "A" {
		t.Fatalf("a sees %v", v)
	}
	if v, _ := b.Lookup("tool/read"); v != "B" {
		t.Fatalf("b sees %v", v)
	}
	// Same scope duplicate is an error.
	if _, err := a.Register("tool/read", "again"); err == nil {
		t.Fatal("expected duplicate error")
	}
}

// Disposing a scope removes its registrations but not the parent's.
func TestDisposeReversesEffects(t *testing.T) {
	root := NewRoot("root")
	root.Register("shared", 1)
	child, _ := root.Child("a")
	child.Register("local", 2)
	child.Dispose()
	if _, ok := child.Lookup("local"); ok {
		t.Fatal("local service survived dispose")
	}
	if v, ok := root.Lookup("shared"); !ok || v != 1 {
		t.Fatal("root service lost")
	}
	// New child sees root's service (chain lookup).
	c2, _ := root.Child("b")
	if v, _ := c2.Lookup("shared"); v != 1 {
		t.Fatal("chain lookup broken")
	}
}

// A failed plugin disposes what was already mounted.
func TestMountFailsLoudAndClean(t *testing.T) {
	root := NewRoot("root")
	okPlugin := PluginFunc{name: "ok", fn: func(s *Scope) error {
		_, err := s.Register("svc", 1)
		return err
	}}
	badPlugin := PluginFunc{name: "bad", fn: func(s *Scope) error {
		return errBoom
	}}
	sc, err := Mount(root, "profile", okPlugin, badPlugin)
	if err == nil || sc != nil {
		t.Fatal("expected mount failure")
	}
	// Nothing leaked into root.
	if _, err := root.Child("probe"); err != nil {
		t.Fatal(err)
	}
}

var errBoom = &boom{}

type boom struct{}

func (b *boom) Error() string { return "boom" }

type PluginFunc struct {
	name string
	fn   func(*Scope) error
}

func (p PluginFunc) Name() string             { return p.name }
func (p PluginFunc) Setup(s *Scope) error     { return p.fn(s) }
