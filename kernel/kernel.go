// Package kernel is the plugin core: everything is a plugin, every
// registration is an effect scoped to a Scope, and disposing a scope
// reverses its effects children-first, then LIFO.
//
// Scopes replace the upstream "root tool layer": per-agent registrations
// mount into the agent's own scope, so two agents in one process can hold
// tools with the same name (deepseek-harness discussion #13710).
package kernel

import (
	"errors"
	"fmt"
	"sync"
)

// Disposable reverses a registered effect.
type Disposable func() error

// Plugin is the unit of composition. Setup registers effects on scope;
// it must fail loud on misconfiguration it can detect locally
// (deepseek-harness discussion #13719).
type Plugin interface {
	Name() string
	Setup(scope *Scope) error
}

// Scope owns registrations and child scopes. It is not safe to register
// on a scope while another goroutine disposes it; callers own that order.
type Scope struct {
	name   string
	parent *Scope

	mu       sync.Mutex
	services map[string]any
	dispos   []Disposable
	children map[*Scope]struct{}
	done     bool
}

// NewRoot creates a root scope.
func NewRoot(name string) *Scope {
	return &Scope{name: name, services: map[string]any{}, children: map[*Scope]struct{}{}}
}

// Child creates a scope nested under s (e.g. one per agent/session).
func (s *Scope) Child(name string) (*Scope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil, fmt.Errorf("kernel: scope %q is disposed", s.name)
	}
	c := &Scope{name: name, parent: s, services: map[string]any{}, children: map[*Scope]struct{}{}}
	s.children[c] = struct{}{}
	return c, nil
}

// Register puts a named service in this scope. Names are scope-local:
// a duplicate in the same scope is an error, a duplicate in an ancestor
// is fine — children shadow parents on lookup.
func (s *Scope) Register(name string, svc any) (Disposable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil, fmt.Errorf("kernel: scope %q is disposed", s.name)
	}
	if _, ok := s.services[name]; ok {
		return nil, fmt.Errorf("kernel: service %q is already registered in scope %q", name, s.name)
	}
	s.services[name] = svc
	var once sync.Once
	d := func() error {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.services, name)
		})
		return nil
	}
	s.dispos = append(s.dispos, d)
	return d, nil
}

// Effect registers an arbitrary side effect reversed by d.
func (s *Scope) Effect(d Disposable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return fmt.Errorf("kernel: scope %q is disposed", s.name)
	}
	s.dispos = append(s.dispos, d)
	return nil
}

// Parent returns the enclosing scope (nil on the root).
func (s *Scope) Parent() *Scope { return s.parent }

// Services returns a snapshot of this scope's own registrations.
func (s *Scope) Services() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, len(s.services))
	for k, v := range s.services {
		out[k] = v
	}
	return out
}

// Lookup finds a service walking up the scope chain.
func (s *Scope) Lookup(name string) (any, bool) {
	for sc := s; sc != nil; sc = sc.parent {
		sc.mu.Lock()
		v, ok := sc.services[name]
		sc.mu.Unlock()
		if ok {
			return v, true
		}
	}
	return nil, false
}

// Dispose releases children first, then own effects in reverse order.
func (s *Scope) Dispose() error {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return nil
	}
	s.done = true
	kids := make([]*Scope, 0, len(s.children))
	for c := range s.children {
		kids = append(kids, c)
	}
	ds := s.dispos
	s.dispos = nil
	s.mu.Unlock()

	var errs []error
	for _, c := range kids {
		if err := c.Dispose(); err != nil {
			errs = append(errs, err)
		}
	}
	for i := len(ds) - 1; i >= 0; i-- {
		if err := ds[i](); err != nil {
			errs = append(errs, err)
		}
	}
	if s.parent != nil {
		s.parent.mu.Lock()
		delete(s.parent.children, s)
		s.parent.mu.Unlock()
	}
	return errors.Join(errs...)
}

// Mount installs plugins into a fresh child scope; any failure disposes
// what was already installed, so a bad plugin never half-loads.
func Mount(parent *Scope, name string, plugins ...Plugin) (*Scope, error) {
	sc, err := parent.Child(name)
	if err != nil {
		return nil, err
	}
	for _, p := range plugins {
		if err := p.Setup(sc); err != nil {
			_ = sc.Dispose()
			return nil, fmt.Errorf("kernel: plugin %q: %w", p.Name(), err)
		}
	}
	return sc, nil
}
