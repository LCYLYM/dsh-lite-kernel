package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/kernel"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
)

// Tool is a model-invocable function. Schema goes to the model verbatim.
type Tool interface {
	Name() string
	Description() string
	Schema() json.RawMessage
	Call(ctx context.Context, args json.RawMessage) (any, error)
}

// Registry is the tool seam for one agent scope — never a global map, so
// two agents can register same-named tools without colliding (#13710).
type Registry struct {
	scope   *kernel.Scope
	timeout time.Duration
}

func NewRegistry(scope *kernel.Scope, callTimeout time.Duration) *Registry {
	return &Registry{scope: scope, timeout: callTimeout}
}

// Register mounts a tool into the agent's scope; the returned disposer
// removes it.
func (r *Registry) Register(t Tool) (kernel.Disposable, error) {
	return r.scope.Register("tool/"+t.Name(), t)
}

// Specs returns the tool list as advertised to the model — schemas pass
// through untouched (union types survive, #13630).
func (r *Registry) Specs() []llm.ToolSpec {
	var out []llm.ToolSpec
	for sc := r.scope; sc != nil; sc = sc.Parent() {
		for name, svc := range sc.Services() {
			if len(name) > 5 && name[:5] == "tool/" {
				if t, ok := svc.(Tool); ok {
					out = append(out, llm.ToolSpec{Name: t.Name(), Description: t.Description(), Schema: t.Schema()})
				}
			}
		}
	}
	return out
}

// Call invokes a tool by name with a hard timeout — a hung tool never
// blocks the loop indefinitely (the same class of fix as the MCP
// initialize timeout, #13653).
func (r *Registry) Call(ctx context.Context, tc llm.ToolCall) (any, error) {
	svc, ok := r.scope.Lookup("tool/" + tc.Name)
	if !ok {
		return nil, fmt.Errorf("agent: tool %q is not registered in this scope", tc.Name)
	}
	t := svc.(Tool)
	to := r.timeout
	if to <= 0 {
		to = 60 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	return t.Call(cctx, tc.Args)
}
