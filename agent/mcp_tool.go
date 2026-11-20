package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/mcp"
)

// MCPTool adapts one tool advertised by an MCP server to the Tool
// interface: the remote spec goes to the model verbatim, and Call
// forwards to the server over the stdio JSON-RPC client.
type MCPTool struct {
	Client      *mcp.Client
	ServerName  string
	RemoteName  string
	Desc        string
	InputSchema json.RawMessage
	CallTimeout time.Duration
}

func (t MCPTool) Name() string { return "mcp_" + sanitize(t.ServerName) + "_" + sanitize(t.RemoteName) }

func (t MCPTool) Description() string { return t.Desc }

func (t MCPTool) Schema() any {
	var s any
	if err := json.Unmarshal(t.InputSchema, &s); err != nil || s == nil {
		return map[string]any{"type": "object"}
	}
	return s
}

func (t MCPTool) Call(ctx context.Context, args map[string]any) (any, error) {
	res, err := t.Client.CallTool(ctx, t.CallTimeout, t.RemoteName, args)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(res, &out); err != nil {
		return nil, fmt.Errorf("agent: mcp tool %s returned undecodable result: %w", t.Name(), err)
	}
	return out, nil
}

// sanitize keeps tool names inside the provider-safe alphabet
// [A-Za-z0-9_-]; server and tool names are operator-supplied and can
// contain anything.
func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, s)
}

// toolListResult is the MCP tools/list response shape.
type toolListResult struct {
	Tools []struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		InputSchema json.RawMessage `json:"inputSchema"`
	} `json:"tools"`
}

// RegisterMCP lists the server's tools and registers each as
// mcp_<server>_<tool> in the registry. A server that connects but
// fails tools/list is a startup error, not a silent absence of tools.
func RegisterMCP(reg *Registry, serverName string, c *mcp.Client, callTimeout time.Duration) (int, error) {
	if callTimeout <= 0 {
		callTimeout = mcp.DefaultCallTimeout
	}
	raw, err := c.ToolList(context.Background())
	if err != nil {
		return 0, fmt.Errorf("mcp server %q tools/list: %w", serverName, err)
	}
	var env struct {
		Result toolListResult `json:"result"`
	}
	// The client's call() already unwraps result, so raw IS the result.
	if err := json.Unmarshal(raw, &env); err != nil {
		return 0, err
	}
	var list toolListResult
	if env.Result.Tools == nil {
		if err := json.Unmarshal(raw, &list); err != nil {
			return 0, fmt.Errorf("mcp server %q: undecodable tools/list", serverName)
		}
	} else {
		list = env.Result
	}
	for _, t := range list.Tools {
		if t.Name == "" {
			continue
		}
		if _, err := reg.Register(MCPTool{
			Client: c, ServerName: serverName, RemoteName: t.Name,
			Desc: t.Description, InputSchema: t.InputSchema, CallTimeout: callTimeout,
		}); err != nil {
			return 0, err
		}
	}
	return len(list.Tools), nil
}
