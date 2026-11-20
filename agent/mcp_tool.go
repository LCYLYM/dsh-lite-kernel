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

func (t MCPTool) Schema() json.RawMessage {
	if len(t.InputSchema) == 0 || !json.Valid(t.InputSchema) {
		return json.RawMessage(`{"type":"object"}`)
	}
	return t.InputSchema
}

func (t MCPTool) Call(ctx context.Context, args json.RawMessage) (any, error) {
	var a map[string]any
	if len(args) > 0 {
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, fmt.Errorf("agent: mcp tool %s got undecodable args: %w", t.Name(), err)
		}
	}
	res, err := t.Client.CallTool(ctx, t.CallTimeout, t.RemoteName, a)
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

// ListTools returns the server's advertised tools as ready-to-register
// MCPTool values. One tools/list at startup — a server that connects
// but fails to enumerate is a loud startup error, not a silent absence
// of tools; a server that later changes its set needs a reconnect.
func ListTools(c *mcp.Client, serverName string, callTimeout time.Duration) ([]MCPTool, error) {
	if callTimeout <= 0 {
		callTimeout = mcp.DefaultCallTimeout
	}
	raw, err := c.ToolList(context.Background())
	if err != nil {
		return nil, fmt.Errorf("mcp server %q tools/list: %w", serverName, err)
	}
	// call() already unwraps the envelope's result field.
	var list toolListResult
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("mcp server %q: undecodable tools/list", serverName)
	}
	var out []MCPTool
	for _, t := range list.Tools {
		if t.Name == "" {
			continue
		}
		out = append(out, MCPTool{
			Client: c, ServerName: serverName, RemoteName: t.Name,
			Desc: t.Description, InputSchema: t.InputSchema, CallTimeout: callTimeout,
		})
	}
	return out, nil
}
