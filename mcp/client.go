// Package mcp is a minimal stdio JSON-RPC client. Its one job beyond the
// wire protocol is the fix for discussion #13653: every external step —
// spawn, initialize, list, call — has a deadline, and startup failures
// are loud errors, not silent absence of tools.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"
)

const DefaultInitTimeout = 15 * time.Second
const DefaultCallTimeout = 60 * time.Second

// Client is one stdio MCP server connection.
type Client struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser

	mu      sync.Mutex
	nextID  int
	pending map[int]chan rpcMsg
	wrMu    sync.Mutex
	done    chan error
}

// Connect spawns command and completes the MCP initialize handshake
// before returning. A server that spawns but never answers fails here,
// within initTimeout — it cannot block the caller forever (#13653).
func Connect(ctx context.Context, initTimeout time.Duration, command string, args ...string) (*Client, error) {
	if initTimeout <= 0 {
		initTimeout = DefaultInitTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()

	// The child must outlive the init deadline: CommandContext would
	// kill it the moment cctx ends, so spawn unbound and kill on
	// handshake failure or Close instead.
	cmd := exec.Command(command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp: %s: %w", command, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp: %s: %w", command, err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("mcp: spawn %s: %w", command, err)
	}
	c := &Client{cmd: cmd, stdin: stdin, pending: map[int]chan rpcMsg{}, done: make(chan error, 1)}
	go c.readLoop(stdout)

	_, err = c.call(cctx, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "dsh-lite-kernel", "version": "0.1.0"},
	})
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("mcp: initialize %s: %w", command, err)
	}
	_ = c.notify("notifications/initialized", nil)
	return c, nil
}

// ToolList returns the server's tools; schemas pass through as raw JSON.
func (c *Client) ToolList(ctx context.Context) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, DefaultCallTimeout)
	defer cancel()
	return c.call(ctx, "tools/list", map[string]any{})
}

// CallTool invokes name with args, bounded by timeout.
func (c *Client) CallTool(ctx context.Context, timeout time.Duration, name string, args map[string]any) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.call(ctx, "tools/call", map[string]any{"name": name, "arguments": args})
}

// Close kills the child process.
func (c *Client) Close() error {
	if c.cmd.Process != nil {
		return c.cmd.Process.Kill()
	}
	return nil
}

type rpcMsg struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// call registers a response slot before writing so the read loop can
// route the reply to this caller; a slow sibling call can no longer
// consume another call's response out of a shared queue.
func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	ch := make(chan rpcMsg, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	body, err := json.Marshal(rpcMsg{JSONRPC: "2.0", ID: id, Method: method, Params: mustJSON(params)})
	if err != nil {
		return nil, err
	}
	c.wrMu.Lock()
	_, err = c.stdin.Write(append(body, '\n'))
	c.wrMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		// A response that raced in before the slot was dropped still
		// wins — including its error.
		select {
		case m := <-ch:
			if m.Error != nil {
				return nil, fmt.Errorf("mcp: %s: %s", method, m.Error.Message)
			}
			return m.Result, nil
		default:
		}
		return nil, ctx.Err()
	case err := <-c.done:
		return nil, err
	case m := <-ch:
		if m.Error != nil {
			return nil, fmt.Errorf("mcp: %s: %s", method, m.Error.Message)
		}
		return m.Result, nil
	}
}

func (c *Client) notify(method string, params any) error {
	body, err := json.Marshal(rpcMsg{JSONRPC: "2.0", Method: method, Params: mustJSON(params)})
	if err != nil {
		return err
	}
	c.wrMu.Lock()
	defer c.wrMu.Unlock()
	_, err = c.stdin.Write(append(body, '\n'))
	return err
}

func (c *Client) readLoop(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 32<<20)
	for sc.Scan() {
		var m rpcMsg
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		if m.ID == 0 {
			continue // server notification: no caller is waiting
		}
		c.mu.Lock()
		if ch, ok := c.pending[m.ID]; ok {
			delete(c.pending, m.ID)
			ch <- m
		}
		c.mu.Unlock()
	}
	err := sc.Err()
	if err == nil {
		err = io.EOF
	}
	select {
	case c.done <- fmt.Errorf("mcp: server closed pipe: %w", err):
	default:
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
