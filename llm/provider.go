// Package llm is the model-provider seam: one Request/Response contract,
// classified failures, and bounded retries.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Message is one chat entry. Content is a string for user/system text or
// a structured value a provider understands.
type Message struct {
	Role       string     `json:"role"`
	Content    any        `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ToolCall is a model-requested tool invocation.
type ToolCall struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Args json.RawMessage `json:"arguments"`
}

// ToolSpec is passed to the model verbatim — the kernel never rewrites
// advertised schemas, so anyOf/oneOf unions survive intact
// (deepseek-harness discussion #13630).
type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"parameters"`
}

// Request is one model call.
type Request struct {
	Model       string     `json:"model"`
	Messages    []Message  `json:"messages"`
	Tools       []ToolSpec `json:"tools,omitempty"`
	MaxTokens   int        `json:"max_tokens,omitempty"`
	Temperature *float64   `json:"temperature,omitempty"`
	// User correlates requests to one end-user/session on gateways
	// (LiteLLM & co. group by it — #13879: without a stable key every
	// call looked like a fresh conversation).
	User string `json:"user,omitempty"`
}

// Usage reports token counts for the call.
type Usage struct {
	InputTokens  int `json:"prompt_tokens"`
	OutputTokens int `json:"completion_tokens"`
}

// Response is one completed model call.
type Response struct {
	Message      Message
	FinishReason string // "stop" | "length" | "tool_calls" | provider-native
	Usage        Usage
	Raw          json.RawMessage
}

// Provider executes model calls. Implementations must classify failures
// via Classify before returning them.
type Provider interface {
	Complete(ctx context.Context, req *Request) (*Response, error)
}

// openaiRequest is the wire shape for OpenAI-compatible endpoints.
type openaiRequest struct {
	Model       string        `json:"model"`
	Messages    []wireMessage `json:"messages"`
	Tools       []toolSpec    `json:"tools,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
	User        string        `json:"user,omitempty"`
}

// wireMessage is the outgoing message shape: the API requires tool
// calls as nested `function` objects with arguments as a JSON *string*,
// not the kernel's flat ToolCall form.
type wireMessage struct {
	Role       string         `json:"role"`
	Content    any            `json:"content,omitempty"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type wireToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func wireMessages(msgs []Message) []wireMessage {
	out := make([]wireMessage, 0, len(msgs))
	for _, m := range msgs {
		wm := wireMessage{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
		for _, tc := range m.ToolCalls {
			var w wireToolCall
			w.ID = tc.ID
			w.Type = "function"
			w.Function.Name = tc.Name
			w.Function.Arguments = string(tc.Args)
			wm.ToolCalls = append(wm.ToolCalls, w)
		}
		out = append(out, wm)
	}
	return out
}

type toolSpec struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

type openaiResponse struct {
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Role      string `json:"role"`
			Content   any    `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string          `json:"name"`
					Arguments json.RawMessage `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Usage Usage `json:"usage"`
}

// OpenAIProvider talks to any OpenAI-compatible /chat/completions endpoint.
type OpenAIProvider struct {
	BaseURL string
	APIKey  string
	Client  *http.Client
}

func (p *OpenAIProvider) Complete(ctx context.Context, req *Request) (*Response, error) {
	or := openaiRequest{Model: req.Model, Messages: wireMessages(req.Messages), MaxTokens: req.MaxTokens, Temperature: req.Temperature, User: req.User}
	for _, t := range req.Tools {
		var ts toolSpec
		ts.Type = "function"
		ts.Function.Name = t.Name
		ts.Function.Description = t.Description
		ts.Function.Parameters = t.Schema
		or.Tools = append(or.Tools, ts)
	}
	payload, err := json.Marshal(or)
	if err != nil {
		return nil, err
	}
	hc := p.Client
	if hc == nil {
		hc = &http.Client{Timeout: 120 * time.Second}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	if p.APIKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+p.APIKey)
	}
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, Classify(nil, nil, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, Classify(nil, nil, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, Classify(resp, body, nil)
	}
	var orr openaiResponse
	if err := json.Unmarshal(body, &orr); err != nil {
		// A malformed body — e.g. one tool call carrying an illegal JSON
		// escape — fails the turn permanently upstream (#13803). A bad
		// wire frame is not a state we asked for, so retry it like any
		// other transient fault instead of dying on the spot.
		return nil, &CallError{Kind: KindTransient, Message: fmt.Sprintf("llm: undecodable response: %v", err)}
	}
	if len(orr.Choices) == 0 {
		return nil, &CallError{Kind: KindTransient, Message: "llm: provider returned no choices"}
	}
	ch := orr.Choices[0]
	out := &Response{FinishReason: ch.FinishReason, Usage: orr.Usage, Raw: body}
	out.Message.Role = ch.Message.Role
	out.Message.Content = ch.Message.Content
	for _, tc := range ch.Message.ToolCalls {
		out.Message.ToolCalls = append(out.Message.ToolCalls, ToolCall{
			ID: tc.ID, Name: tc.Function.Name, Args: tc.Function.Arguments,
		})
	}
	return out, nil
}
