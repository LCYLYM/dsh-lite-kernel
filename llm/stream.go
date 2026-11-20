package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// Delta is one incremental piece of a streamed response. Tool-call
// fragments share a CallIndex and are assembled by the provider.
type Delta struct {
	Text      string
	ToolName  string
	CallIndex int
}

// Streamer is a Provider that can stream deltas. Loop prefers it when
// present so the event feed shows progress, not just a final blob.
type Streamer interface {
	Provider
	Stream(ctx context.Context, req *Request, delta func(Delta) error) (*Response, error)
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content   any    `json:"content"`
			Role      string `json:"role"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
}

// Stream implements Streamer for OpenAI-compatible endpoints.
func (p *OpenAIProvider) Stream(ctx context.Context, req *Request, delta func(Delta) error) (*Response, error) {
	or := openaiRequest{Model: req.Model, Messages: wireMessages(req.Messages), MaxTokens: req.MaxTokens, Temperature: req.Temperature, User: req.User}
	for _, t := range req.Tools {
		var ts toolSpec
		ts.Type = "function"
		ts.Function.Name = t.Name
		ts.Function.Description = t.Description
		ts.Function.Parameters = t.Schema
		or.Tools = append(or.Tools, ts)
	}
	or.Stream = true
	payload, err := json.Marshal(or)
	if err != nil {
		return nil, err
	}
	hc := p.Client
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Minute}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/event-stream")
	if p.APIKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+p.APIKey)
	}
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, Classify(nil, nil, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		return nil, Classify(resp, body, nil)
	}

	out := &Response{Message: Message{Role: "assistant"}}
	var text bytes.Buffer
	// toolCallBuf assembles fragmented call arguments by index.
	type callBuf struct {
		id, name string
		args     bytes.Buffer
	}
	calls := map[int]*callBuf{}
	var order []int

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 1<<20), 32<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if bytes.Equal(data, []byte("[DONE]")) {
			break
		}
		var ch streamChunk
		if err := json.Unmarshal(data, &ch); err != nil {
			continue
		}
		if len(ch.Choices) == 0 {
			continue
		}
		d := ch.Choices[0].Delta
		if s, ok := d.Content.(string); ok && s != "" {
			text.WriteString(s)
			if delta != nil {
				if err := delta(Delta{Text: s}); err != nil {
					return nil, err
				}
			}
		}
		for _, tc := range d.ToolCalls {
			b, ok := calls[tc.Index]
			if !ok {
				b = &callBuf{}
				calls[tc.Index] = b
				order = append(order, tc.Index)
			}
			if tc.ID != "" {
				b.id = tc.ID
			}
			if tc.Function.Name != "" {
				b.name += tc.Function.Name
				if delta != nil {
					if err := delta(Delta{ToolName: b.name, CallIndex: tc.Index}); err != nil {
						return nil, err
					}
				}
			}
			b.args.WriteString(tc.Function.Arguments)
		}
		if fr := ch.Choices[0].FinishReason; fr != nil {
			out.FinishReason = *fr
		}
	}
	if err := sc.Err(); err != nil {
		return nil, Classify(nil, nil, err)
	}
	out.Message.Content = text.String()
	for _, i := range order {
		b := calls[i]
		out.Message.ToolCalls = append(out.Message.ToolCalls, ToolCall{
			ID: b.id, Name: b.name, Args: json.RawMessage(b.args.String()),
		})
	}
	// A stream that ends before any finish_reason was truncated by the
	// transport — the partial answer must not be committed as complete.
	if out.FinishReason == "" {
		return nil, &CallError{Kind: KindTransient, Message: "llm: stream ended before finish_reason"}
	}
	return out, nil
}

// Stream implements Streamer for the deterministic test provider.
func (e *EchoProvider) Stream(ctx context.Context, req *Request, delta func(Delta) error) (*Response, error) {
	// Simulate deltas: emit the input text in pieces, then behave as
	// Complete does (one tool call, then a final answer on next call).
	resp, err := e.Complete(ctx, req)
	if err != nil {
		return nil, err
	}
	if s, ok := resp.Message.Content.(string); ok {
		for i := 0; i+8 <= len(s); i += 8 {
			if err := delta(Delta{Text: s[i:min(i+8, len(s))]}); err != nil {
				return nil, err
			}
		}
	}
	for _, tc := range resp.Message.ToolCalls {
		if err := delta(Delta{ToolName: tc.Name, CallIndex: 0}); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ Streamer = (*OpenAIProvider)(nil)
var _ Streamer = (*EchoProvider)(nil)
