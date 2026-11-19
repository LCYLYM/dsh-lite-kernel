package llm

import (
	"context"
	"encoding/json"
	"fmt"
)

// EchoProvider is a deterministic no-network provider for tests and
// benchmarks: it calls the echo tool once with the last user text, then
// returns a final answer.
type EchoProvider struct {
	calls int
}

func (e *EchoProvider) Complete(_ context.Context, req *Request) (*Response, error) {
	e.calls++
	lastUser := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			if s, ok := req.Messages[i].Content.(string); ok {
				lastUser = s
			}
			break
		}
	}
	if e.calls == 1 {
		args, _ := json.Marshal(map[string]string{"text": lastUser})
		return &Response{
			Message: Message{Role: "assistant", ToolCalls: []ToolCall{
				{ID: "c1", Name: "echo", Args: args},
			}},
			FinishReason: "tool_calls",
		}, nil
	}
	return &Response{
		Message:      Message{Role: "assistant", Content: fmt.Sprintf("echoed: %s", lastUser)},
		FinishReason: "stop",
	}, nil
}
