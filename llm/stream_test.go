package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A fake OpenAI-compatible SSE endpoint: emits text deltas, a fragmented
// tool call, then [DONE].
func sseServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		emit := func(d string) {
			fmt.Fprintf(w, "data: %s\n\n", d)
			fl.Flush()
		}
		emit(`{"choices":[{"delta":{"role":"assistant","content":"Hel"}}]}`)
		emit(`{"choices":[{"delta":{"content":"lo"}}]}`)
		emit(`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"echo","arguments":"{\"te"}}]}}]}`)
		emit(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"xt\":\"hi\"}"}}]}}]}`)
		emit(`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`)
		emit(`[DONE]`)
	}))
}

func TestStreamAssemblesDeltas(t *testing.T) {
	ts := sseServer(t)
	defer ts.Close()
	p := &OpenAIProvider{BaseURL: ts.URL}
	var texts []string
	var intents []string
	resp, err := p.Stream(context.Background(), &Request{Model: "m", Messages: []Message{{Role: "user", Content: "hi"}}},
		func(d Delta) error {
			if d.Text != "" {
				texts = append(texts, d.Text)
			}
			if d.ToolName != "" {
				intents = append(intents, d.ToolName)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Message.Content != "Hello" {
		t.Fatalf("content=%q", resp.Message.Content)
	}
	if len(resp.Message.ToolCalls) != 1 || resp.Message.ToolCalls[0].Name != "echo" {
		t.Fatalf("calls=%+v", resp.Message.ToolCalls)
	}
	if string(resp.Message.ToolCalls[0].Args) != `{"text":"hi"}` {
		t.Fatalf("args=%s", resp.Message.ToolCalls[0].Args)
	}
	if resp.FinishReason != "tool_calls" {
		t.Fatalf("finish=%q", resp.FinishReason)
	}
	if strings.Join(texts, "") != "Hello" {
		t.Fatalf("deltas=%v", texts)
	}
}

func TestStreamErrorClassified(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		w.Write([]byte(`{"type":"quota_exceeded"}`))
	}))
	defer ts.Close()
	p := &OpenAIProvider{BaseURL: ts.URL}
	_, err := p.Stream(context.Background(), &Request{Model: "m"}, nil)
	var ce *CallError
	if !AsCallError(err, &ce) || ce.Kind != KindRateLimit {
		t.Fatalf("err=%v", err)
	}
}

// The wire payload carries the session's user key so gateways group
// the calls (#13879).
func TestStreamSendsUserField(t *testing.T) {
	var body []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer ts.Close()
	p := &OpenAIProvider{BaseURL: ts.URL}
	if _, err := p.Stream(context.Background(), &Request{Model: "m", User: "ses-1", Messages: []Message{{Role: "user", Content: "hi"}}}, func(Delta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["user"] != "ses-1" {
		t.Fatalf("user field missing/mismatched: %v", got["user"])
	}
}
