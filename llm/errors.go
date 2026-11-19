package llm

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Kind classifies provider failures for retry policy.
type Kind int

const (
	KindUnknown Kind = iota
	KindTransient     // network resets, EOF, 5xx — always retryable
	KindRateLimit     // 429 / throttling — retryable, honor Retry-After
	KindQuota         // plan balance exhausted — not retryable
	KindInvalid       // 4xx request errors — not retryable
	KindTimeout       // deadline exceeded — retryable
	KindContentFilter // upstream moderation — not retryable
)

// CallError is a classified provider failure.
type CallError struct {
	Kind       Kind
	Status     int
	RetryAfter time.Duration
	Message    string
}

func (e *CallError) Error() string { return e.Message }

// Retryable reports whether the retry policy may attempt the call again.
func (e *CallError) Retryable() bool {
	switch e.Kind {
	case KindTransient, KindRateLimit, KindTimeout:
		return true
	}
	return false
}

// errorBody is the superset of the provider error envelopes seen in the
// wild (OpenAI-style, Anthropic-style, Baidu qianfan).
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
	Error   *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// Classify maps a response (or transport error) to a CallError.
//
// HTTP 429 is RATE_LIMIT even when the body shouts "quota_exceeded" —
// several providers (e.g. Baidu qianfan Token Plan) label rolling TPM
// limits as quota errors; aborting the turn on them kills tasks that
// would succeed one window later (deepseek-harness discussion #13742).
// Transport resets (ECONNRESET, EOF) are TRANSIENT, never fatal
// (#13722, #13629).
func Classify(resp *http.Response, body []byte, err error) *CallError {
	if err != nil {
		return classifyTransport(err)
	}
	ce := &CallError{Status: resp.StatusCode}
	var eb errorBody
	_ = json.Unmarshal(body, &eb)
	code, typ, msg := eb.Code, eb.Type, eb.Message
	if eb.Error != nil {
		if eb.Error.Code != "" {
			code = eb.Error.Code
		}
		if eb.Error.Type != "" {
			typ = eb.Error.Type
		}
		if eb.Error.Message != "" {
			msg = eb.Error.Message
		}
	}
	if msg == "" {
		msg = strings.TrimSpace(string(body))
	}
	if len(msg) > 300 {
		msg = msg[:300]
	}
	ce.Message = msg

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		// 429 is throttling by definition; parse Retry-After.
		ce.Kind = KindRateLimit
		ce.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	case strings.Contains(code+typ, "rate_limit") || strings.Contains(code+typ, "throttl"):
		ce.Kind = KindRateLimit
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusGatewayTimeout || resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable:
		ce.Kind = KindTransient
	case resp.StatusCode >= 500:
		ce.Kind = KindTransient
	case strings.Contains(code+typ+strings.ToLower(msg), "content_filter") ||
		strings.Contains(strings.ToLower(code+typ), "moderation"):
		ce.Kind = KindContentFilter
	case strings.Contains(code+typ, "quota") || strings.Contains(code+typ, "insufficient") ||
		strings.Contains(strings.ToLower(code+typ+msg), "balance"):
		// Genuine balance exhaustion, reached only on non-429 responses.
		ce.Kind = KindQuota
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		ce.Kind = KindInvalid
	default:
		ce.Kind = KindUnknown
	}
	return ce
}

func classifyTransport(err error) *CallError {
	ce := &CallError{Message: err.Error()}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		ce.Kind = KindTimeout
		return ce
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		ce.Kind = KindTransient
		return ce
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection reset"), strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection refused"), strings.Contains(msg, "no such host"),
		strings.Contains(msg, "econnreset"), strings.Contains(msg, "eof"):
		ce.Kind = KindTransient
	default:
		ce.Kind = KindTransient // unknown transport faults are safer retried
	}
	return ce
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if d, err := time.ParseDuration(v + "s"); err == nil {
		return d
	}
	if t, err := http.ParseTime(v); err == nil {
		return time.Until(t)
	}
	return 0
}
