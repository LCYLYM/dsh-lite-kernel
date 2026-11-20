package llm

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

// The qianfan regression: a 429 whose body says "quota_exceeded" must
// classify as RATE_LIMIT, not QUOTA (#13742).
func TestClassify429BodyQuotaIsRateLimit(t *testing.T) {
	body := []byte(`{"code":"token_plan_person_rate_limit_exceeded","message":"Token Plan Person request rate limit exceeded","type":"quota_exceeded"}`)
	resp := &http.Response{StatusCode: 429, Header: http.Header{}}
	ce := Classify(resp, body, nil)
	if ce.Kind != KindRateLimit {
		t.Fatalf("got %v, want KindRateLimit", ce.Kind)
	}
	if !ce.Retryable() {
		t.Fatal("rate limit must be retryable")
	}
}

func TestClassifyQuotaNon429(t *testing.T) {
	body := []byte(`{"error":{"code":"insufficient_quota","message":"balance empty","type":"quota"}}`)
	resp := &http.Response{StatusCode: 403, Header: http.Header{}}
	ce := Classify(resp, body, nil)
	if ce.Kind != KindQuota || ce.Retryable() {
		t.Fatalf("got kind=%v retryable=%v, want non-retryable quota", ce.Kind, ce.Retryable())
	}
}

// ECONNRESET-class transport errors are TRANSIENT — never fatal
// (#13722, #13629).
func TestClassifyECONNRESET(t *testing.T) {
	ce := Classify(nil, nil, fmt.Errorf("read: %w", errors.New("connection reset by peer")))
	if ce.Kind != KindTransient || !ce.Retryable() {
		t.Fatalf("got %v, want transient retryable", ce.Kind)
	}
}

func TestClassify5xxTransient(t *testing.T) {
	resp := &http.Response{StatusCode: 503, Header: http.Header{}}
	ce := Classify(resp, []byte("service unavailable"), nil)
	if ce.Kind != KindTransient {
		t.Fatalf("got %v", ce.Kind)
	}
}

// Provider vocabularies are case-erratic: a camel-case throttle code must
// still classify as retryable RATE_LIMIT, not degrade to INVALID.
func TestClassifyMixedCaseRateLimit(t *testing.T) {
	body := []byte(`{"error":{"code":"RateLimitExceeded","message":"slow down"}}`)
	resp := &http.Response{StatusCode: 400, Header: http.Header{}}
	ce := Classify(resp, body, nil)
	if ce.Kind != KindRateLimit || !ce.Retryable() {
		t.Fatalf("got kind=%v, want retryable rate limit", ce.Kind)
	}
}
