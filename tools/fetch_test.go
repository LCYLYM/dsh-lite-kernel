package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fetchCall(t *testing.T, f Fetch, url string) map[string]any {
	t.Helper()
	args, _ := json.Marshal(map[string]string{"url": url})
	out, err := f.Call(context.Background(), args)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	return out.(map[string]any)
}

func TestFetchGetsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "hello body")
	}))
	defer srv.Close()
	f := Fetch{AllowLAN: true} // httptest is loopback
	res := fetchCall(t, f, srv.URL+"/x")
	if res["status"] != 200 || res["body"] != "hello body" {
		t.Fatalf("%v", res)
	}
}

// Loopback is refused by default — the metadata-endpoint class of SSRF.
func TestFetchRefusesLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	f := Fetch{}
	args, _ := json.Marshal(map[string]string{"url": srv.URL})
	if _, err := f.Call(context.Background(), args); err == nil ||
		!strings.Contains(err.Error(), "non-public") {
		t.Fatalf("loopback reached: %v", err)
	}
}

func TestFetchBodyCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(make([]byte, 4096))
	}))
	defer srv.Close()
	res := fetchCall(t, Fetch{AllowLAN: true, MaxBytes: 100}, srv.URL)
	if res["truncated"] != true || len(res["body"].(string)) != 100 {
		t.Fatalf("%v", res)
	}
}

func TestFetchRefusesSharedAddressSpace(t *testing.T) {
	// RFC 6598 shared space — Alibaba metadata lives at 100.100.100.200.
	args, _ := json.Marshal(map[string]string{"url": "http://100.100.100.200/latest/meta-data/"})
	_, err := Fetch{}.Call(context.Background(), args)
	if err == nil || !strings.Contains(err.Error(), "non-public") {
		t.Fatalf("expected non-public refusal, got %v", err)
	}
}
