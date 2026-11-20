package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

// Fetch is a GET-only HTTP tool with SSRF posture: every dial resolves
// and rejects private/loopback/link-local addresses — at dial time, so
// each redirect hop is checked again and DNS answers are judged as
// dialed, not as looked up. Upstream had no such tool; the community
// complaint pattern was fetch tools reaching the cloud metadata
// endpoint and internal services.
type Fetch struct {
	MaxBytes  int64 // response body cap; 0 → 1MB
	Timeout   time.Duration
	AllowLAN  bool // permit private/loopback targets (dev setups)
	Redirects int  // max followed redirects; 0 → 3
}

func (t Fetch) Name() string        { return "fetch" }
func (t Fetch) Description() string { return "HTTP GET a URL; private-network targets are refused" }
func (t Fetch) Schema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}`)
}

func (t Fetch) client() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	tr := &http.Transport{
		// No pooling — each Call is one-shot; idle conns from
		// abandoned transports would accumulate otherwise.
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			var lastErr error
			permitted := false
			for _, ip := range ips {
				if !t.AllowLAN && !public(ip) {
					continue
				}
				permitted = true
				conn, derr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				if derr == nil {
					return conn, nil
				}
				lastErr = derr // first candidate dead? try the rest
			}
			if !permitted {
				return nil, fmt.Errorf("tools: fetch target %s resolves to a non-public address", host)
			}
			return nil, fmt.Errorf("tools: fetch dial %s: %w", host, lastErr)
		},
	}
	maxRedir := t.Redirects
	if maxRedir <= 0 {
		maxRedir = 3
	}
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) > maxRedir {
				return fmt.Errorf("tools: fetch redirect limit %d", maxRedir)
			}
			return nil
		},
	}
}

// cgnat is shared address space (RFC 6598) — netip.IsPrivate does not
// cover it, but cloud metadata endpoints (e.g. Alibaba's
// 100.100.100.200) live there.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

func public(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() &&
		!ip.IsMulticast() && !cgnat.Contains(ip)
}

func (t Fetch) Call(ctx context.Context, args json.RawMessage) (any, error) {
	var a struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	to := t.Timeout
	if to <= 0 {
		to = 20 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, "GET", a.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	max := t.MaxBytes
	if max <= 0 {
		max = 1 << 20
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, err
	}
	truncated := int64(len(body)) > max
	if truncated {
		body = body[:max]
	}
	return map[string]any{
		"status": resp.StatusCode, "body": string(body), "truncated": truncated,
	}, nil
}
