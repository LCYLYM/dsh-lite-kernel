package llm

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Profile is one named model route: which provider, which endpoint,
// which credential, which model.
type Profile struct {
	Provider string // "openai" (any OpenAI-compatible endpoint) | "echo"
	BaseURL  string // required for remote providers
	APIKey   string // required for remote providers
	Model    string // required
	Timeout  time.Duration
}

// Profiles is a route table — the map a summarizer/subagent/tool names
// when it says "use profile X".
type Profiles map[string]Profile

// Resolve validates and instantiates a profile, failing loud at
// startup — unknown provider names, missing keys, and malformed URLs
// are configuration errors, not runtime errors. Upstream's failure
// this prevents (discussion #13719): a route referenced by name that
// doesn't exist only surfaced deep inside a turn as a masked error.
func (p Profile) Resolve() (Provider, string, error) {
	if p.Model == "" {
		return nil, "", fmt.Errorf("llm: profile missing model")
	}
	switch p.Provider {
	case "echo":
		return &EchoProvider{}, p.Model, nil
	case "openai", "":
		if p.APIKey == "" {
			return nil, "", fmt.Errorf("llm: profile for %q missing API key", p.Model)
		}
		u, err := url.Parse(p.BaseURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, "", fmt.Errorf("llm: profile for %q has malformed base URL %q", p.Model, p.BaseURL)
		}
		to := p.Timeout
		if to <= 0 {
			to = 120 * time.Second
		}
		return &OpenAIProvider{
			BaseURL: p.BaseURL, APIKey: p.APIKey,
			Client: &http.Client{Timeout: to},
		}, p.Model, nil
	default:
		return nil, "", fmt.Errorf("llm: unknown provider %q", p.Provider)
	}
}

// Resolve looks a route up by name, failing loud when absent — a
// silent fallback to another profile would send a key to the wrong
// endpoint.
func (ps Profiles) Resolve(name string) (Provider, string, error) {
	p, ok := ps[name]
	if !ok {
		return nil, "", fmt.Errorf("llm: no such profile %q", name)
	}
	return p.Resolve()
}
