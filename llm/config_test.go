package llm

import "testing"

func TestProfileResolveFailsLoud(t *testing.T) {
	for name, p := range map[string]Profile{
		"no-model":    {Provider: "openai", BaseURL: "https://x", APIKey: "k"},
		"no-key":      {Provider: "openai", BaseURL: "https://x", Model: "m"},
		"bad-url":     {Provider: "openai", BaseURL: "not a url", APIKey: "k", Model: "m"},
		"unknown":     {Provider: "mystery", Model: "m"},
		"empty-url":   {Provider: "openai", BaseURL: "", APIKey: "k", Model: "m"},
		"scheme-only": {Provider: "openai", BaseURL: "https://", APIKey: "k", Model: "m"},
	} {
		if _, _, err := p.Resolve(); err == nil {
			t.Fatalf("profile %q resolved without error", name)
		}
	}
}

func TestProfileResolveOK(t *testing.T) {
	p := Profile{Provider: "openai", BaseURL: "https://api.example.com/v1", APIKey: "k", Model: "m"}
	prov, model, err := p.Resolve()
	if err != nil || model != "m" {
		t.Fatalf("%v %q", err, model)
	}
	if _, ok := prov.(*OpenAIProvider); !ok {
		t.Fatalf("provider=%T", prov)
	}
	ep, model, err := Profile{Provider: "echo", Model: "echo-1"}.Resolve()
	if err != nil || model != "echo-1" {
		t.Fatal(err)
	}
	if _, ok := ep.(*EchoProvider); !ok {
		t.Fatalf("provider=%T", ep)
	}
}

func TestProfilesLookupFailsLoud(t *testing.T) {
	ps := Profiles{"main": {Provider: "echo", Model: "m"}}
	if _, _, err := ps.Resolve("summarizer"); err == nil {
		t.Fatal("missing profile resolved")
	}
	if _, _, err := ps.Resolve("main"); err != nil {
		t.Fatal(err)
	}
}
