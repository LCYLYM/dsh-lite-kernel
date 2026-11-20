// dshk is the lite kernel binary: `serve` for the HTTP/SSE daemon,
// `run` for one headless turn.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/LCYLYM/dsh-lite-kernel/agent"
	"github.com/LCYLYM/dsh-lite-kernel/kernel"
	"github.com/LCYLYM/dsh-lite-kernel/llm"
	"github.com/LCYLYM/dsh-lite-kernel/server"
	"github.com/LCYLYM/dsh-lite-kernel/session"
	"github.com/LCYLYM/dsh-lite-kernel/tools"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: dshk <serve|run|ls|msg|fork|cancel|tail|history|rm> [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "run":
		cmdRun(os.Args[2:])
	case "ls", "msg", "fork", "cancel", "tail", "history", "rm":
		cmdAPI(os.Args[1], os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		os.Exit(2)
	}
}

// cmdAPI is the thin client half of dshk: every operation the server
// exposes gets a subcommand so sessions can be managed without curl.
// Target: DSHK_ADDR (default 127.0.0.1:3080), bearer DSHK_TOKEN.
func cmdAPI(name string, args []string) {
	addr := os.Getenv("DSHK_ADDR")
	if addr == "" {
		addr = "127.0.0.1:3080"
	}
	base := "http://" + addr + "/v1/sessions"
	call := func(method, path, body string) {
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, base+path, rdr)
		if err != nil {
			log.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if tok := os.Getenv("DSHK_TOKEN"); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer res.Body.Close()
		io.Copy(os.Stdout, res.Body)
		if res.StatusCode >= 400 {
			os.Exit(1)
		}
		fmt.Println()
	}
	arg := func(i int) string {
		if len(args) <= i {
			fmt.Fprintf(os.Stderr, "usage: dshk %s <session-id> ...\n", name)
			os.Exit(2)
		}
		return args[i]
	}
	switch name {
	case "ls":
		call("GET", "", "")
	case "msg":
		call("POST", "/"+arg(0)+"/messages", fmt.Sprintf("{\"text\": %q}", arg(1)))
	case "fork":
		call("POST", "/"+arg(0)+"/fork", "")
	case "cancel":
		call("POST", "/"+arg(0)+"/cancel", "")
	case "tail":
		n := "200"
		if len(args) > 1 {
			n = args[1]
		}
		call("GET", "/"+arg(0)+"/tail?n="+n, "")
	case "history":
		q := ""
		if len(args) > 1 {
			q = "?after=" + args[1]
		}
		call("GET", "/"+arg(0)+"/history"+q, "")
	case "rm":
		call("DELETE", "/"+arg(0), "")
	}
}

func provider() (llm.Provider, string) {
	key := os.Getenv("DEEPSEEK_API_KEY")
	base := os.Getenv("DSHK_BASE_URL")
	model := os.Getenv("DSHK_MODEL")
	// Match the endpoint to the key being sent: an OpenAI key must never
	// be attached to a request aimed at another provider.
	if key == "" {
		key = os.Getenv("OPENAI_API_KEY")
		if key != "" && base == "" {
			base = "https://api.openai.com/v1"
		}
		if key != "" && model == "" {
			model = "gpt-4o-mini"
		}
	}
	if base == "" {
		base = "https://api.deepseek.com/v1"
	}
	if model == "" {
		model = "deepseek-chat"
	}
	if key == "" {
		log.Printf("no API key — using local EchoProvider (deterministic, no network)")
		return &llm.EchoProvider{}, "echo"
	}
	return &llm.OpenAIProvider{BaseURL: base, APIKey: key}, model
}

func newLoopFactory(store *session.Store, prov llm.Provider, model string, ws string) func(*session.Session) *agent.Loop {
	return func(ses *session.Session) *agent.Loop {
		root := kernel.NewRoot("root")
		sc, _ := root.Child("agent/" + ses.ID)
		reg := agent.NewRegistry(sc, 60*time.Second)
		w := tools.Workspace{Root: ws}
		reg.Register(tools.Echo{})
		reg.Register(tools.ReadFile{W: w})
		reg.Register(tools.WriteFile{W: w})
		reg.Register(tools.EditFile{W: w})
		reg.Register(tools.ListFiles{W: w})
		reg.Register(tools.GrepFile{W: w})
		reg.Register(tools.Fetch{})
		// Shell runs under namespace confinement; on hosts without user
		// namespaces it reports "unavailable" per call unless the operator
		// explicitly opts into unsandboxed execution.
		reg.Register(tools.Shell{
			W:                w,
			AllowUnsandboxed: os.Getenv("DSHK_ALLOW_UNSANDBOXED") != "",
			AllowNet:         os.Getenv("DSHK_SHELL_NET") != "",
		})
		reg.Register(agent.PlanTool{Log: ses.Log})
		b := session.DefaultBudget()
		b.SystemText = "You are dsh-lite, a minimal agent. Use tools when needed."
		return &agent.Loop{
			Model: model, Provider: prov, Registry: reg, Log: ses.Log,
			Budget: b, Guard: agent.DefaultGuard(), Retry: llm.DefaultPolicy(),
			AutoResume: true, Stream: true,
		}
	}
}

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:3080", "listen address")
	dir := fs.String("data", ".dshk/sessions", "session data dir")
	ws := fs.String("workspace", ".", "workspace root for file tools")
	// Prefer DSHK_TOKEN: a -token flag lands in `ps` output readable by
	// other local users.
	token := fs.String("token", "", "bearer token required on every request; prefer the DSHK_TOKEN env var — argv is visible in process listings")
	_ = fs.Parse(args)

	store, err := session.NewStore(*dir)
	if err != nil {
		log.Fatal(err)
	}
	tok := os.Getenv("DSHK_TOKEN")
	if *token != "" {
		if tok == "" {
			tok = *token
			log.Print("warning: -token passed on argv is visible in process listings; prefer DSHK_TOKEN")
		}
	}
	loopback := strings.HasPrefix(*addr, "127.") || strings.HasPrefix(*addr, "localhost") || strings.HasPrefix(*addr, "[::1]")
	if tok == "" && !loopback {
		log.Fatal("refusing to serve without a token on a non-loopback address (set DSHK_TOKEN)")
	}
	if !loopback {
		log.Print("warning: serving bearer-auth over plain HTTP — terminate TLS in front (reverse proxy) or on-path observers can steal the token")
	}
	prov, model := provider()
	srv := &server.Server{Store: store, Provider: prov, Model: model,
		NewLoop: newLoopFactory(store, prov, model, *ws), Token: tok}

	log.Printf("dsh-lite serving on http://%s (workspace %s)", *addr, *ws)
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dir := fs.String("data", ".dshk/sessions", "session data dir")
	ws := fs.String("workspace", ".", "workspace root")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: dshk run [flags] \"task\"")
		os.Exit(2)
	}
	store, err := session.NewStore(*dir)
	if err != nil {
		log.Fatal(err)
	}
	ses, err := store.Create(*ws)
	if err != nil {
		log.Fatal(err)
	}
	prov, model := provider()
	loop := newLoopFactory(store, prov, model, *ws)(ses)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	state, err := loop.Run(ctx, fs.Arg(0))
	fmt.Printf("turn: %s (%v)\n", state, err)
}
