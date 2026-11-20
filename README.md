# dsh-lite-kernel

精简版 agent harness 内核 — a minimal, memory-bounded rewrite of the
[DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness)
kernel in Go, with the kernel-level defects reported by its community
designed out ([triage](ISSUES_TRIAGE.md)).

中文摘要：这是参考 deepseek-ai/deepseek-harness 公开仓库重写的**精简版
内核**（测试版）。目标：内核常驻内存几 MB、单会话开销 ~0.2 KiB、上千
并发回合无压力；并把社区 Discussion 里确认有效的内核级问题在架构层
面修掉（详见 ISSUES_TRIAGE.md）。

## Design goals and honest numbers

| Goal | Mechanism | Measured (this VM, `go test ./bench -bench=.`) |
|------|-----------|-----------------------------------------------|
| Small footprint | Static Go binary, no VM runtime, ~8 B/event log index | **~0.18 KiB heap per session** (10k sessions ≈ 1.8 MiB) |
| High concurrency | Goroutine per turn, no locks held during model calls | **1,000 concurrent turns ≈ 1 s**, ~0.25 KiB each |
| Bounded per-session memory | Context assembled by tail-scan under a byte cap, never materialize history | 50k-event session → context built in **~9 ms / 4 MiB**, independent of history length |

About "上千万并发 / 几 MB": a single process can't literally hold ten
million model turns in a few megabytes — every real session needs a
socket, a log file and live state, and providers are the bottleneck long
before RAM. What this kernel *does* deliver is the property that makes
those numbers approachable at fleet scale: near-zero cost per idle
session and bounded memory per active one, so capacity is a function of
how many boxes you run, not how big the box is.

## What it fixes

Everything in [ISSUES_TRIAGE.md](ISSUES_TRIAGE.md). Highlights:

- **Resumable event stream** — `GET .../events?after_seq=N` replays the
  durable log then goes live; a dropped connection can never freeze
  state the way upstream's `session/control` stream did.
- **Writes with deadlines** — dead peers are closed, not fed events.
- **Classified retries** — 429 → `RATE_LIMIT` (even when the body says
  "quota"), `ECONNRESET`/EOF/5xx → `TRANSIENT`; only misconfiguration is
  fatal, and it fails loud at plugin setup.
- **Loop guards** — n-gram repetition abort, zero-tool-call streak cap,
  step budget; a degenerate turn ends `aborted`, never fake-`completed`.
- **`waiting` is a state, not a silence** — retry-budget exhaustion is
  logged and resumable (optional `AutoResume`).
- **Per-agent scopes** — tools mount on the agent's scope; same-named
  tools coexist; setup errors dispose cleanly.
- **Durable mailbox** — cross-agent notices are log entries, replayed on
  resume; nothing is lost while a parent idles.
- **No delete tool, workspace-confined file ops** — the kernel cannot
  erase anything outside its workspace; symlink escapes resolve to the
  real path and are rejected.
- **Sandboxed shell** — commands run in a user/mount/net namespace:
  private mounts, no network by default (`DSHK_SHELL_NET` opts in),
  credential-ish env vars scrubbed, `setpriv` drops the bounding set.
- **Bounded tool output** — every tool result is truncated to 64 KiB
  before it can flood the context budget.
- **Graceful shutdown** — SIGINT/SIGTERM drains requests (≤5 s), then
  `Store.Close` flushes every live log.
- **Token accounting** — every call lands a `turn/usage` event;
  `GET /sessions/{id}/usage` aggregates it and `MaxSessionTokens`
  (default 2M) aborts a runaway session at budget, not at the bill.
- **Schemas pass through verbatim** — `anyOf`/`oneOf` unions survive.

## Layout

```
kernel/    plugin lifecycle: scoped effects, LIFO disposal
llm/       provider seam, error classification, retry policy
session/   append-only JSONL log, offset index, bounded context,
           deterministic compaction, stream-copy fork, store flock
agent/     loop, guard, tool registry, durable mailbox, plan tool
mcp/       stdio MCP client: per-id dispatch, init timeout
server/    HTTP + SSE: sessions, turns, resumable event stream
tools/     echo, read/write/edit_file, list_files, grep, fetch
           (SSRF-checked), sandboxed shell — all workspace-confined
bench/     memory + concurrency benchmarks
cmd/dshk/  the binary (serve, run, and a client for every endpoint)
```

## Run

```sh
go build -o dshk ./cmd/dshk

# Headless turn (EchoProvider — no API key needed)
./dshk run "say hi"
./dshk run -session session-1-1 "continue"   # resume a session

# Server
./dshk serve -addr 127.0.0.1:3080 -data .dshk/sessions -workspace /path/to/ws
```

Every endpoint has a `dshk` subcommand against a running daemon
(`DSHK_ADDR`, `DSHK_TOKEN`; plain HTTP is refused for non-loopback
targets unless `DSHK_INSECURE=1`):

```sh
dshk ls                     # sessions on disk
dshk msg <id> hello there   # post a message (all args join)
dshk notice <id> [-nowake]  # mailbox notice; -nowake = wake:false
dshk fork <id>              # stream-copy clone
dshk cancel <id>            # abort the in-flight turn
dshk tail <id> [n]          # last n events
dshk history <id> [after]   # paged reads without SSE
dshk usage <id>             # aggregated token usage
dshk rm <id>                # delete
```

HTTP surface:

```
POST   /v1/sessions                     create
GET    /v1/sessions                     list (disk + live)
GET    /v1/sessions/{id}                workspace + created_at
POST   /v1/sessions/{id}/messages       run a turn (409 while busy)
GET    /v1/sessions/{id}/events         SSE; ?after_seq= & ?types=
GET    /v1/sessions/{id}/history        paged JSON events
GET    /v1/sessions/{id}/tail?n=        lazy tail for long sessions
POST   /v1/sessions/{id}/notices        mailbox notice {payload, wake}
GET    /v1/sessions/{id}/usage          token totals
POST   /v1/sessions/{id}/fork           stream-copy clone
POST   /v1/sessions/{id}/cancel         abort in-flight turn
DELETE /v1/sessions/{id}                remove (idempotent)
```

Real model: set `DEEPSEEK_API_KEY` (or `OPENAI_API_KEY`), optionally
`DSHK_BASE_URL` / `DSHK_MODEL`. Provider calls carry the session id as
the OpenAI `user` field so API gateways can correlate them.

## Test

```sh
go test ./...
go test ./bench -bench=. -benchmem
```

## License

MIT. This is a clean-room rewrite inspired by the public upstream repo's
architecture and its community's issue reports; no upstream code is
vendored.
