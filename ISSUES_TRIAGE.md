# Issue triage — deepseek-ai/deepseek-harness community reports

The upstream repo has GitHub issues disabled; reports live in
[Discussions](https://github.com/deepseek-ai/deepseek-harness/discussions).
This file judges which reports describe real, kernel-level problems worth
fixing in a rewrite, which are valuable but out of kernel scope, and which
are noise. Each accepted item names the design answer in this repo.

## Accepted — fixed in this kernel

| # | Report | Verdict | Design answer here |
|---|--------|---------|--------------------|
| [#13747](https://github.com/deepseek-ai/deepseek-harness/discussions/13747) | One transient `service-unavailable` permanently kills the `session/control` stream; all projections freeze until renderer reload | Real, root-caused upstream | The event stream is **resumable by construction**: `GET /v1/sessions/{id}/events?after_seq=N` replays from the durable log then goes live. There is no stream that "dies"; a client reconnects and resumes (`server/server.go`). |
| [#13759](https://github.com/deepseek-ai/deepseek-harness/discussions/13759) | Dead WebSocket peers fed events for 5–16 min because write completion counted as heartbeat activity | Real, upstream fix in progress | SSE writes carry a **write deadline**; a peer that stops reading is closed at the deadline, not detected via application heartbeats (`server/server.go`). |
| [#13653](https://github.com/deepseek-ai/deepseek-harness/discussions/13653) | MCP `initialize` has no timeout — a hung stdio server blocks the agent forever; startup failures silent | Real, trivially reproducible | `mcp.Connect` bounds spawn + `initialize` with a deadline; spawn failure is a loud error (`mcp/client.go`). |
| [#13659](https://github.com/deepseek-ai/deepseek-harness/discussions/13659) | Model loops "做。做。…" — 97k chars, 0 tool calls, host marks `completed` | Real; the costliest class of bug — it does not error | `agent.Guard` does n-gram repetition detection plus a zero-tool-call streak cap; the turn ends `aborted` with the reason logged (`agent/guard.go`, `agent/loop.go`). |
| [#13621](https://github.com/deepseek-ai/deepseek-harness/discussions/13621) | Unattended goal turn goes silently idle after the retry budget is spent | Real | Retry exhaustion ends the turn in an explicit **`waiting`** state written to the log; `AutoResume` re-drives with capped backoff. Silent-idle is impossible (`agent/loop.go`). |
| [#13639](https://github.com/deepseek-ai/deepseek-harness/discussions/13639) | One long session costs hundreds of MB: full-history restore + repeated `structuredClone` | Real, matches our design goal | The log keeps an int64 offset index (~8 B/event). `BuildContext` tail-scans under a byte cap — 50k-event session builds context in ~9 ms / 4 MiB, independent of history size (`session/`). |
| [#13656](https://github.com/deepseek-ai/deepseek-harness/discussions/13656) | Subagent completion notice silently lost when the parent is idle | Real, deadlocks delegation trees | Mailbox notices are appended to the recipient's durable log before delivery; `Pending()` replays unacked notices on resume (`agent/mailbox.go`). |
| [#13647](https://github.com/deepseek-ai/deepseek-harness/discussions/13647) | "继续" after max-tokens is a stateless re-run; truncated reasoning output carries nothing forward | Real, well-diagnosed | Truncated assistant output is committed with `truncated=true`; the next request carries the partial output plus an explicit continue instruction (`agent/loop.go`, `session/context.go`). |
| [#13715](https://github.com/deepseek-ai/deepseek-harness/discussions/13715) | Empty retry-marker user message poisons strict providers (permanent 400s) | Real | Retry markers are control metadata; `BuildContext` never serializes them as user messages. |
| [#13742](https://github.com/deepseek-ai/deepseek-harness/discussions/13742) | 429 TPM throttle misclassified as `QUOTA` → turn aborts | Real | `llm.Classify`: HTTP 429 is `RATE_LIMIT` regardless of what the body calls it; honors `Retry-After` (`llm/errors.go`). |
| [#13722](https://github.com/deepseek-ai/deepseek-harness/discussions/13722), [#13629](https://github.com/deepseek-ai/deepseek-harness/discussions/13629) | `ECONNRESET` on a fetch treated as fatal → app crash | Real | Transport errors classify `TRANSIENT` and retry; only config errors are fatal. |
| [#13710](https://github.com/deepseek-ai/deepseek-harness/discussions/13710) | Per-agent tool presets mount into the root layer → second agent fails "already registered" | Real | Tools register on the **agent's scope**, never a global map; same-named tools coexist across scopes (`kernel/`, `agent/tools.go`). |
| [#13717](https://github.com/deepseek-ai/deepseek-harness/discussions/13717) | Seeded session stuffs the whole inherited log into the first request (8 MiB → 413 → permanent deadlock) | Real, root-caused | Request context is byte-capped by `ContextBudget` regardless of inherited history. |
| [#13719](https://github.com/deepseek-ai/deepseek-harness/discussions/13719) | `summarizationProvider` naming a missing route key accepted at load, fails at compaction with masked error | Real | Fail-loud rule: `kernel.Mount` aborts and disposes on any plugin setup error; config references are validated at setup. |
| [#13624](https://github.com/deepseek-ai/deepseek-harness/discussions/13624), [#13646](https://github.com/deepseek-ai/deepseek-harness/discussions/13646) | Blank-session minting races; stale policies carried over by reuse | Real | `CreateOrReuseBlank` is atomic under the store lock — tested under 32-way racing (`session/store.go`). |
| [#13630](https://github.com/deepseek-ai/deepseek-harness/discussions/13630) | MCP tool `anyOf` schema collapsed to the first branch | Real | Tool schemas pass through as `json.RawMessage` — nothing is rewritten (`agent/tools.go`, `llm/provider.go`). |
| [#13675](https://github.com/deepseek-ai/deepseek-harness/discussions/13675) | Host process permanently deleted ~86k files under `%USERPROFILE%` | Catastrophic, real | There is **no delete tool**. File tools resolve paths inside the workspace and reject escapes (`tools/builtin.go`). |
| [#13638](https://github.com/deepseek-ai/deepseek-harness/discussions/13638) | `ask_user_question` self-excited loop after toolset loss | Real | Tool loss can't corrupt a turn: specs are read from the scope each step; a zero-progress streak aborts. |
| [#13617](https://github.com/deepseek-ai/deepseek-harness/discussions/13617) | `dsh web` burns ~1.2 CPU cores forever when the profile dir is deleted | Real upstream; moot here | No fs-watching exists in the lite kernel — documented absence, not a fix. |

## Valid but out of kernel scope (UI/app-layer, packaging, platform)

These are real defects but belong to the desktop/web app or release
pipeline, which this kernel does not ship:

- [#13667](https://github.com/deepseek-ai/deepseek-harness/discussions/13667) 174 `:has()` rules → whole-page RecalcStyle
- [#13625](https://github.com/deepseek-ai/deepseek-harness/discussions/13625) LaTeX rendering stalls the composer
- [#13640](https://github.com/deepseek-ai/deepseek-harness/discussions/13640), [#13655](https://github.com/deepseek-ai/deepseek-harness/discussions/13655), [#13752](https://github.com/deepseek-ai/deepseek-harness/discussions/13752) Windows updater failures (signature path case, MoveFileExW, JSON parse)
- [#13711](https://github.com/deepseek-ai/deepseek-harness/discussions/13711), [#13707](https://github.com/deepseek-ai/deepseek-harness/discussions/13707) Desktop exits silently / 0xC0000005
- [#13740](https://github.com/deepseek-ai/deepseek-harness/discussions/13740), [#13669](https://github.com/deepseek-ai/deepseek-harness/discussions/13669), [#13628](https://github.com/deepseek-ai/deepseek-harness/discussions/13628), [#13654](https://github.com/deepseek-ai/deepseek-harness/discussions/13654), [#13661](https://github.com/deepseek-ai/deepseek-harness/discussions/13661), [#13648](https://github.com/deepseek-ai/deepseek-harness/discussions/13648) UI glitches (composer off-viewport, clipped button, usage card, caret, file picker, slash menu)
- [#13762](https://github.com/deepseek-ai/deepseek-harness/discussions/13762) Sidebar browser can't complete Feishu device auth
- [#13705](https://github.com/deepseek-ai/deepseek-harness/discussions/13705) Black console window flash on Windows tool exec
- [#13631](https://github.com/deepseek-ai/deepseek-harness/discussions/13631), [#13616](https://github.com/deepseek-ai/deepseek-harness/discussions/13616) Windows sandbox ACL / signature timeouts
- [#13642](https://github.com/deepseek-ai/deepseek-harness/discussions/13642) Atomic-write temp dir inside workspace kills external watchers (real; our writes rename in place and don't claim to interoperate with watchers)
- [#13672](https://github.com/deepseek-ai/deepseek-harness/discussions/13672) "撤回" retracts more than the message itself

## Feature requests — noted, not in a minimal kernel

Multiple isolated profiles (#13763), cross-session memory seam (#13749),
async systemPrompt context providers (#13739), PolicyGate fail-closed
(#13750), mobile ports (#13724), voice mode (#13633), model-picker UX
(#13649, #13657), agent-team/subagent UX (#13618, #13621→#13634 seams),
per-provider User-Agent override (#13670), `@`-mention filtering
(#13651), session-label seam (#13619). Reasonable asks; a lite kernel's
answer is that scope seams exist (`kernel.Scope`) — features mount as
plugins rather than being baked in.

## Noise / not actionable

- #13632 "vulnerable codes i dont know if its right or not but it maded by ai" — no repro
- #13741 发错了 (posted by mistake), #13704 "bug", #13636 LiveShare deployment question, #13641 question about source-checkout updates, #13713 transient transport error report
- Plugin showcase posts — community content, not defects
