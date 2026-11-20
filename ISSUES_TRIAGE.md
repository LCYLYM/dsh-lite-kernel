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
| [#13656](https://github.com/deepseek-ai/deepseek-harness/discussions/13656) | Subagent completion notice silently lost when the parent is idle | Real, deadlocks delegation trees | Mailbox notices are appended to the recipient's durable log before delivery; `Drain` consumes them **mid-turn** at every step (inject-before-ack, crash replays as an invisible dedup), and a notice landing on an idle session kicks `Loop.Resume` — the parent wakes instead of waiting for the next keystroke (`agent/mailbox.go`, `agent/loop.go`, `server/server.go`). |
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

## Hardening landed after the first pass

Defects found in design review of this kernel — same class of bugs the
community reports above, caught before they shipped:

- **Session log integrity**: corrupt mid-log lines or seq gaps fail loud at
  open; a partial crash tail is truncated as an uncommitted append
  (`session/log.go`, PR #14, #21).
- **Concurrent turns on one log** interleave events — second caller gets
  `ErrTurnBusy`; the server maps busy to `409` and drops the loop cache
  when a turn ends (PR #16, #17).
- **Two processes sharing a store dir** interleave `jsonl` — the second
  `NewStore` gets an exclusive-flock error, not a corrupted log (PR #13).
- **SSE subscriber-buffer overflow** dropped live events; the stream now
  refills gaps from the durable log by seq (PR #19).
- **Provider calls unbounded** — `Guard.CallTimeout` per call; an inner
  deadline classifies `TIMEOUT` (retryable → `waiting`), not a fatal
  turn (PR #18).
- **Tool results unbounded** — capped before hitting the log
  (`Guard.MaxToolResultChars`), truncation is model-visible (PR #20).
- **Symlink/FS edge cases in file tools** — escapes through link
  ancestors rejected, FIFOs rejected before read (a FIFO open blocks
  forever), uid/gid preserved on edit, write locks track where the
  rename actually lands (PR #11, #12).

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

## Hardening landed after the first pass (PR #25–#32)

Follow-up review rounds plus a second audit of the kernel turned up
further defects fixed in their own PRs:

| PR | Defect | Fix |
|----|--------|-----|
| #25 | `POST /v1/sessions` ignored decode errors (malformed JSON silently minted a blank session) and no endpoint bounded request bodies | `decodeJSON` wraps every body in a 1 MiB `MaxBytesReader`; bad input is a 400 |
| #26 | MCP responses pulled from a shared queue — a slow sibling call consumed another call's reply; writes interleaved; `CommandContext` killed the child when the init deadline's cancel ran | Replies demultiplexed by id through a `pending` map; writes serialized; the child is spawn-bound, not init-ctx-bound |
| #27 | A `tool/result` whose `assistant` call was evicted by the context budget reached the model as an orphan (strict providers 400) | `BuildContext` drops results whose `tool_call_id` isn't in the kept window |
| #28 | Provider error codes matched case-sensitively — `RateLimitExceeded` degraded to non-retryable `KindInvalid` | Codes folded once before classification |
| #29 | `read_file` allocated the whole file before enforcing its 1 MiB cap; a fifo blocked `os.Open` forever | `Stat` first: non-regular rejected, size checked from metadata, read capped by `LimitReader` |
| #30 | `Workspace`/`CreatedAt` lived in memory only — reloads lost them; the id counter restarted at 0 and reused `session-1-1`, appending into the old log | `session/meta` event carries the binding; the seq is seeded from disk and the id is `session-<seq>` |
| #31 | Events appended mid-replay were delivered twice on the SSE stream (log replay + live channel) | `lastSeq` dedupes across the replay/live seam |
| #32 | `Tail(0)` returned the last event; `Append` ignored its seek error; EOF detected by string compare | Edge cases corrected |
| #34 | Child-scope tool shadowing a same-named parent was advertised twice in `Specs` (same root as [#13829](https://github.com/deepseek-ai/deepseek-harness/discussions/13829): multi-preset global-layer name collisions) | First-found wins, matching `Lookup`; non-`Tool` registrations under `tool/` return an error instead of panicking |
| #35 | [#13822](https://github.com/deepseek-ai/deepseek-harness/discussions/13822): reasoning-only/empty `stop` responses bypass EMPTY_RESPONSE retry and complete silently | Empty reply → retryable `CallError`; stays empty → `Waiting` (logged, resumable); nothing empty reaches the log |

Second triage pass over newer discussions — already benign in the lite kernel:

- [#13842](https://github.com/deepseek-ai/deepseek-harness/discussions/13842) one corrupt JSONL aborts the whole listing → `Store.List` only ever returns live sessions; a corrupt file fails loud on `Get`, never kills the list.
- [#13834](https://github.com/deepseek-ai/deepseek-harness/discussions/13834) legacy `turn/end` without `reason` breaks projection → no turn/end projection exists; events are only filtered during context assembly.
- [#13840](https://github.com/deepseek-ai/deepseek-harness/discussions/13840), [#13826](https://github.com/deepseek-ai/deepseek-harness/discussions/13826), [#13827](https://github.com/deepseek-ai/deepseek-harness/discussions/13827) renderer OOM / jank on long sessions → same class as the bounded-log design (#13639, #13717).
- [#13829](https://github.com/deepseek-ai/deepseek-harness/discussions/13829) presets colliding in a global tool layer → scoped `kernel.Scope` registration (#13710); residual spec-level dedupe fixed in #34.

| #36 | `after_seq` / `Last-Event-ID` parse errors returned 500 | Bad cursor is a 400 |
| #37 | [#13812](https://github.com/deepseek-ai/deepseek-harness/discussions/13812): a persisted session invisible until first `Get` — `List` only reported live ids | `List` unions live ids with `session-*.jsonl` files on disk |
| #38 | [#13790](https://github.com/deepseek-ai/deepseek-harness/discussions/13790): a crashed turn leaves a dangling `tool_call`; strict providers then reject every later request, bricking the session | `BuildContext` splices an interrupted-marker result after a call with no result; synthesized results spend the same byte/event budget |
| #39 | [#13803](https://github.com/deepseek-ai/deepseek-harness/discussions/13803): one malformed wire frame killed the turn — decode failure wasn't a retryable `CallError` | Undecodable body → `KindTransient` |
| #40 | [#13766](https://github.com/deepseek-ai/deepseek-harness/discussions/13766): no way to abort an in-flight turn | `POST /v1/sessions/{id}/cancel` cancels the turn's context; 409 when idle |

Third triage pass — already benign or by-design in the lite kernel:

- [#13825](https://github.com/deepseek-ai/deepseek-harness/discussions/13825) hold-and-release of pending input at turn boundaries → the durable mailbox already queues notices until a turn consumes them; wake-on-notice keeps an idle session responsive.
- [#13806](https://github.com/deepseek-ai/deepseek-harness/discussions/13806) replaced stdio MCP servers keep running → no hot-reload of MCP config exists; a `Client` owns its child for its lifetime.
- [#13787](https://github.com/deepseek-ai/deepseek-harness/discussions/13787) unknown `FinishReason` throws → finish reasons pass through as strings; only `length`/`max_tokens` get special handling, anything else completes normally.
- [#13799](https://github.com/deepseek-ai/deepseek-harness/discussions/13799) sandboxed tool reads parent env → the file tools carry no environment; the sandboxed `shell` tool (PR #4) launches with a scrubbed env.
- [#13759](https://github.com/deepseek-ai/deepseek-harness/discussions/13759) dead WS peers never reaped → transport is SSE with a per-request context; a dead client is just a closed response writer.
- [#13767](https://github.com/deepseek-ai/deepseek-harness/discussions/13767), [#13768](https://github.com/deepseek-ai/deepseek-harness/discussions/13768) checkpoints vs full replay, async consistency → the lite answer is append-only + bounded tail-scan; reopening replays the log, not a checkpoint fork.

| #41 | Replay and `Tail` held `l.mu` while reading the log — a slow SSE catch-up on a big log blocked every `Append` from the running turn | Readers snapshot offsets/size under the lock, then read via a private fd + `LimitReader` outside it |
| #42 | No session fork; a copy built by replaying events would be O(history) and could collide on reused ids | `Fork` stream-copies the file (constant memory); `create` never reuses an id that exists on disk; a failed fork cleans up |
| #43 | Rendering a long session required streaming the whole log; no way to remove a session | `GET /v1/sessions/{id}/tail` (O(n) lazy tail), `DELETE /v1/sessions/{id}` (idempotent) |
| #44 | `write_file` shared one `.tmp` name (concurrent writes clobbered each other) and never fsynced | `writeAtomic`: unique temp + fsync + rename + dir fsync, matching `Log.Append` durability |
| #45 | Hosts had no way to flush all open sessions on shutdown | `Store.Close` closes every live log; `Get` lazily reopens afterwards |

Fourth triage pass (#13852–#13870) — already benign or by-design in the lite kernel:

- [#13862](https://github.com/deepseek-ai/deepseek-harness/discussions/13862) once messages exceed ~2× the model window, model-written compaction can never succeed → lite compaction is deterministic (drop-with-marker), needs no model call, and `BuildContext` is byte-bounded so the request can never outgrow the window.
- [#13859](https://github.com/deepseek-ai/dsh/discussions/13859) aborted parent turn leaves child without a settle line; a stop notice wakes the idle parent and the model re-dispatches → no subagents exist; wake-on-notice only fires on explicit mailbox notices, and cancellation (#40) records a `turn/end` every time.
- [#13863](https://github.com/deepseek-ai/deepseek-harness/discussions/13863) restart-cost economics / handoff → `turn/usage` accounting (#7) plus `Fork` (#42) covers the primitives; the EOQ policy itself is host-side.
- [#13858](https://github.com/deepseek-ai/deepseek-harness/discussions/13858) stable reference back to an original session event → every event carries a durable `seq`; `session id + seq` is already addressable (`after_seq` resume, `?n=` tail).
- [#13855](https://github.com/deepseek-ai/deepseek-harness/discussions/13855) maintenance phase reports `idle` → no maintenance phase; a session is either in a cancellable turn or not.
- [#13857](https://github.com/deepseek-ai/deepseek-harness/discussions/13857), [#13860](https://github.com/deepseek-ai/deepseek-harness/discussions/13860), [#13865](https://github.com/deepseek-ai/deepseek-harness/discussions/13865), [#13867](https://github.com/deepseek-ai/deepseek-harness/discussions/13867), [#13864](https://github.com/deepseek-ai/deepseek-harness/discussions/13864), [#13868](https://github.com/deepseek-ai/deepseek-harness/discussions/13868), [#13869](https://github.com/deepseek-ai/deepseek-harness/discussions/13869), [#13870](https://github.com/deepseek-ai/deepseek-harness/discussions/13870), [#13866](https://github.com/deepseek-ai/deepseek-harness/discussions/13866) → host UI, packaging, transport-client or feature-request territory; out of the kernel's scope.
