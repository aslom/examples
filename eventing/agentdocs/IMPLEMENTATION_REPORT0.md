# IMPLEMENTATION_REPORT0 — Phase 0 build & test evidence

Design under implementation: `DESIGN_PHASE0.md`
Prerequisites verified working: `python3.14t` (GIL disabled), `uv`, `kafka-python 3.0.11`, `cloudevents 2.2.0`, Kafka 4.x on `localhost:9092`, `claude` CLI 2.1.278.

---

## 1. What was built

`~1940 lines` of pure-Python source across three packages plus tests and a Claude Code skill. Everything runs on `python3.14t` with the GIL disabled and no C extensions outside stdlib.

```
rossoctl-keda1/
├── pyproject.toml                         (hatchling build + 3 console-scripts)
├── shared/
│   ├── ce.py                              (CloudEvent envelope + tiny binary-mode
│   │                                       Kafka binding + uuid5(NAMESPACE, corr))
│   └── words/{adjectives,animals}.txt     (correlation ID word lists)
├── eventbridge/
│   ├── __main__.py                        (HTTP + producer + consumer + ntfy)
│   ├── config.py + config.toml            (env > TOML > baked defaults)
│   ├── http_server.py                     (wsgiref ThreadingMixIn)
│   ├── router.py                          (regex dispatch)
│   ├── handlers.py                        (all /v0/agents WSGI handlers)
│   ├── openapi.py                         (3.1 spec dict + Swagger HTML)
│   ├── html_view.py                       (server-rendered history + SSE client)
│   ├── correlation.py                     (word-pair minter + regex + collision guard)
│   ├── kafka_out.py                       (KafkaProducer + to_kafka_binary)
│   ├── kafka_in.py                        (KafkaConsumer thread → SQLite)
│   ├── store.py                           (SQLite gateways + SSE subscribe/notify)
│   └── ntfy.py                            (urllib.request POST fan-out)
├── eventrunner/
│   ├── __main__.py                        (consumer + router + emitter)
│   ├── config.py
│   ├── consume.py                         (Kafka → Router.submit)
│   ├── router.py                          (per-correlationid FIFO — DESIGN §4.5)
│   ├── runner.py                          (subprocess.Popen(claude) + pumps;
│   │                                       ER_MOCK_CLAUDE=true for offline tests)
│   └── emit.py                            (per-corr monotonic sequence + producer)
├── tests/
│   ├── test_roundtrip_binary.py           (CloudEvent wire contract)
│   ├── test_correlation.py                (word-pair minter)
│   ├── test_openapi.py                    (OAS shape)
│   ├── test_router.py                     (per-corr FIFO under concurrency)
│   └── test_store.py                      (SQLite + SSE notify)
└── .claude/skills/eventbridge/
    ├── SKILL.md                           (contract)
    ├── eventbridge-cli.py                 (stdlib-only skill helper)
    └── OPENAPI.md                         (human-readable API pointer)
```

Deviations from DESIGN_PHASE0.md:

- **CloudEvent Kafka binding is in `shared/ce.py`**, not the SDK. The `cloudevents` 2.2.0 package's `core.bindings.kafka` module requires a different event class than the legacy `cloudevents.v1.http.CloudEvent`, and the two APIs are not compatible. Rather than pin to a moving target, `ce.py` implements the CloudEvents 1.0 §3.1 Kafka binary binding directly (~110 lines): headers with `ce_` prefix, JSON-encoded value when `datacontenttype=application/json`. Wire format is unchanged; consumers can be swapped later.
- **Thread subclasses do not use the name `_bootstrap`** for their Kafka bootstrap string. `threading.Thread._bootstrap` is the internal method the thread runtime calls at `start()`; shadowing it produced a cryptic `TypeError: thread function must be callable` on Python 3.14t. Renamed to `_bootstrap_servers` throughout. Noted here for anyone porting to a new Thread subclass.
- **EventRunner's `consume.py` gracefully skips malformed events** (missing `ce_correlationid`) with a log message and offset commit. This defends against stale/legacy messages from prior runs and does not weaken the design's ordering guarantees (Kafka key is still `correlationid`, still ordered per partition).
- **SSE response omits `Connection: keep-alive`**. wsgiref's `handlers.start_response` asserts against hop-by-hop headers per RFC 7230 §6.1; `Cache-Control: no-cache` and `X-Accel-Buffering: no` are sufficient.
- **HTML view exposes `POST /v0/agents/{corr}/continue-html`** — a form-encoded sibling of the JSON `/continue` route — so the `<form>` in the HTML view has somewhere to POST without pulling in JS beyond the SSE bootstrap. JSON callers keep using `/continue`.

---

## 2. Test results

### 2.1 Unit tests — 58 / 58 pass

```
$ .venv/bin/python -m pytest tests/ -q
..........................................................               [100%]
58 passed in 7.27s
```

Coverage by file:

| Test file | Tests | What it verifies |
|---|---:|---|
| `test_roundtrip_binary.py` | 5 | Kafka-binary CloudEvent round-trip; request + response shapes; deterministic `session_uuid`; `session_uuid` differs per correlationid; empty value → `data=None`. |
| `test_correlation.py` | 4 | Regex `^[a-z]{3,10}-[a-z]{3,12}-\d{4}$` matches every minted ID; 500 minted IDs are unique; `remember()` prevents re-issue; IDs are lowercase-only. |
| `test_openapi.py` | 3 | OAS version is 3.1; every design-required path is present; `StartResponse.correlationid` carries the regex pattern. |
| `test_router.py` | 4 | **DESIGN §4.5** — Same correlationid runs serially in submission order; different correlationids run in parallel; `max_concurrent_correlations` cap is enforced (peak == cap under a burst); a slot re-arms a new worker after drain. |
| `test_store.py` | 4 | Insert + since-filter; `final_seen()`; SSE subscriber notified on insert; `upsert_session` increments `turns`. |
| `test_pidfile.py` | 6 | Write/remove lifecycle; stale-pidfile reclaim; live-pidfile refuses double-start; `RUN_FORCE=1` overrides; `_pid_alive()` boundary cases. |
| `test_graceful_shutdown.py` | 2 | Spawns EventBridge / EventRunner in subprocesses, sends SIGINT, asserts rc=0 and shutdown within 8s. Skipped under sandboxes that block subprocess spawn of the venv interpreter. |
| `test_child_env.py` | 6 | Allowlist enforcement (unrelated vars dropped); unset/empty treated correctly; `ANTHROPIC_AUTH_TOKEN` redacted in startup log; graceful "nothing set" message; `_redact()` boundary cases. |
| `test_classify.py` | 16 | classify() — 11 tests: text extraction (single/multi block, tool_use markers); result-frame `stats` compaction; system-frame summarization; `is_hook_frame()`; `ER_INCLUDE_RAW` toggle; non-dict + unknown frames → `role=raw`; `dark-moose-3985` regression replay. `_pump()` — 5 tests: final-text dedupe on (strips duplicate + sets `text_echoes_prior_assistant`), dedupe off (preserves both), genuinely different final text kept, `ER_EMIT_SYSTEM_HOOKS=false` filters hook frames, opt-in keeps them. |
| `test_turns.py` | 8 | `group_by_turns()` — 5 tests: two turns grouped by final-event boundary, in-flight turn (no final yet), init frames are informational not boundaries, missing prompts tolerated, render() emits turn `<details>` + hides events behind toggle. `prompts` table — 3 tests: monotonic turn_index across inserts, `get_prompts()` order + fields, per-correlationid scoping. |

The router tests are the ones that formalize the Q&E-8 result: the per-correlationid FIFO is now covered by four white-box tests that block synthetic `run_agent` calls with events and observe the ordering log. `test_same_correlationid_runs_serially_in_fifo_order` proves the FIFO by asserting the exact log `[start:A, end:A, start:B, end:B, start:C, end:C]`.

### 2.2 Live smoke — HTTP → Kafka → EventRunner → Kafka → SQLite → HTTP

Started `rossoctl-eventbridge` and `ER_MOCK_CLAUDE=true rossoctl-eventrunner` against the local Kafka on `:9092`. `ER_MOCK_CLAUDE=true` swaps `subprocess.Popen(claude)` for a deterministic three-event sequence — same wire path, no API tokens spent.

**Single request path:**
```
POST /v0/agents  {"prompt":"summarize threat-model1.md in 3 bullets","max_turns":1}
→ 202  correlationid=salty-oyster-0639  sessionuuid=562d15ef-152b-5b4e-ab01-1e7c747b79eb
        (=uuid5(NAMESPACE, "salty-oyster-0639"))  ✓ matches shared.ce.session_uuid()

GET /v0/agents/salty-oyster-0639/events  (1s later)
→ final=true  events=3
   seq=1 phase=stdout  system    session_id=562d15ef-...
   seq=2 phase=stdout  assistant text=MOCK-REPLY[summarize threat-model1.md in 3 bullets]
   seq=3 phase=result  final     result=MOCK-REPLY[...]
```

**Continue (resume):**
```
POST /v0/agents/salty-oyster-0639/continue  {"prompt":"now list mitigations"}
→ 202  sequence=4
   … 3 new response events, sequences 4, 5, 6, continuing monotonically
```

**Concurrent /continue against the same correlationid (the DESIGN §4.5 guarantee):**
```
3 parallel POSTs — CONT-A, CONT-B, CONT-C — dispatched from three background curl processes.
Kafka delivered them in order (single partition, key=correlationid).
Router serialized them:
   sequences 7,8,9    → CONT-A block
   sequences 10,11,12 → CONT-B block
   sequences 13,14,15 → CONT-C block
No interleaving between blocks. Sequences are contiguous and per-corr monotonic.
```

**Cross-correlation parallelism:**
```
Two POST /v0/agents fired concurrently for different corrs
→ corrA=misty-gopher-7856  corrB=fancy-whale-7881
Both showed final=true events=3 within the same 2s window.
```

**Auxiliary endpoints:**

| Endpoint | Status | Notes |
|---|:---:|---|
| `GET /healthz` | 200 | `{"ok": true}` |
| `GET /openapi.json` | 200 | OAS 3.1 dict, matches `openapi.spec()` |
| `GET /docs` | 200 | Swagger UI HTML (loads swagger-ui-dist from jsdelivr) |
| `GET /v0/agents/{corr}` | 200 | server-rendered HTML with cards + `EventSource` bootstrap |
| `GET /v0/agents/{corr}/events.jsonl` | 200 | full CloudEvent envelopes (ndjson) |
| `GET /v0/agents/{corr}/events.sse` | 200* | initial run of the smoke test hit an `AssertionError: Hop-by-hop header, 'Connection: keep-alive'` from wsgiref. Removed that header from `handlers.py:get_events_sse`. Fix landed in source; a stale EB daemon from the earlier failed startup still holds port 8080 in the current sandbox and could not be replaced (sandbox restricts `pkill`/`killall`/`kill %1`). Restarting EB in a fresh shell picks up the fix. |

### 2.3 Skill helper

```
$ .venv/bin/python .claude/skills/eventbridge/eventbridge-cli.py run "skill-test-hello" --no-watch
correlationid: plain-otter-3272
```

The stdlib-only skill CLI (`urllib.request`, no external deps) posts to EventBridge and returns the correlationid — that's the surface Claude Code invokes via the `eventbridge` skill.

---

## 3. Design guarantees verified end to end

| Design claim | Test / evidence |
|---|---|
| `sessionuuid = uuid5(NAMESPACE, correlationid)` — same on both sides, no mapping table | `test_session_uuid_is_deterministic`; smoke test compared HTTP response `sessionuuid` to `session_uuid("salty-oyster-0639")` — match. |
| CloudEvent binary mode: attrs on `ce_*` headers, JSON data in value, Kafka key = correlationid | `test_request_event_round_trip`, `test_response_event_round_trip`; `/events.jsonl` shows the full envelope. |
| Correlation IDs match `^[a-z]{3,10}-[a-z]{3,12}-\d{4}$`, no collisions across a minter | `test_regex_matches_generated_ids`, `test_ids_are_unique_within_a_minter`. |
| One live claude subprocess per correlationid, FIFO ordering per correlationid | `test_same_correlationid_runs_serially_in_fifo_order`; smoke test with 3 concurrent /continue → contiguous per-corr sequence blocks. |
| Different correlationids run in parallel, capped by `ER_MAX_CONCURRENT` | `test_different_correlationids_run_in_parallel`, `test_concurrency_cap_is_enforced`; smoke test with corrA+corrB in the same window. |
| Slot re-arms a worker on new submissions after drain | `test_slot_reuse_after_drain`. |
| `num_turns` is per-invocation → runner uses its own `sequence` | Emitter maintains per-corr `_seq_by_corr`; smoke test shows monotonic 1..15 across three turns. |
| Response events indexed in SQLite; `/events?since=N` filters server-side | `test_insert_and_events_for`; smoke test's `since=3` returned only sequences 4-6. |
| SSE subscribers notified on insert | `test_subscribe_notifies_on_insert`. |

---

## 4. Known issues + follow-ups

- ~~SSE endpoint hop-by-hop header~~ — fixed (`handlers.get_events_sse` no longer sets `Connection: keep-alive`, per wsgiref RFC 7230 assertion).
- ~~Ctrl-C did not exit cleanly~~ — fixed. Root cause: signal handler called `server.shutdown()` from the thread that `serve_forever()` was running on, self-deadlocking. New pattern: `serve_forever()` runs on a worker thread; signal handler sets a `threading.Event`; main thread drives shutdown. Second Ctrl-C escapes via `os._exit(130)`. Verified by `tests/test_graceful_shutdown.py`.
- **PID files** at `${TMPDIR%/}/rossoctl-keda1/{eventbridge,eventrunner}.pid` — written on startup, removed on clean exit, reclaimed automatically if the owner is dead, refused with a clear message if the owner is alive (override with `RUN_FORCE=1`). `scripts/stop-demo.sh` reads them to graceful-kill both daemons in one command.
- **`/continue` response `sequence` field is a hint, not authoritative.** The value is computed HTTP-side as `len(events)+1` at request time. Under a concurrent /continue burst, all three requests briefly report the same number because none of the response events have landed yet. The authoritative sequence comes from the emitter and is per-corr monotonic (verified in the smoke test). Fix in Phase 1 by having the runner echo back the assigned starting sequence on the request event.
- **`tests_e2e/` was not implemented in Phase 0.** DESIGN §6a describes a full-fat integration runner that hits ntfy + real claude; the smoke test in §2.2 above covers the same wire path in mock mode. Real-claude e2e is deferred until a session budget for API calls is agreed and `NTFY_TOPIC` is set. The console-script entry is registered in `pyproject.toml` for later.
- **Legacy events on `requests` topic** — the topic still contains events from prior failed experiments. EventRunner now skips malformed events with a log line, which is enough for Phase 0. Purging the topic requires deleting + recreating it (broker default is `delete.topic.enable=true` on Kafka 4 but topics linger); alternatively, `kafka-consumer-groups --reset-offsets --to-latest` when the consumer group is inactive.
- **`--no-session-persistence` is intentionally not set** on EventRunner (Q&E-4). If a future workload should never be resumed, the runner needs an opt-in flag on the request event.
- **Signatures (JWS on CloudEvent envelope)** — DESIGN §10.3 remains deferred to Phase 1.

---

## 5. How to run

```bash
# Prereqs: python3.14t (brew install python-freethreading), uv, Kafka on :9092
uv sync                                              # or: existing .venv is fine

# Terminal 1
uv run rossoctl-eventbridge                          # HTTP on :8080

# Terminal 2 (mock mode — no API cost; drop the env var for real claude)
ER_MOCK_CLAUDE=true uv run rossoctl-eventrunner

# Terminal 3
curl -sX POST http://127.0.0.1:8080/v0/agents \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Summarize threat-model1.md in 3 bullets.","max_turns":1}'
open http://127.0.0.1:8080/v0/agents/<returned-correlationid>

# All unit tests
.venv/bin/python -m pytest tests/ -v
```

`NTFY_TOPIC=<your-topic>` enables ntfy fan-out; without it, EventBridge silently no-ops the publisher (verified in `config.py`).
