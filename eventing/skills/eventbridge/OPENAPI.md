# EventBridge OpenAPI reference (human-readable)

Full machine-readable spec at `GET /openapi.json`. Live UI at `GET /docs`.

## Start an agent

```
POST /v0/agents
Content-Type: application/json
{"prompt": "...", "model": "claude-opus-5", "max_turns": 3}

→ 202
{"correlationid": "wake-brave-otter-4718",
 "sessionuuid":   "5f0f5f0f-...",
 "event_id":      "...",
 "topic":         "requests",
 "html_url":      "http://127.0.0.1:8080/v0/agents/wake-brave-otter-4718"}
```

## Continue an agent (resumes same claude session)

```
POST /v0/agents/{correlationid}/continue
Content-Type: application/json
{"prompt": "Now list mitigations."}

→ 202  {"correlationid": "...", "sequence": <next>, "event_id": "..."}
```

## Get events

- `GET /v0/agents/{correlationid}/events?since=<N>` → JSON list, filterable by monotonic `sequence`.
- `GET /v0/agents/{correlationid}/events.jsonl`     → full CloudEvent JSON stream (ndjson).
- `GET /v0/agents/{correlationid}/events.sse`       → text/event-stream, one frame per new event.
- `GET /v0/agents/{correlationid}`                  → server-rendered HTML history view.

## Correlation ID scheme

`<adjective>-<animal>-<4 digits>`, e.g. `wake-brave-otter-4718`.
Regex: `^[a-z]{3,10}-[a-z]{3,12}-\d{4}$`.
