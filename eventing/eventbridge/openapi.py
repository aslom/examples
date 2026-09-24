"""OpenAPI 3.1 spec + Swagger UI HTML. Hand-written, no framework."""
from __future__ import annotations


def spec() -> dict:
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "rossoctl-keda1 EventBridge",
            "version": "0.1.0",
            "description": "Local signed-event wake demo — Phase 0. HTTP ↔ CloudEvents adapter over Kafka.",
        },
        "servers": [{"url": "http://127.0.0.1:8080"}],
        "paths": {
            "/v0/agents": {
                "post": {
                    "summary": "Start a new agent run",
                    "operationId": "startAgent",
                    "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/StartRequest"}}}},
                    "responses": {"202": {"description": "Accepted", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/StartResponse"}}}}},
                }
            },
            "/v0/agents/{correlationid}/continue": {
                "post": {
                    "summary": "Send a new prompt to the same correlationid (resumes claude session)",
                    "operationId": "continueAgent",
                    "parameters": [{"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}}],
                    "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ContinueRequest"}}}},
                    "responses": {"202": {"description": "Accepted"}, "404": {"description": "unknown correlationid"}},
                }
            },
            "/v0/agents/{correlationid}": {
                "get": {
                    "summary": "HTML history view",
                    "operationId": "getAgentHtml",
                    "parameters": [
                        {"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}},
                        {"in": "query", "name": "continue", "required": False,
                         "schema": {"type": "string", "enum": ["1", "true", "yes", "on"]},
                         "description": "Set to 1 to reveal the Continue-this-conversation textarea + button. Hidden by default so read-only viewers can't accidentally submit."},
                    ],
                    "responses": {"200": {"description": "HTML", "content": {"text/html": {}}}, "404": {"description": "not found"}},
                }
            },
            "/v0/agents/{correlationid}/turns": {
                "get": {
                    "summary": "Turn-grouped chat view — pairs each user prompt with its assistant reply",
                    "operationId": "getAgentTurns",
                    "parameters": [
                        {"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}},
                        {"in": "query", "name": "full", "required": False,
                         "schema": {"type": "boolean"},
                         "description": "Set full=1 to include the flat event list under each turn"},
                    ],
                    "responses": {"200": {"description": "OK", "content": {"application/json": {}}}},
                }
            },
            "/v0/groups": {
                "post": {
                    "summary": "Create a group of agents and fan it out",
                    "description": (
                        "Submits every prompt BEFORE anything is watched, which is what "
                        "makes the batch scale: N pending requests means consumer lag N, "
                        "so KEDA scales past one pod. Submitting one at a time lets each "
                        "finish before the next arrives and nothing ever scales. "
                        "Send an Idempotency-Key header so a client retry cannot launch "
                        "the batch twice. See DESIGN_PHASE1.md §21."
                    ),
                    "operationId": "createGroup",
                    "parameters": [{"in": "header", "name": "Idempotency-Key",
                                    "required": False,
                                    "schema": {"type": "string"}}],
                    "requestBody": {"required": True, "content": {"application/json": {
                        "schema": {"$ref": "#/components/schemas/CreateGroupRequest"}}}},
                    "responses": {
                        "202": {"description": "Created and fanned out",
                                "content": {"application/json": {}}},
                        "200": {"description": "Idempotency-Key replay — the original group"},
                        "400": {"description": "no prompts/expected, or they contradict"},
                    },
                },
                "get": {"summary": "Recent groups", "operationId": "listGroups",
                        "responses": {"200": {"description": "OK"}}},
            },
            "/v0/groups/{groupid}": {
                "get": {
                    "summary": "Group progress page (HTML)",
                    "description": (
                        "A dashboard, not a transcript. The percent-done bar is honest "
                        "here because the denominator is the number of agents; a "
                        "per-agent percentage would not be, so member rows show state."
                    ),
                    "operationId": "getGroupHtml",
                    "parameters": [{"in": "path", "name": "groupid", "required": True,
                                    "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "HTML", "content": {"text/html": {}}},
                                  "404": {"description": "unknown groupid"}},
                },
            },
            "/v0/groups/{groupid}/status": {
                "get": {
                    "summary": "Group progress as JSON — what the CLI polls",
                    "operationId": "getGroupStatus",
                    "parameters": [
                        {"in": "path", "name": "groupid", "required": True,
                         "schema": {"type": "string"}},
                        {"in": "query", "name": "full", "required": False,
                         "schema": {"type": "boolean"},
                         "description": "include full member rows"},
                    ],
                    "responses": {
                        "200": {"description": "OK", "content": {"application/json": {
                            "schema": {"$ref": "#/components/schemas/GroupStatus"}}}},
                        "404": {"description": "unknown groupid"},
                    },
                },
            },
            "/v0/groups/{groupid}/close": {
                "post": {
                    "summary": "Fix the expected count of an open-ended group",
                    "operationId": "closeGroup",
                    "parameters": [{"in": "path", "name": "groupid", "required": True,
                                    "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "OK"}},
                },
            },
            "/v0/groups/{groupid}/cancel": {
                "post": {
                    "summary": "Stop submitting queued members",
                    "description": (
                        "Queued members are not submitted. Agents ALREADY RUNNING are "
                        "not interrupted — that needs a mechanism EventRunner honours "
                        "and belongs with the Job-per-request work (§21.9.5)."
                    ),
                    "operationId": "cancelGroup",
                    "parameters": [{"in": "path", "name": "groupid", "required": True,
                                    "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "OK"}},
                },
            },
            "/v0/agents/{correlationid}/transcript": {
                "put": {
                    "summary": "Checkpoint the claude session transcript for this correlation",
                    "description": (
                        "Called by EventRunner after each turn. The runner is stateless and "
                        "its $HOME is a pod's ephemeral layer, so without this a /continue "
                        "issued after the agent scaled to zero has no transcript to resume "
                        "from. EventBridge is the single-replica component with a volume, so "
                        "it is where per-correlation session state lives. "
                        "See DESIGN_PHASE1.md §16 Gap B."
                    ),
                    "operationId": "putTranscript",
                    "parameters": [{"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}}],
                    "requestBody": {"required": True, "content": {"application/x-ndjson": {"schema": {"type": "string"}}}},
                    "responses": {
                        "200": {"description": "Stored", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/TranscriptMeta"}}}},
                        "400": {"description": "empty body or bad correlationid"},
                        "404": {"description": "unknown correlationid"},
                        "413": {"description": "over EB_TRANSCRIPT_MAX_BYTES"},
                    },
                },
                "get": {
                    "summary": "Fetch the checkpointed transcript, verbatim",
                    "description": (
                        "EventRunner writes the body to a local scratch path and passes THAT "
                        "path to `claude --resume`, which accepts a transcript path as well as "
                        "a session id."
                    ),
                    "operationId": "getTranscript",
                    "parameters": [{"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}}],
                    "responses": {
                        "200": {"description": "The transcript", "content": {"application/x-ndjson": {}}},
                        "404": {"description": "no transcript checkpointed"},
                    },
                },
            },
            "/v0/agents/{correlationid}/events": {
                "get": {
                    "summary": "JSON events with optional since=<sequence>",
                    "operationId": "getAgentEvents",
                    "parameters": [
                        {"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}},
                        {"in": "query", "name": "since", "required": False, "schema": {"type": "integer", "default": 0}},
                    ],
                    "responses": {"200": {"description": "OK", "content": {"application/json": {}}}},
                }
            },
            "/v0/agents/{correlationid}/events.jsonl": {
                "get": {
                    "summary": "Full CloudEvent JSON dump",
                    "operationId": "getAgentEventsJsonl",
                    "parameters": [{"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "OK", "content": {"application/x-ndjson": {}}}},
                }
            },
            "/v0/agents/{correlationid}/events.sse": {
                "get": {
                    "summary": "Server-Sent Events stream of new response events",
                    "operationId": "getAgentEventsSse",
                    "parameters": [{"in": "path", "name": "correlationid", "required": True, "schema": {"type": "string"}}],
                    "responses": {"200": {"description": "OK", "content": {"text/event-stream": {}}}},
                }
            },
            "/openapi.json": {"get": {"summary": "OpenAPI spec", "responses": {"200": {"description": "OK"}}}},
            "/docs": {"get": {"summary": "Swagger UI", "responses": {"200": {"description": "OK"}}}},
            "/healthz": {"get": {"summary": "Liveness", "responses": {"200": {"description": "OK"}}}},
        },
        "components": {
            "schemas": {
                "CreateGroupRequest": {
                    "type": "object",
                    "properties": {
                        "label":    {"type": "string"},
                        "prompts":  {"type": "array", "items": {"type": "string"},
                                     "description": "one agent per prompt, all submitted together"},
                        "expected": {"type": "integer",
                                     "description": "for a group populated by later POST /v0/agents calls"},
                        "min_success": {"type": "integer",
                                        "description": "complete as soon as this many members succeed"},
                        "deadline_s": {"type": "number",
                                       "description": "straggler cutoff; without one a short group never completes and never notifies"},
                        "max_turns": {"type": "integer", "default": 3},
                        "model":     {"type": "string"},
                    },
                },
                "GroupStatus": {
                    "type": "object",
                    "properties": {
                        "groupid":     {"type": "string"},
                        "label":       {"type": "string"},
                        "state":       {"type": "string",
                                        "enum": ["running", "all", "quorum", "deadline", "cancelled"]},
                        "expected":    {"type": "integer"},
                        "denominator": {"type": "integer",
                                        "description": "max(expected, members seen) — never shrinks"},
                        "denominator_revised": {"type": "boolean"},
                        "finished":    {"type": "integer"},
                        "failed":      {"type": "integer"},
                        "running":     {"type": "integer"},
                        "queued":      {"type": "integer"},
                        "terminal":    {"type": "integer"},
                        "percent":     {"type": "number", "nullable": True},
                        "elapsed":     {"type": "string"},
                        "eta":         {"type": "string", "nullable": True,
                                        "description": "rounded, and withheld until enough members have finished to be honest"},
                        "throughput_per_min": {"type": "number", "nullable": True},
                        "stall_note":  {"type": "string", "nullable": True},
                        "completion_reason": {"type": "string", "nullable": True},
                    },
                },
                "TranscriptMeta": {
                    "type": "object",
                    "properties": {
                        "correlationid": {"type": "string"},
                        "size":          {"type": "integer"},
                        "sha256":        {"type": "string"},
                        "checkpoints":   {"type": "integer",
                                          "description": "how many turns have written this transcript"},
                        "updated_utc":   {"type": "string"},
                    },
                },
                "StartRequest": {
                    "type": "object", "required": ["prompt"],
                    "properties": {
                        "prompt":    {"type": "string"},
                        "model":     {"type": "string"},
                        "max_turns": {"type": "integer", "default": 3},
                        "notify":    {"type": "boolean", "default": True},
                    },
                },
                "StartResponse": {
                    "type": "object", "required": ["correlationid", "sessionuuid"],
                    "properties": {
                        "correlationid": {"type": "string", "pattern": "^[a-z]{3,10}-[a-z]{3,12}-\\d{4}$"},
                        "sessionuuid":   {"type": "string", "format": "uuid"},
                        "event_id":      {"type": "string"},
                        "topic":         {"type": "string"},
                        "html_url":      {"type": "string"},
                    },
                },
                "ContinueRequest": {
                    "type": "object", "required": ["prompt"],
                    "properties": {"prompt": {"type": "string"}},
                },
            }
        },
    }


SWAGGER_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>rossoctl-keda1 EventBridge — API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head><body><div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url:"/openapi.json",dom_id:"#swagger-ui"});</script>
</body></html>
"""
