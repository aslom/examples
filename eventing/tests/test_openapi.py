"""OpenAPI 3.1 spec basic shape checks."""
from eventbridge.openapi import spec


def test_openapi_version_is_3_1():
    s = spec()
    assert s["openapi"].startswith("3.1")


def test_required_paths_present():
    paths = spec()["paths"].keys()
    for p in [
        "/v0/agents",
        "/v0/agents/{correlationid}/continue",
        "/v0/agents/{correlationid}",
        "/v0/agents/{correlationid}/events",
        "/v0/agents/{correlationid}/events.jsonl",
        "/v0/agents/{correlationid}/events.sse",
        "/openapi.json",
        "/docs",
        "/healthz",
    ]:
        assert p in paths, f"missing path: {p}"


def test_start_response_schema_has_correlationid_regex():
    s = spec()
    start = s["components"]["schemas"]["StartResponse"]
    assert start["properties"]["correlationid"]["pattern"]
