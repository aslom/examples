"""Bearer-token identity for the submit path. Stdlib only.

Scope is deliberately narrow: this answers "who is asking me to run an agent?"
on the two routes that CREATE work. It is not a general authorization layer.

Two properties worth stating, because both are easy to get wrong:

* **Constant-time comparison.** Tokens are compared with
  `hmac.compare_digest`, not `==`. A short-circuiting compare leaks the shared
  prefix length through timing, which over enough requests recovers the token
  one byte at a time.
* **Identity is a name, not a boolean.** The config maps a name to each token,
  so a validated request yields `"alice"` rather than `True`. That name rides
  onto the request event as `ce_submitter`, which is the whole point — a `401`
  tells you nothing after the fact, a recorded submitter does.

What this is NOT: the submitter attribute is **unsigned**. Anyone who can write
to the Kafka `requests` topic can forge it, and the broker is plaintext. The
honest claim is "EventBridge refuses unauthenticated submissions and records who
it believes submitted this" — not "this event proves who submitted it." Proving
it needs `submitter` inside `signing.SIGNED_ATTRS` and a producer that actually
signs (today nothing does; see agentdocs/IMPLEMENTATION_REPORT1.md §811-814).
"""
from __future__ import annotations

import hmac
from typing import Any

# Returned verbatim as the `WWW-Authenticate` value on a 401 so a client knows
# which scheme to retry with.
CHALLENGE = 'Bearer realm="eventbridge"'


def parse_tokens(raw: str) -> dict[str, str]:
    """`"alice:tok1,bob:tok2"` -> `{"tok1": "alice", "tok2": "bob"}`.

    Keyed by token because lookup goes token -> name. Malformed entries (no
    colon, empty name, empty token) are skipped rather than raising: a typo in
    one entry must not take the whole service down at startup, and a skipped
    entry fails closed — that token simply does not authenticate.
    """
    out: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        name, _, token = entry.partition(":")
        name, token = name.strip(), token.strip()
        if name and token:
            out[token] = name
    return out


def resolve_identity(environ: dict[str, Any], tokens: dict[str, str]) -> tuple[str | None, str | None]:
    """Resolve the caller from a WSGI environ.

    Returns `(identity, error)`:

    * `(None, None)`   — auth is disabled (no tokens configured). Callers treat
                         this as "allowed, anonymous", which keeps the default
                         demo path working unchanged.
    * `(name, None)`   — a valid credential for `name`.
    * `(None, reason)` — reject with 401; `reason` is safe to return to the
                         client (it never echoes the presented token).

    Reads only headers. It must not touch `wsgi.input`: `handlers._read_json`
    reads the body from a non-seekable stream, so consuming it here would leave
    every downstream handler with an empty body.
    """
    if not tokens:
        return None, None

    header = environ.get("HTTP_AUTHORIZATION") or ""
    if not header:
        return None, "authentication required"

    scheme, _, presented = header.partition(" ")
    if scheme.lower() != "bearer":
        return None, "unsupported authentication scheme; expected Bearer"
    presented = presented.strip()
    if not presented:
        return None, "empty bearer token"

    # Compare against every configured token so the work done is independent of
    # which entry matches (and of whether any does). `compare_digest` on str
    # requires ASCII, so a non-ASCII token is rejected rather than raising.
    try:
        matched = None
        for token, name in tokens.items():
            if hmac.compare_digest(token, presented):
                matched = name
    except TypeError:
        return None, "malformed bearer token"

    if matched is None:
        return None, "invalid bearer token"
    return matched, None
