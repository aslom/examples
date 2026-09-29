# DESIGN — Phase 2: identity on the event path

Status: draft (revision 1)
Scope: **delta over `DESIGN_PHASE1.md`.** Read that first. This document records
only what changes when the demo stops trusting whoever can reach it.

Phase 0 proved the wire shape. Phase 1 decided where the consumer runs and how
many of them there are. Phase 2 answers two questions neither phase asked:

1. **Which person asked for this work?** — and is that person allowed to ask?
2. **Which agent produced this answer?** — and is that agent one we approved?

```text
   👤 ──sign in──▶ 🌐 GitHub
   │                    │ GET /user -> login
   │  Bearer <token>    ▼
   └──────────▶ ╔═══════════════════╗
                ║ 🔒 EventBridge     ║  401 no credential
                ║    the PEP        ║  403 known, not approved
                ╚═════════╤═════════╝
                          │ ce_submitter, ce_submitteriss
                          ▼
                   Kafka:requests ─▶ EventRunner ─▶ Kafka:responses
```

The single new moving part in this half is GitHub: EventBridge verifies a
sign-in once at the edge, then records *who* on the request event. The second
half — proving which agent answered — has its groundwork in place (§4) but is
not yet wired, and this document says so plainly rather than describing it as
done.

---

## 1. What does NOT change

Stated explicitly, because the temptation in a security document is to redesign
things that already work:

- **The CloudEvent contract** (Phase 0 §2). Two new extension attributes are
  *added* (`submitter`, `submitteriss`); nothing existing changes shape.
  `ce.new_event(**attrs)` already accepts arbitrary attributes and
  `to_kafka_binary` already emits every non-empty one as a `ce_*` header, so the
  codec needed no change at all.
- **Kafka message key = `correlationid`**, the per-correlation FIFO router, the
  `uuid5` session derivation, `--session-id` / `--resume` mechanics.
- **KEDA scaling on consumer lag** and scale-to-zero. Identity is checked at the
  HTTP edge, so it never touches the scaling path.
- **Pure-Python discipline** (Phase 1 §1.1). No new runtime dependency:
  `urllib`, `hmac`, `hashlib`, `json`. In particular **no JWT library**, because
  there is no JWT to verify — see §2.1.
- **Auth is off by default.** With no client id, no approved-user list and no
  static tokens configured, `resolve()` returns "allowed, anonymous" and the
  Phase 0/1 demo behaves exactly as before. Every existing test passes
  untouched; that is the check that this is additive.

---

## 2. User identity

### 2.1 The constraint that shapes everything

**GitHub does not issue a verifiable token for user login.** The OAuth device
flow returns an *opaque* access token: no signature, no claims, nothing to check
offline. The JWKS at `token.actions.githubusercontent.com` is for Actions
workloads, not users, and there is no user-facing equivalent.

This is the fact that rules out the obvious design. We cannot validate a token
locally the way AuthBridge validates a Keycloak JWT. EventBridge must ask GitHub
who holds the token, via `GET https://api.github.com/user`.

Three consequences, all accepted deliberately:

| Consequence | Why it is acceptable, and what it costs |
|---|---|
| Sign-in depends on GitHub being reachable | A failed lookup is a `401`, never an allow. Failing closed is the only safe direction for "who is this". A GitHub outage means nobody can submit — correct, and an operator can keep a static break-glass token (§2.5). |
| A lookup per request, against a 5000/hour budget | The cache (§2.3) is therefore **load-bearing, not an optimisation**. |
| GitHub's latency joins the request path | Same answer: the cache. A cache hit costs nothing. |

### 2.2 Why the device flow

The alternative — an OAuth web flow with a redirect — needs EventBridge to host
a callback endpoint at a URL GitHub can reach. The demo runs on a laptop behind
a VPN (Phase 1 §"Remote access caveat"), so that is precisely what it cannot do.

The device flow inverts it: the CLI asks GitHub for a code, the user types the
code into a page GitHub already hosts, and the CLI polls. Nothing needs to reach
*us*. It also needs **no client secret**, which is why the client id is a
committed default rather than a capability like an ntfy topic.

`login` prints the code and blocks rather than opening a browser. Auto-opening
fails silently over SSH and inside a container, which is where this is most
often run.

**Scopes requested: none.** Verified against the live API — `GET /user` answers
with `x-accepted-oauth-scopes:` empty, so an unscoped token reads the login. The
demo asks for the least access that answers its question, and cannot read a
repository even if the token leaks.

GitHub's pacing contract is honoured exactly: `authorization_pending` means keep
polling, `slow_down` means keep polling and add five seconds. Polling faster
than asked rate-limits the whole OAuth App, which would break sign-in for
everyone rather than just the impatient caller.

### 2.3 The cache

Token → login, TTL 300 s by default, keyed by **`sha256(token)`**.

The hash is not decoration. A cache keyed by the raw token means a memory dump,
a careless `repr`, or a debug log yields a working credential. Keyed by hash, it
yields nothing.

**Failures are not cached.** Two reasons, and they pull the same way: a revoked
token must stop working promptly, and a GitHub outage must not pin a legitimate
user to a failure for the whole TTL.

### 2.4 `401` and `403` are different answers

This is the design decision most worth defending, because collapsing them is
the easy thing to do.

| Status | Meaning | Carries `WWW-Authenticate`? |
|---|---|---|
| `401` | "I do not know you." No credential, a malformed one, or one GitHub does not recognise. | Yes — retrying with a credential is the remedy. |
| `403` | "I know exactly who you are, and you are not approved." | **No** — retrying with another credential is *not* the remedy. |

The `403` body names the login that was refused (`mrsabath is not on the
approved-user list`). A user who is told only "forbidden" goes looking for a
broken token; a user told *which identity* was refused knows to ask an operator.
That is the difference between an actionable error and a support ticket.

An empty approved-user list **denies everyone**. The other reading — empty means
everybody — would turn a missing environment variable into an open door, which
is exactly the class of failure a security feature must not have.

Logins compare case-insensitively, because GitHub logins are. Comparing exactly
would refuse a genuinely approved user over capitalisation, which reads as a
broken deployment rather than a policy.

### 2.5 Static tokens remain, on purpose

`EB_AUTH_TOKENS` is not deprecated. It serves three things GitHub sign-in cannot:

- **Tests must not reach the network.** A suite that calls GitHub is slow,
  flaky, rate-limited, and fails on a machine without credentials.
- **An offline demo has to stay possible.** Conference wifi is not a dependency
  worth accepting.
- **A break-glass credential.** When GitHub is unreachable, an operator with a
  static token can still drive the system. `resolve()` checks it before giving
  up for exactly this reason.

### 2.6 Identity on the event, and what it is worth

Two attributes ride the request:

```text
ce_submitter:    mrsabath
ce_submitteriss: github
```

`submitteriss` exists because without it a reader cannot tell a verified GitHub
login from a name an operator typed into an environment variable. Absent issuer
means "static token" — the weaker claim, visible as such.

**On the spelling.** CloudEvents v1.0 requires attribute names to be lower-case
`[a-z0-9]` only — no underscore, hyphen or upper case — because an event crosses
several hops and protocols disagree about metadata case-sensitivity. This first
shipped as `submitter_iss` and was caught in review, not by the code: the codec
here only adds and strips the `ce_` prefix, so a non-compliant name round-trips
locally and is rejected or silently dropped by a spec-compliant SDK, an
HTTP-binding gateway or a Knative broker further along. `test_roundtrip_binary.py`
now asserts the rule over every `EXT_*` constant, so the next extension cannot
repeat it.

**Both are unsigned.** Anything with write access to the `requests` topic can
forge them, and the broker is plaintext. The honest claim after this phase is:

> A real GitHub user, on an approved list, authorised this request — as recorded
> by EventBridge.

**Not** "the event proves who submitted it." Making it provable needs `submitter`
inside `signing.SIGNED_ATTRS` *and* a producer that signs. See §4.

### 2.7 Where the check lives, and the trust that follows

EventBridge is the **policy enforcement point**. It verifies once, then attests
by recording. EventRunner never contacts GitHub.

That means **compromising EventBridge means being able to claim any user.** This
is standard PEP design, and the alternative is worse: passing the user's GitHub
token through to every runner would spread a live credential across every
workload and make each one a lookup client. Stated here so it is a documented
property rather than a discovery during questions.

---

## 3. What is deliberately left open

### 3.1 `/continue` is unauthenticated

`ntfy.py` emits an ntfy `http` action so a notification has a **Continue…**
button. That action is a recipe serialised into the notification, so any
credential it carries comes to rest in four places outside our control: the
payload sent to `ntfy.sh`, ntfy's message store, the phone's notification
history, and every other subscriber of the topic.

A long-lived bearer token must not go there. So `/continue` stays open, and the
reasoning is not a shrug: continuing requires already knowing an unguessable
correlationid, which is a **capability URL** — the same model the HTML transcript
already relies on. Creating *new* work is the privileged act.

**Planned fix**: derive `key = HMAC(server_secret, correlationid + exp)`, bake
`?k=<key>` into the action URL, and accept either a bearer token or a valid key.
A leaked notification then grants one conversation, with an expiry, instead of
the API. Stdlib `hmac`, nothing stored.

### 3.2 `PUT /transcript` is unauthenticated

EventRunner uses it for session checkpointing and has no credential concept
anywhere in `eventrunner/config.py`. Gating it breaks `/continue` after a cold
pod — the Phase 1 §16 Gap B path. Fixing it properly means giving EventRunner an
identity, which is §4's territory.

### 3.3 Not addressed at all

- **Kafka is plaintext**, with no transport authentication. SASL and ACLs were
  evaluated and rejected for this demo: the Kafka authorizer is global rather
  than per-listener, so enabling it either denies every existing PLAINTEXT client
  or, with `allow.everyone.if.no.acl.found=true`, makes the ACL demo vacuous.
  More importantly they authenticate the *connection*, not the payload — they
  cannot tell a real EventRunner from anything else holding valid credentials.
- **No rate limiting.** Refusing an invalid request is cheap but not free.
- **The approved lists are files.** They record what an operator approved, not
  what a platform attested.

---

## 4. Agent identity: groundwork, not yet wired

The second question — *which agent produced this answer?* — matters more than it
first appears. Today anything with write access to the `responses` topic gets its
output stored, rendered in the HTML transcript, and pushed to the operator's
phone **as a legitimate agent answer**.

### 4.1 What exists

- `signing.sign_event(event, seed, kid=None)` writes a key id into the JWS
  protected header. Because the header is part of the signed input, **the `kid`
  cannot be swapped** to relabel an event as coming from another agent.
  Omitting it is byte-identical to before, so this was additive — no
  canonicalisation break and no signature migration.
- `signing.token_kid(token)` reads the `kid` *before* verification, to choose a
  key. It is a hint until verification succeeds with the key it named.
- `shared/keyset.py` maps `kid` → Ed25519 public key from a JSON file. **That
  file is the authorization list**: an unknown or absent `kid` has no key and the
  event is refused.

`select(None)` returns a key only when exactly one is approved. With several,
an unnamed token is ambiguous, and guessing would mean accepting a signature
from *any* approved agent for an event that named none of them.

### 4.2 The blocker, stated plainly

**`sign_event()` has no production caller.** `ER_REQUIRE_SIGNATURE=true` today
rejects 100% of traffic — it is a kill switch, not a feature.
`IMPLEMENTATION_REPORT1.md` §811-814 already says "EventBridge does not sign."
Nothing about signed attributes means anything until that is fixed, which is why
`submitter` is **not** in `SIGNED_ATTRS` yet: adding it before the signing side
exists would only invalidate canonicalisation twice.

### 4.3 Why not Keycloak, and why not HMAC

**Keycloak client credentials** prove an agent *holds a secret* — which a
compromised pod also holds. It adds a token-issuing dependency while proving the
least of the available options.

**HMAC** is symmetric: EventBridge would hold the key it verifies with, so it
could forge any agent's response and any agent could forge another's. That fails
the goal as stated. It is ~170,000× faster than the hand-rolled Ed25519
(0.0013 ms/op against ~150 ms), which is tempting and still wrong here.

**Ed25519** proves possession of a private key that never leaves the runner, and
upgrades cleanly: SPIRE later distributes the same keys rooted in workload
attestation, and the verification code does not change — only where keys come
from.

### 4.4 The remaining work

1. Sign requests in `kafka_out.publish_request`, after `ce.new_event` (which
   fills `id`/`time`, both signed) and before `to_kafka_binary`.
2. Sign responses in `emit.py` between event construction and serialisation.
   Hold the seed **on the Emitter** — its constructor runs once, so none of the
   seven `emit()` call sites change. Sign **terminal events only**: `emit()` is
   on the hot path for every `stdout` frame and Ed25519 costs ~150 ms here.
3. Verify responses in `kafka_in.py` while the `CloudEvent` is still in hand.
   Two hazards: the decode and store write are **not** inside a `try`, so a raise
   kills the consumer thread — the verification path must degrade, never raise.
   And group events are published by EventBridge itself, carry `groupid` but no
   `correlationid`, so they need either their own `kid` or a skip.
4. Then add `submitter`, `submitteriss` and the missing `groupid` to
   `SIGNED_ATTRS`. `DESIGN_PHASE1.md` §21.9.9 requires `groupid`; its absence
   means signatures currently say nothing about batch membership.

On failure, set `phase="error"`. That reuses machinery already wired: ntfy
**priority 5** with an error tag, a visually distinct bubble in the live SSE
transcript, and `raw_json` persistence for audit. The demo artifact costs
nothing extra.

---

## 5. Configuration

| Variable | Default | Effect |
|---|---|---|
| `EB_GITHUB_CLIENT_ID` | from `config.toml` | OAuth App client id. **Public** — the device flow has no client secret. |
| `EB_ALLOWED_USERS` | empty | Comma-separated approved logins. Empty denies everyone. |
| `EB_GITHUB_CACHE_TTL_S` | `300` | Token → login cache lifetime. |
| `EB_AUTH_TOKENS` | empty | `name:token,...` fallback. Empty plus no GitHub config means auth is off. |
| `EVENTBRIDGE_TOKEN` | unset | CLI: overrides the stored token, so a shell can act as another identity. |

The client id lives in `config.toml` because it is not a capability. `test_manifests.py`
pins the opposite rule for `NTFY_TOPIC`/`NTFY_TOKEN`, and that distinction is the
point: one is public by construction, the others grant access.

---

## 6. Verification

**Tests: 490 passed, 5 skipped.** Baseline on the same tree is 450/5, so this
adds 40 and regresses nothing. No test reaches the network — the device flow and
`GET /user` are exercised through injected fakes.

Verified end to end against a real GitHub account, not only in unit tests:

| Check | Result |
|---|---|
| `login` browser flow | `✔ signed in as mrsabath`, token stored `0600` |
| No credential | `401` + `WWW-Authenticate` |
| Unknown token | `401` |
| Real user, not on the list | `403` — `mrsabath is not on the approved-user list` |
| Approved user | `202`, agent ran, `final=True` |
| On the wire | `ce_submitter:mrsabath`, `ce_submitteriss:github` |
| Group of 5 | every member carried both attributes |

### 6.1 One environment finding worth recording

During the group test the completion counter read 1/5 while all five members had
run. Cause: several EventBridge instances were running against one Kafka, and the
**response consumer uses a fixed shared group** (`eventbridge-responses`) while
the requests mirror uses a per-PID one. The instances therefore split the twelve
response partitions between them and no single one saw every response.

Not a defect in this change, and not a bug in Phase 1 either — a single deployed
EventBridge is the intended topology, and Phase 1 lists multi-replica EventBridge
as deferred. But it is a real trap for anyone running two copies on a laptop:
**symptoms look like lost responses, not like a split consumer group.** Worth
knowing before a demo.

---

## 7. Reading order for whoever picks this up

- **Running it?** `README_PHASE1.md`, then `EB_GITHUB_CLIENT_ID` and
  `EB_ALLOWED_USERS` from §5.
- **Changing the identity model?** §2.1 first — the opaque-token constraint is
  what rules out the design most people reach for.
- **Finishing agent identity?** §4.2 for the blocker, then §4.4 in order.
- **Presenting it?** §2.6 and §3 — what the controls do *not* prove. A security
  demo that oversells its guarantee is worse than one that does not exist.
