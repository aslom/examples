---
name: eventbridge
description: |
  Run an autonomous Claude agent through the local EventBridge (Kafka + CloudEvents),
  stream the reply, and resume prior conversations by correlationID. Use this when the
  user asks to "run an agent to X", "ask an agent to X", "print the response", "continue
  <corr>", or "show the conversation for <corr>". Talks HTTP to one EventBridge endpoint
  taken from --base-url, else $EVENTBRIDGE_URL, else localhost — never run kubectl or
  discover an endpoint.
---

## When to invoke

Match the user's phrasing to one of these commands. Different surface forms all
map to the same intent — pick the right verb, not a literal string match.

| The user says… | Run | Notes |
|---|---|---|
| "run an agent to X" · "run agent to X and print the response" · "ask an agent to X" | `run "X"` | Default output prints the model's reply inline. |
| "run agent to X quietly" · "just start it, I'll check later" | `run "X" --no-watch` | Returns as soon as the correlationid is minted. |
| "show every step" · "verbose" · "with tool calls" | `run "X" --verbose` | One labeled line per stream-json event. |
| "check on <corr>" · "what happened with <corr>" · "any progress on <corr>" | `watch <corr>` | Polls until the correlation reaches `final=true`. |
| "continue <corr>: Y" · "follow up on <corr> with Y" · "and now Y" (after a recent run) | `cont <corr> "Y"` | Resumes the same claude session. |
| "chat <corr>" · "show the conversation for <corr>" · "print the transcript" | `chat <corr>` | Turn-grouped USER/ASSISTANT view. |
| "run agent1 to X and agent2 to Y" · "run two agents" · "start these in parallel" | `runmany "X" "Y"` | Submits ALL prompts first, then follows them all. This is what makes KEDA scale past one pod. |
| "run a batch of N agents, each …" · "run N agents as a group" · "100 agents saying Hello N" | `group run --template "…{n}…" --count N` | A **batch**: one start + one finish notification instead of N, a shared progress page, and watching is the default. Say "batch"/"group"/a count ≥ ~5 and this is the intent. |

**Two or more agents at once** — use `runmany`, never two separate `run` calls:

    python3 .../eventbridge-cli.py runmany "Say hello" "Say world" \
      --label agent1 --label agent2

How the words in the request map to the command — follow this literally, so the
same request always produces the same command:

**N agents from a pattern** ("run 100 agents, each saying Hello N") — use
`--template`/`--count`, never 100 positional arguments:

    python3 .../eventbridge-cli.py group run \
      --template "Say Hello {n}" --count 100 --label hello-100

`{n}` is 1-based (shift it with `--start`), `{i}` is 0-based. This is the reliable form:
100 shell-quoted arguments is easy to get wrong and hard to read back, and the CLI echoes
the first and last generated prompt so the pattern is visible before anything runs.

With `--watch` (the default) the command then prints a progress line every 2s until the
batch finishes, so the monitoring happens right in the transcript. It also prints the two
URLs to open — the group page and the group list.

| In the request | Becomes |
|---|---|
| each task, in the order given | one positional `PROMPT`, quoted |
| the agents are **given names** ("agent1 … agent2", "researcher … writer") | `--label <name>` once per agent, in the same order |
| the agents are not named | omit `--label`; they default to `agent1`, `agent2`, … |
| an environment or URL is named ("on ykt1", "on kind") | `--base-url` — resolve it per the table below |
| a shared constraint ("one word each", "be brief") | append it to **every** prompt |
| a numbered pattern ("each saying Hello N", "for N=1..100") | `--template "…{n}…" --count N`, not N positional prompts |
| "monitor it" · "show progress" · "print updates" | nothing — `group run` watches by default and prints a line every 2s. `--interval` changes the cadence |
| "quietly" · "don't wait" | `--no-watch` |

So *"run agent1 to say hello and agent2 to say world, one word each"* becomes just:

    python3 .../eventbridge-cli.py runmany \
      "Say hello. One word." "Say world. One word." \
      --label agent1 --label agent2

with no `--base-url` at all when `$EVENTBRIDGE_URL` is exported — which is the usual
case. Add `--base-url <url>` only when the user names a target for this one command.

The ordering is the whole point. `runmany` POSTs every prompt *before* it watches
anything, so N requests sit on the topic together, KEDA sees lag N and scales to N
pods. Two sequential `run` calls would each finish before the next was submitted —
lag would never exceed 1, one pod would serve both, and nothing would scale.
Verified on a cluster: two prompts took the Deployment `0 → 1 → 2` with two pods
Running and two notifications delivered.

**When the user just says "run agent to \<X> and print/show the response"** — the
default `run` command already does exactly that. Don't add flags.

## Always show the command

**Show the exact command line, never a paraphrase of it.** The CLI echoes its own
invocation as its first line of output, shell-quoted and ready to paste:

```
$ python3 skills/eventbridge/eventbridge-cli.py group run --template 'Say Hello {n}' --count 100 --label hello-100 --base-url https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com
· endpoint https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com
```

So the only rule is: **do not hide it.** Don't pipe through `tail`/`grep` in a way
that drops the first line, and don't summarise the command in prose instead of
letting the line through — "I started 100 agents" cannot be checked, and it reads
identically whether `--count` was 100 or 10. When you do describe what you ran,
quote the echoed line rather than rewriting it.

The echo is on by default. `EVENTBRIDGE_ECHO=0` silences it, for the rare case where
the same command runs in a tight loop and the repetition is pure noise. Any
`user:pass@` in a URL argument is replaced with `***@` before printing.

## Run it so the output is actually visible

A tool call shows its output **when the command exits**, not while it runs. So a single
blocking command that watches agents for two minutes shows nothing for two minutes and
then everything at once — and if it hits the call's timeout first, it is killed and the
progress is never reported at all. Watching in one long call is therefore the wrong
shape here, however natural it looks.

**Submit, then poll. Never one long blocking call.**

Step 1 — submit with `--no-watch`. This returns in about **2 seconds** with every
correlationid and URL, so the user immediately has something to click:

```
python3 .../eventbridge-cli.py runmany "Say hello." "Say world." "Say again." \
  --label agent1 --label agent2 --label agent3 --no-watch
```
```
· endpoint https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com (from $EVENTBRIDGE_URL)
▸ agent1 sweet-crab-0956 · https://…/v0/agents/sweet-crab-0956
▸ agent2 lazy-seal-6061  · https://…/v0/agents/lazy-seal-6061
▸ agent3 stale-mole-7331 · https://…/v0/agents/stale-mole-7331
· 3 agent(s) submitted together — KEDA should scale to 3 pod(s)
```

Step 2 — check each one in its own short call, with a bounded `--timeout`:

```
python3 .../eventbridge-cli.py watch sweet-crab-0956 --timeout 30
```
```
ASSISTANT: MOCK-REPLY[Say hello. One word.]
✔ done · 2.6s · 20→32 tokens
```

Each check returns in seconds, so the user sees agent 1's reply while agents 2 and 3
are still working, instead of a blank three-minute wait. Report each result as it
arrives. If a `watch` times out it says so and exits cleanly — run it again; the
correlationid stays valid, nothing is lost, and a cold KEDA pod needs ~15 s to start.

For a **group**, `group status <gid>` is the one-shot equivalent — one line of counts,
no waiting. Call it every few seconds rather than using `group watch`, which blocks by
design. (`group run` without `--no-watch` blocks too; it is meant for a human at a
terminal who can watch it live, and for a batch of 100 it is reasonable to let it run —
but then say up front that it will report nothing until it finishes.)

**Do not append `2>&1`.** Errors already reach the user on stderr, and merging the
streams only throws away which one a line came from — the difference between a progress
line and `✖ cannot reach EventBridge` is worth keeping. It also does nothing whatsoever
for latency, which is the problem it tends to be reached for.

Don't pipe through `tail`, `head` or `grep` either: the echoed command, the endpoint
line and the reply are each a few lines total, and filtering them is how the target
being wrong becomes invisible.

## Output styling (compact by default)

Default (no flags):
```
$ python3 skills/eventbridge/eventbridge-cli.py run 'Say hi'
▸ agent brave-otter-4718 · http://127.0.0.1:8080/v0/agents/brave-otter-4718
ASSISTANT: Hi! What can I help you with?
✔ done · 6.6s · $0.0470 · 812→24 tokens
```

That's the echoed command, then three lines: the correlationid + a click-safe URL
(no wrapping `()`, so a terminal auto-link opens the correct URL), the model's
reply, and a one-line usage summary. `system/init` frames and echoed-final frames are
suppressed because they add no information to the user.

`--verbose` shows every event with a short label — useful when the agent uses
tools or takes multiple turns:
```
[01 21:29:22 system   ] init: model=claude-opus-5 tools=27
[02 21:29:22 assistant] Hi! What can I help you with?
[03 21:29:22 final    ] ★  ✔ done · 6.6s · $0.0470
```

`--raw` dumps each event's JSON payload — debug only.

## Endpoints (see OPENAPI.md for full schema)

- `POST /v0/agents`                          — start an agent
- `POST /v0/agents/{corr}/continue`          — resume same claude session
- `GET  /v0/agents/{corr}`                   — HTML turn-grouped view
- `GET  /v0/agents/{corr}/turns`             — JSON turn-grouped view
- `GET  /v0/agents/{corr}/events?since=N`    — flat JSON event list

## Runtime

The CLI is **stdlib-only** — no third-party deps. Invoke it with plain
`python3`; `uv run python` also works when the project venv is available:

    python3 .claude/skills/eventbridge/eventbridge-cli.py <cmd> ...

Prefer `python3` because it survives restricted environments where
`uv run` refuses to canonicalize the venv path.

## Setup before running the skill

**Nothing.** The CLI is stdlib-only — no venv, no pip install, no `kubectl`, no
cluster access. It speaks plain HTTP to one EventBridge endpoint.

| | Needed? | |
|---|---|---|
| An endpoint | only if it is not already in `$EVENTBRIDGE_URL` | See below. There is nothing to discover. |
| `kubectl` / a kubeconfig | **no** | Never run it. The CLI does not use it and neither should you. |
| `NTFY_TOPIC` | **no** | Phone notifications are a property of the *deployment*, not of this client. If EventBridge was deployed with an ntfy Secret it notifies; nothing is needed here. |
| A credential | **no** | Real vs mock is decided by the EventRunner deployment. Against the `test`/`kind` overlays every reply is `MOCK-REPLY[...]`, which is expected. |
| Python venv | **no** | Plain `python3`. |

## Which EventBridge to talk to

The CLI resolves this itself, from exactly three sources, in this order:

1. `--base-url <url-or-hostname>` — one command
2. **`$EVENTBRIDGE_URL`** — the whole session, and the normal case
3. `http://127.0.0.1:8080` — the laptop default

**Never discover the endpoint.** Do not run `kubectl get route`, do not look up a
Service, do not probe or curl `/healthz` to find out whether something is listening,
and do not ask which cluster is meant when `$EVENTBRIDGE_URL` is already set. Just
run the command — the CLI prints which endpoint it used and why, as its second line:

```
· endpoint https://eventbridge-kev1.apps.ykt1.hcp.res.ibm.com (from $EVENTBRIDGE_URL)
· endpoint http://127.0.0.1:8080 (from default)
```

So when the user names no environment, **that is not a reason to investigate** — it
means source 2 or 3, whichever applies, and the printed line says which. A bare
hostname works in either the flag or the variable (`https://` is assumed), so a Route
host can be pasted straight in:

    export EVENTBRIDGE_URL=eventbridge-kev1.apps.ykt1.hcp.res.ibm.com
    python3 .../eventbridge-cli.py run "Say hi"          # no flag needed

Pass `--base-url` only when the user names a *different* target than the exported one
for this one command:

| The user says… | Do |
|---|---|
| nothing about where | nothing — run the command, let `$EVENTBRIDGE_URL` or the default apply |
| a URL or hostname outright | `--base-url <it>`, verbatim |
| "locally" · "on my laptop" | `--base-url http://127.0.0.1:8080` |
| "on kind" | `--base-url http://eventbridge.127.0.0.1.nip.io:30080` |
| "on ykt1" · a named cluster you have no URL for | ask for the URL, or use `$EVENTBRIDGE_URL` if it is set — do **not** shell out to find it |

If the endpoint is wrong or down, the CLI says so and exits 2 with the fix, so there
is no reason to pre-check:

```
✖ cannot reach EventBridge at http://127.0.0.1:8080 ([Errno 61] Connection refused)
  endpoint came from: default
  No endpoint was given, so this used the laptop default. Point it at the
  right EventBridge — nothing here discovers one for you:
      export EVENTBRIDGE_URL=https://<eventbridge-host>   # whole session
      … --base-url https://<eventbridge-host>             # one command
```

**Two URLs, easily confused.** `EVENTBRIDGE_URL` / `--base-url` is *client side* —
where this CLI sends requests. `EVENT_BRIDGE_PUBLIC_BASE_URL` is set *on the
EventBridge deployment* and is what it advertises in HTML links and ntfy actions.
Changing one does not change the other.

**In-cluster runs are mock by default.** The `test` and `kind` overlays mount no
credential, so EventRunner auto-selects mock mode and every reply is
`MOCK-REPLY[...]`. That is expected, not a failure — a real agent needs the `demo`
overlay.

## Examples

Start an agent and print its reply:
```
python3 .claude/skills/eventbridge/eventbridge-cli.py run "Say hi"
```

Continue that conversation:
```
python3 .claude/skills/eventbridge/eventbridge-cli.py cont brave-otter-4718 "Now say goodbye"
```

Peek at what an in-flight or finished agent said, without restarting it:
```
python3 .claude/skills/eventbridge/eventbridge-cli.py watch brave-otter-4718
```

See the whole conversation as a chat log:
```
python3 .claude/skills/eventbridge/eventbridge-cli.py chat brave-otter-4718
```
