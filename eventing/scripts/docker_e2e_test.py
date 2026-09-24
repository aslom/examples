#!/usr/bin/env python3
"""Container end-to-end test for the wire path. Converted from
`docker-e2e-test.sh` (T0.6), absorbing its `_events.py` helper.

T0.6 is the load-bearing conversion: the shell version passed end to end before
Phase 1 started, so there was a known-good baseline to diff against. The same
assertions are made here, in the same order, with the same `[e2e] PASS` output.

Builds both images, brings up a single-node KRaft Kafka, publishes EventBridge on
a NEW host port (18080 by default, so it does not collide with a local demo on
8080), drives one agent turn through Kafka and asserts the response events come
back.

Deliberately free and deterministic: EventRunner is given no Anthropic
credential, so it auto-selects mock mode. A credential in your shell is NOT
forwarded unless you pass --real.

  python3 scripts/docker_e2e_test.py                 # build, test, tear down
  python3 scripts/docker_e2e_test.py --port 19090
  python3 scripts/docker_e2e_test.py --keep          # leave containers up
  python3 scripts/docker_e2e_test.py --no-build      # reuse existing images

Exit 0 only if every assertion passed.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from imagelib import runtime_ready  # noqa: E402
from proclib import Checks, die, run, wait_for  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
CTX = HERE.parent
PREFIX = "e2e"
PROMPT = "docker-e2e probe"
CONTINUE_PROMPT = "and one more thing"


# ---- HTTP helpers (absorbing the old _events.py) ----------------------------

def http_json(url: str, *, timeout: float = 5.0, method: str = "GET",
              body: bytes | None = None, content_type: str = "application/json"):
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


def http_bytes(url: str, *, timeout: float = 5.0) -> tuple[int, bytes]:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read()


def has_final(events: list[dict]) -> bool:
    return any(e.get("final") or (e.get("data") or {}).get("role") == "final"
               for e in events)


def summarize(c: Checks, events: list[dict]) -> None:
    c.log(f"  events={len(events)}")
    for e in events:
        d = e.get("data") or {}
        text = (d.get("text") or "")[:60].replace("\n", " ")
        c.log(f"  seq={e.get('sequence')} phase={e.get('phase')} "
              f"role={d.get('role')} text={text!r}")


# ---- docker plumbing --------------------------------------------------------

class Compose:
    """The three containers plus their network, as one teardown-able unit."""

    def __init__(self, runtime: str, tag: str, net: str, log_dir: pathlib.Path):
        self.rt = runtime
        self.tag = tag
        self.net = net
        self.log_dir = log_dir
        self.eb = f"{tag}-eventbridge"
        self.er = f"{tag}-eventrunner"
        self.kfk = f"{tag}-kafka"

    def rm_all(self) -> None:
        run([self.rt, "rm", "-f", self.eb, self.er, self.kfk], timeout=120)
        run([self.rt, "network", "rm", self.net], timeout=60)

    def logs(self, container: str) -> str:
        r = run([self.rt, "logs", container], timeout=60, merge_stderr=True)
        return r.out or ""

    def save_logs(self) -> None:
        for name in (self.er, self.eb, self.kfk):
            (self.log_dir / f"{name}.log").write_text(self.logs(name))

    def running(self, container: str) -> bool:
        r = run([self.rt, "inspect", "-f", "{{.State.Running}}", container], timeout=30)
        return r.ok and r.out.strip() == "true"

    def exec_(self, container: str, argv: list[str], timeout: float = 60):
        return run([self.rt, "exec", container, *argv], timeout=timeout)


KAFKA_ENV = {
    "KAFKA_NODE_ID": "1",
    "KAFKA_PROCESS_ROLES": "broker,controller",
    "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
    "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092",
    "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@kafka:9093",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
    "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
    # 0 rather than the 3000ms default: this is the same rebalance delay that
    # §16 Gap A records as a floor on cluster wake latency. Locally we can just
    # turn it off, which keeps the test fast.
    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
}


def env_flags(mapping: dict[str, str]) -> list[str]:
    out = []
    for k, v in mapping.items():
        out += ["-e", f"{k}={v}"]
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 18080)))
    ap.add_argument("--tag", default=os.environ.get("TAG", "rossoctl-eventing-e2e"))
    ap.add_argument("--kafka-image", default=os.environ.get("KAFKA_IMAGE", "apache/kafka:4.3.1"))
    ap.add_argument("--docker", default=os.environ.get("DOCKER", "docker"))
    ap.add_argument("--keep", action="store_true", help="leave containers up to poke at")
    ap.add_argument("--no-build", action="store_true", help="reuse existing images")
    ap.add_argument("--real", action="store_true", help="forward your Anthropic credential")
    ap.add_argument("--log-dir", default=os.environ.get(
        "LOG_DIR", f"{os.environ.get('TMPDIR', '/tmp').rstrip('/')}/rossoctl-eventing-e2e"))
    args = ap.parse_args(argv)

    base = f"http://127.0.0.1:{args.port}"
    c = Checks(prefix=PREFIX)
    log_dir = pathlib.Path(args.log_dir)

    # ---- preflight ----------------------------------------------------------
    ok, why = runtime_ready(args.docker)
    if not ok:
        die(why, prefix=PREFIX)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        die(f"cannot create log dir {log_dir}: {e}", prefix=PREFIX)

    import socket
    with socket.socket() as s:
        if s.connect_ex(("127.0.0.1", args.port)) == 0:
            die(f"host port {args.port} is already in use; pass --port <free-port>",
                prefix=PREFIX)

    c.log(f"runtime={args.docker}  port={args.port}  context={CTX}  logs={log_dir}")
    net = os.environ.get("NET", f"{args.tag}-net")
    comp = Compose(args.docker, args.tag, net, log_dir)

    # Stale containers from an interrupted run would make `docker run` fail on a
    # name conflict, which reads like a real failure. Clear them first.
    comp.rm_all()

    try:
        return _run(c, args, comp, base, log_dir)
    finally:
        if args.keep:
            c.log("--keep set; leaving containers running:")
            c.log(f"  EventBridge  {base}   (logs: {args.docker} logs {comp.eb})")
            c.log(f"  EventRunner            (logs: {args.docker} logs {comp.er})")
            c.log(f"  tear down:   {args.docker} rm -f {comp.eb} {comp.er} {comp.kfk} "
                  f"&& {args.docker} network rm {net}")
        else:
            c.log("tearing down")
            comp.save_logs()
            comp.rm_all()


def _run(c: Checks, args, comp: Compose, base: str, log_dir: pathlib.Path) -> int:
    rt = args.docker
    eb_img = f"{args.tag}-eventbridge:dev"
    er_img = f"{args.tag}-eventrunner:dev"

    # ---- build -------------------------------------------------------------
    if not args.no_build:
        c.log("building images (the first build downloads a free-threaded "
              "CPython — expect a few minutes)")
        for dockerfile, img, name in (("Dockerfile-eventbridge", eb_img, "eventbridge"),
                                      ("Dockerfile-eventrunner", er_img, "eventrunner")):
            res = run([rt, "build", "-f", str(CTX / dockerfile), "-t", img, str(CTX)],
                      timeout=3600, merge_stderr=True)
            (log_dir / f"build-{name}.log").write_text(res.out or "")
            if not res.ok:
                die(f"{name} build failed — see {log_dir}/build-{name}.log\n"
                    f"{res.tail(15)}", prefix=PREFIX)
            c.log(f"  built {img}")
    else:
        c.log("--no-build: reusing existing images")

    # ---- mock-mode decision, tested directly on the image ------------------
    # Exercises eventrunner/config.py inside the built image with no Kafka, so a
    # regression in the credential sniffing shows up in about a second.
    c.log("checking mock-mode auto-selection")
    probe = ("import eventrunner.config as c; f=c.load(); "
             'print(f"mock={f.mock_claude} reason={f.mock_reason}")')
    for extra_env, needle, desc in (
        ([], "mock=True", "no credential in env -> mock mode"),
        (["-e", "ANTHROPIC_API_KEY=sk-ant-not-a-real-key"], "mock=False",
         "ANTHROPIC_API_KEY present -> real claude mode"),
        (["-e", "ER_MOCK_CLAUDE=false"], "mock=False",
         "explicit ER_MOCK_CLAUDE=false overrides a missing credential"),
    ):
        res = run([rt, "run", "--rm", *extra_env, er_img, "python", "-c", probe],
                  timeout=180, merge_stderr=True)
        if not res.ok:
            c.fail(desc, f"config probe crashed:\n{res.tail(8)}")
            continue
        c.expect_contains(res.out, needle, desc)

    # Phase 1: the consumer group must be configurable, because a mismatch with
    # the ScaledObject trigger is a silent no-scale failure (§8.2).
    res = run([rt, "run", "--rm", "-e", "ER_CONSUMER_GROUP=kev1-eventrunner",
               er_img, "python", "-c",
               "import eventrunner.config as c; print('group=' + c.load().consumer_group)"],
              timeout=180, merge_stderr=True)
    c.expect_contains(res.out, "group=kev1-eventrunner",
                      "ER_CONSUMER_GROUP reaches the config in-image")

    # ---- Kafka -------------------------------------------------------------
    c.log(f"starting Kafka ({args.kafka_image})")
    run([rt, "network", "create", comp.net], timeout=60)
    res = run([rt, "run", "-d", "--name", comp.kfk, "--network", comp.net,
               "--network-alias", "kafka", *env_flags(KAFKA_ENV), args.kafka_image],
              timeout=300, merge_stderr=True)
    if not res.ok:
        die(f"could not start Kafka:\n{res.tail(10)}", prefix=PREFIX)

    c.log("waiting for the broker to accept admin calls")
    up = wait_for(
        lambda: comp.exec_(comp.kfk, ["/opt/kafka/bin/kafka-topics.sh",
                                      "--bootstrap-server", "localhost:9092",
                                      "--list"], timeout=30).ok,
        timeout=120, interval=2)
    if not c.expect(up, f"Kafka is up (after {up.elapsed_s:.0f}s)"):
        die(f"Kafka not ready within 120s ({rt} logs {comp.kfk})", prefix=PREFIX)

    # Topics are created up front. Phase 1 gave EventRunner a retry loop (§8.1)
    # so this is no longer strictly required, but creating them keeps the test
    # measuring the wire path rather than the retry backoff.
    for topic in ("requests", "responses"):
        res = comp.exec_(comp.kfk, ["/opt/kafka/bin/kafka-topics.sh",
                                    "--bootstrap-server", "localhost:9092",
                                    "--create", "--if-not-exists", "--topic", topic,
                                    "--partitions", "1", "--replication-factor", "1"],
                         timeout=90)
        if not res.ok:
            die(f"could not create topic {topic}: {res.tail(4)}", prefix=PREFIX)
    c.ok("topics requests/responses created")

    # ---- the services ------------------------------------------------------
    runner_env = ["-e", "KAFKA_BOOTSTRAP=kafka:9092",
                  # Phase 1: point the runner at EventBridge so transcript
                  # checkpointing (§16 Gap B) is exercised in containers too.
                  "-e", f"ER_EVENTBRIDGE_URL=http://{comp.eb}:8080"]
    if args.real:
        token = os.environ.get("ANTHROPIC_AUTH_TOKEN")
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not (token or key):
            die("--real needs ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN exported",
                prefix=PREFIX)
        c.log("--real: forwarding your credential. This image ships no `claude` CLI,")
        c.log("        so expect 'claude binary not found' unless you provide one.")
        if key:
            runner_env += ["-e", f"ANTHROPIC_API_KEY={key}"]
        if token:
            runner_env += ["-e", f"ANTHROPIC_AUTH_TOKEN={token}"]

    c.log("starting EventRunner")
    res = run([rt, "run", "-d", "--name", comp.er, "--network", comp.net,
               "--network-alias", comp.er, *runner_env, er_img],
              timeout=180, merge_stderr=True)
    if not res.ok:
        die(f"could not start EventRunner:\n{res.tail(10)}", prefix=PREFIX)

    c.log(f"starting EventBridge on host port {args.port}")
    res = run([rt, "run", "-d", "--name", comp.eb, "--network", comp.net,
               "--network-alias", comp.eb,
               "-p", f"127.0.0.1:{args.port}:8080",
               "-e", "KAFKA_BOOTSTRAP=kafka:9092",
               "-e", "EB_HTTP_ADDR=0.0.0.0:8080",
               "-e", f"EVENT_BRIDGE_PUBLIC_BASE_URL={base}",
               "-e", "NTFY_ENABLED=false", eb_img],
              timeout=180, merge_stderr=True)
    if not res.ok:
        die(f"could not start EventBridge:\n{res.tail(10)}", prefix=PREFIX)

    c.log(f"waiting for {base}/healthz")

    def healthz():
        # Fail fast if the container died rather than burning the whole timeout.
        if not comp.running(comp.eb):
            raise RuntimeError("EventBridge exited during startup")
        try:
            status, _ = http_json(f"{base}/healthz", timeout=2)
            return status == 200
        except (urllib.error.URLError, OSError):
            return False

    healthy = wait_for(healthz, timeout=60, interval=1)
    if not healthy:
        (log_dir / "eventbridge.log").write_text(comp.logs(comp.eb))
        die(f"no /healthz response from {base} after 60s — see {log_dir}/eventbridge.log",
            prefix=PREFIX)
    c.ok(f"HTTP server answering on new port {args.port}")

    time.sleep(2)
    startup = comp.logs(comp.er)
    (log_dir / "eventrunner-startup.log").write_text(startup)
    if not args.real:
        c.expect_contains(startup, "mock_claude=True",
                          "EventRunner announced mock mode at startup")
        c.expect_contains(startup, "auto:",
                          "startup log explains why mock mode was chosen")
    c.expect_contains(startup, "connected to kafka:9092",
                      "consumer connected (and says so — §8.1 retry loop)")

    # ---- drive one turn through the full wire path -------------------------
    c.log("POST /v0/agents")
    try:
        status, start = http_json(f"{base}/v0/agents", method="POST", timeout=15,
                                  body=json.dumps({"prompt": PROMPT, "max_turns": 1}).encode())
    except (urllib.error.URLError, OSError) as e:
        die(f"POST /v0/agents failed: {e}", prefix=PREFIX)
    corr = (start or {}).get("correlationid")
    if not corr:
        die(f"no correlationid in response: {start}", prefix=PREFIX)
    c.ok(f"run accepted, correlationid={corr}")

    c.log("polling /events until final")
    events: list[dict] = []

    def final_arrived():
        nonlocal events
        try:
            _, body = http_json(f"{base}/v0/agents/{corr}/events", timeout=4)
        except (urllib.error.URLError, OSError):
            return False
        events = (body or {}).get("events") or []
        return has_final(events)

    got = wait_for(final_arrived, timeout=60, interval=1)
    if not c.expect(got, f"final event received (after {got.elapsed_s:.1f}s)",
                    f"EventRunner may not be consuming — {rt} logs {comp.er}"):
        (log_dir / "eventrunner.log").write_text(comp.logs(comp.er))
    else:
        summarize(c, events)
        if not args.real:
            c.expect_contains(json.dumps(events), "MOCK-REPLY",
                              "mock assistant reply rode the wire")
        # §11 causation binding: every response names its triggering request.
        raw_status, raw = http_bytes(f"{base}/v0/agents/{corr}/events.jsonl")
        lines = [json.loads(ln) for ln in raw.decode().splitlines() if ln.strip()]
        c.expect(lines and all(e.get("causationid") for e in lines),
                 "every response event carries ce_causationid (§11)",
                 f"missing on {sum(1 for e in lines if not e.get('causationid'))} of {len(lines)}")
        c.expect(len({e.get("causationid") for e in lines}) == 1,
                 "all events of one turn share the same causationid")

    # The ndjson and turn-grouped views are what downstream tooling reads.
    try:
        status, raw = http_bytes(f"{base}/v0/agents/{corr}/events.jsonl")
        (log_dir / "events.jsonl").write_bytes(raw)
        nlines = len([ln for ln in raw.decode().splitlines() if ln.strip()])
        c.expect(status == 200 and nlines > 0, f"events.jsonl served ({nlines} lines)")
    except (urllib.error.URLError, OSError) as e:
        c.fail("events.jsonl was empty or errored", str(e))

    try:
        _, turns = http_json(f"{base}/v0/agents/{corr}/turns", timeout=10)
        rows = (turns or {}).get("turns") or []
        c.expect(rows and rows[0].get("prompt") == PROMPT,
                 "/turns paired the prompt with its turn",
                 f"got {rows[:1]}")
    except (urllib.error.URLError, OSError) as e:
        c.fail("/turns did not report the prompt", str(e))

    # ---- Phase 1: transcript checkpoint endpoints (§16 Gap B / T1.8) -------
    # Mock mode spawns no `claude`, so no real transcript exists — the endpoint
    # contract is still asserted directly, which is what the runner depends on.
    try:
        payload = b'{"type":"user"}\n{"type":"assistant"}\n'
        status, meta = http_json(f"{base}/v0/agents/{corr}/transcript", method="PUT",
                                 body=payload, content_type="application/x-ndjson")
        c.expect(status == 200 and meta.get("size") == len(payload),
                 "PUT /transcript stored the session transcript", f"{status} {meta}")
        status, got_bytes = http_bytes(f"{base}/v0/agents/{corr}/transcript")
        c.expect(status == 200 and got_bytes == payload,
                 "GET /transcript returned it byte-identical")
    except (urllib.error.URLError, OSError) as e:
        c.fail("transcript endpoints failed", str(e))

    try:
        http_json(f"{base}/v0/agents/never-minted-9999/transcript", method="PUT",
                  body=b"x", content_type="application/x-ndjson")
        c.fail("PUT /transcript for an unknown correlationid is refused",
               "it returned success")
    except urllib.error.HTTPError as e:
        c.expect(e.code == 404,
                 "PUT /transcript for an unknown correlationid is refused (404)",
                 f"got HTTP {e.code}")
    except (urllib.error.URLError, OSError) as e:
        c.fail("transcript 404 check errored", str(e))

    # ---- a /continue turn, which is what resume-by-path is for -------------
    try:
        status, cont = http_json(f"{base}/v0/agents/{corr}/continue", method="POST",
                                 timeout=15,
                                 body=json.dumps({"prompt": CONTINUE_PROMPT}).encode())
        c.expect(status == 202, "POST /continue accepted", f"status={status}")

        def second_turn():
            _, body = http_json(f"{base}/v0/agents/{corr}/turns", timeout=5)
            return len((body or {}).get("turns") or []) >= 2

        two = wait_for(second_turn, timeout=60, interval=1)
        c.expect(two, "the second turn completed and /turns shows both")
    except (urllib.error.URLError, OSError) as e:
        c.fail("/continue failed", str(e))

    # ---- the runner drains rather than abandoning work (§8.3) --------------
    res = run([rt, "stop", "-t", "30", comp.er], timeout=90, merge_stderr=True)
    if res.ok:
        shutdown = comp.logs(comp.er)
        c.expect_contains(shutdown, "shut down",
                          "EventRunner exited cleanly on SIGTERM (docker stop)")
        c.expect("caught signal 15" in shutdown or "caught signal" in shutdown,
                 "SIGTERM was handled by the signal handler, not SIGKILL",
                 shutdown[-400:])
    else:
        c.fail("docker stop of EventRunner", res.tail(5))

    c.note("correlationid", corr)
    c.note("logs", str(log_dir))
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
