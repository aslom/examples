"""EventBridge entrypoint: HTTP server + Kafka consumer/producer."""
from __future__ import annotations

import json
import pathlib
import signal
import sys
import threading

from shared.pidfile import PidFile

from eventbridge.config import load
from eventbridge.correlation import Minter
from eventbridge.group_service import GroupService
from eventbridge.handlers import Handlers
from eventbridge.http_server import make
from eventbridge.kafka_in import Consumer
from eventbridge.kafka_out import Producer
from eventbridge.kafka_group_mirror import GroupMirror
from eventbridge.kafka_requests_mirror import RequestsMirror
from eventbridge.netcands import enumerate_candidates, format_startup_hint
from eventbridge.ntfy import NtfyPublisher
from eventbridge.openapi import spec
from eventbridge.router import Dispatcher
from eventbridge.store import Store


def main() -> int:
    cfg = load()
    pidfile = PidFile("eventbridge")
    pidfile.__enter__()
    print(f"[eventbridge] bootstrap={cfg.kafka_bootstrap} http={cfg.http_addr} ntfy={'on' if cfg.ntfy.enabled and cfg.ntfy.topic else 'off'}")
    try:
        _host, _port_s = cfg.http_addr.rsplit(":", 1)
        cands = enumerate_candidates()
        print(format_startup_hint(cands, int(_port_s), cfg.public_base_url))
    except Exception as e:  # noqa: BLE001
        print(f"[eventbridge] (candidate enumeration failed: {e})")

    # snapshot the OAS
    oas_path = pathlib.Path(cfg.tmpdir) / "eventbridge" / "openapi.json"
    oas_path.parent.mkdir(parents=True, exist_ok=True)
    oas_path.write_text(json.dumps(spec(), indent=2))

    store = Store(pathlib.Path(cfg.tmpdir) / "eventbridge")
    minter = Minter()
    for corr in store.all_correlations(limit=10000):
        minter.remember(corr)

    # The producer needs the RESPONSES topic too: group lifecycle events go there,
    # not on requests, because EventRunner would try to execute anything on requests.
    producer = Producer(cfg.kafka_bootstrap, cfg.request_topic, cfg.source_uri,
                        response_topic=cfg.response_topic)
    groups = GroupService(cfg, store, producer, minter)

    ntfy = NtfyPublisher(cfg.ntfy, cfg.public_base_url, store=store)
    if cfg.ntfy.enabled and cfg.ntfy.topic:
        ntfy.start()

    consumer = Consumer(cfg.kafka_bootstrap, cfg.response_topic, store,
                        on_event=ntfy.submit,
                        on_group_event=groups.on_group_event,
                        on_member_event=groups.on_member_event)
    consumer.start()

    # Back-fill prompts from the requests topic — also gives us prompt visibility
    # for correlations we didn't originate ourselves.
    requests_mirror = RequestsMirror(cfg.kafka_bootstrap, cfg.request_topic, store)
    requests_mirror.start()

    # §21.2: rebuild group history from the responses topic. The live consumer above
    # commits offsets and so never re-reads, which meant a restarted pod (with an
    # emptyDir /data) served 404 for every earlier group even though its notifications
    # had already gone out. One-shot, never publishes, never notifies.
    group_mirror = GroupMirror(cfg.kafka_bootstrap, cfg.response_topic, groups)
    group_mirror.start()

    h = Handlers(cfg, store, producer, minter, groups=groups)
    dsp = Dispatcher()
    dsp.add("POST", r"/v0/agents",                                          h.start_agent)
    dsp.add("POST", r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/continue",   h.continue_agent)
    dsp.add("POST", r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/continue-html", h.continue_agent_html)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)",            h.get_html)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/turns",      h.get_turns)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/events",     h.get_events)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/events\.jsonl", h.get_events_jsonl)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/events\.sse",   h.get_events_sse)
    # §16 Gap B: per-correlation claude transcript checkpoint. PUT by EventRunner
    # after each turn, GET before a mode=continue turn on a cold pod.
    dsp.add("PUT",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/transcript",   h.put_transcript)
    dsp.add("GET",  r"/v0/agents/(?P<correlationid>[a-z0-9-]+)/transcript",   h.get_transcript)
    # §21 agent groups. Registered before the agent routes purely for readability;
    # the path prefixes are disjoint so order does not matter.
    dsp.add("POST", r"/v0/groups",                                          h.create_group)
    dsp.add("GET",  r"/v0/groups",                                          h.list_groups)
    dsp.add("GET",  r"/v0/groups/(?P<groupid>[a-z0-9-]+)",                  h.group_html)
    dsp.add("GET",  r"/v0/groups/(?P<groupid>[a-z0-9-]+)/status",           h.group_status)
    dsp.add("POST", r"/v0/groups/(?P<groupid>[a-z0-9-]+)/close",            h.close_group)
    dsp.add("POST", r"/v0/groups/(?P<groupid>[a-z0-9-]+)/cancel",           h.cancel_group)
    dsp.add("GET",  r"/openapi\.json", h.openapi_json)
    dsp.add("GET",  r"/docs",          h.docs)
    dsp.add("GET",  r"/healthz",       h.healthz)
    dsp.add("GET",  r"/v0/selftest",   h.selftest)

    def app(environ, start_response):
        return dsp.dispatch(environ.get("REQUEST_METHOD", "GET"),
                            environ.get("PATH_INFO", "/"),
                            environ, start_response)

    host, port_s = cfg.http_addr.rsplit(":", 1)
    server = make((host, int(port_s)), app)
    print(f"[eventbridge] listening on http://{cfg.http_addr}  docs=/docs (Ctrl-C to stop)")

    # serve_forever() must run OFF the main thread — shutdown() blocks if it is
    # called from the same thread that is inside serve_forever().
    serve_thread = threading.Thread(target=server.serve_forever, name="wsgi-serve", daemon=True)
    serve_thread.start()

    # Run the URL self-test once the socket has bound so its verdicts reflect
    # reality (whether each candidate address actually answers).
    def _startup_selftest():
        import time

        from eventbridge.selftest import format_report, probe_candidates
        time.sleep(0.4)
        try:
            _h, _p = cfg.http_addr.rsplit(":", 1)
            results = probe_candidates(int(_p), current=cfg.public_base_url)
            print(format_report(results))
        except Exception as e:  # noqa: BLE001
            print(f"[selftest] failed: {e}")
    threading.Thread(target=_startup_selftest, daemon=True, name="startup-selftest").start()

    # §21.9.2: sweep group deadlines. A group short one member would otherwise never
    # complete and never notify, and silence is the worst failure mode for a batch the
    # operator has walked away from.
    def _deadline_sweeper():
        while not stop_evt.wait(timeout=15.0):
            try:
                n = groups.sweep_deadlines()
                if n:
                    print(f"[groups] completed {n} group(s) on deadline")
            except Exception as e:  # noqa: BLE001
                print(f"[groups] deadline sweep failed: {e!r}")

    stop_evt = threading.Event()
    threading.Thread(target=_deadline_sweeper, daemon=True, name="group-deadlines").start()
    signal_count = {"n": 0}

    def _sig(signum, _frame):
        signal_count["n"] += 1
        if signal_count["n"] == 1:
            print(f"\n[eventbridge] caught signal {signum}, shutting down (Ctrl-C again to force)")
            stop_evt.set()
        else:
            print("\n[eventbridge] second signal — forcing exit")
            import os as _os
            _os._exit(130)

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    try:
        while not stop_evt.wait(timeout=1.0):
            pass
    finally:
        try: server.shutdown()
        except Exception as e: print(f"[eventbridge] server.shutdown error: {e}")
        try: server.server_close()
        except Exception: pass
        serve_thread.join(timeout=5.0)
        consumer.stop(); consumer.join(timeout=5.0)
        requests_mirror.stop(); requests_mirror.join(timeout=5.0)
        group_mirror.stop(); group_mirror.join(timeout=5.0)
        ntfy.stop()
        if ntfy.is_alive(): ntfy.join(timeout=5.0)
        producer.close()
        pidfile.__exit__(None, None, None)
        print("[eventbridge] shut down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
