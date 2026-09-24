"""Graceful drain of in-flight agent runs. §8.3 / RQ-2 / T1.3.

Gate: "SIGTERM with a run in flight waits for it, and the drain lines appear on
**stderr**."

Phase 0's `Router.stop()` only set a flag — it never joined, so a terminating pod
abandoned a live `claude` subprocess. RQ-2 decided to wait instead, and to say
what it is waiting on, so `kubectl logs` during a scale-down shows a pod
finishing work rather than a pod hung on nothing.

stderr specifically: it is unbuffered by default and interleaves correctly with a
crash trace, and `kubectl logs` shows both streams.
"""
import threading
import time

from eventrunner.router import Router


def _until(predicate, timeout: float = 10.0, interval: float = 0.005) -> bool:
    """Wait for a condition rather than guessing a duration.

    Generous timeout on purpose: these tests only ever wait for something that is
    about to happen, so a long ceiling costs nothing when things are healthy and
    removes the flakiness when the machine is loaded. Three tests in this suite were
    wall-clock races before this existed — they passed alone and failed under load,
    which trains you to re-run instead of read.
    """
    import time as _t
    deadline = _t.monotonic() + timeout
    while _t.monotonic() < deadline:
        if predicate():
            return True
        _t.sleep(interval)
    return False


def _event(corr="brave-otter-4718"):
    return {"correlationid": corr}


class FakeEvent(dict):
    def __getitem__(self, k):
        return dict.__getitem__(self, k)


def test_drain_returns_immediately_when_nothing_is_in_flight():
    r = Router(lambda e: None, 4)
    lines = []
    started = time.monotonic()
    assert r.drain(5.0, log=lines.append) is True
    assert time.monotonic() - started < 0.5
    assert lines == [], "no drain chatter when there is nothing to drain"


def test_drain_waits_for_an_in_flight_run():
    gate = threading.Event()

    def slow(event):
        gate.wait(timeout=5)

    r = Router(slow, 4)
    r.submit(_event(), on_done=None)
    # Let the worker actually pick the event up.
    assert _until(lambda: r.in_flight_count() == 1), \
        "the worker never picked the event up"
    assert r.in_flight_count() == 1

    r.stop()
    released_at = {}

    def release():
        time.sleep(0.4)
        released_at["t"] = time.monotonic()
        gate.set()
    threading.Thread(target=release, daemon=True).start()

    ok = r.drain(5.0, log=lambda m: None)
    returned_at = time.monotonic()
    assert ok is True
    # Compare against when the run ACTUALLY finished, not a hardcoded duration: the
    # claim is "drain did not return before the work did", and that stays true however
    # the scheduler behaves.
    assert "t" in released_at, "the run never completed"
    assert returned_at >= released_at["t"], \
        "drain returned before the in-flight run finished"
    assert r.in_flight_count() == 0


def test_the_drain_lines_go_to_stderr_with_the_documented_wording(capsys):
    gate = threading.Event()
    r = Router(lambda e: gate.wait(timeout=5), 4)
    r.submit(_event("brave-otter-4718"))
    assert _until(lambda: r.in_flight_count() == 1), \
        "the worker never picked the event up"

    threading.Thread(target=lambda: (time.sleep(0.35), gate.set()), daemon=True).start()
    r.stop()
    r.drain(5.0, progress_interval=0.1)      # default log = stderr

    err = capsys.readouterr().err
    assert "SIGTERM received; draining 1 in-flight run(s), will not accept new work" in err
    assert "still waiting on correlationid=brave-otter-4718" in err
    assert "s elapsed)" in err
    assert "drain complete, exiting" in err


def test_progress_reports_name_every_in_flight_correlation(capsys):
    gate = threading.Event()
    r = Router(lambda e: gate.wait(timeout=5), 4)
    for corr in ("aaa-1", "bbb-2", "ccc-3"):
        r.submit(_event(corr))
    assert _until(lambda: r.in_flight_count() == 3), \
        "the worker never picked the event up"
    assert r.in_flight_count() == 3

    threading.Thread(target=lambda: (time.sleep(0.3), gate.set()), daemon=True).start()
    r.stop()
    r.drain(5.0, progress_interval=0.1)
    err = capsys.readouterr().err
    assert "draining 3 in-flight run(s)" in err
    for corr in ("aaa-1", "bbb-2", "ccc-3"):
        assert f"correlationid={corr}" in err


def test_drain_timeout_says_the_work_will_be_redelivered(capsys):
    never = threading.Event()
    r = Router(lambda e: never.wait(timeout=10), 4)
    r.submit(_event())
    assert _until(lambda: r.in_flight_count() == 1), \
        "the worker never picked the event up"
    r.stop()
    ok = r.drain(0.3, progress_interval=5.0)
    assert ok is False
    err = capsys.readouterr().err
    assert "drain TIMED OUT" in err
    assert "redelivered" in err, \
        "the operator needs to know the work is not lost, just repeated"
    never.set()


# ---- on_done: the contract the deferred commit depends on -------------------

def test_on_done_fires_after_the_run_completes_not_before():
    order = []

    def run(event):
        order.append("run-start")
        time.sleep(0.05)
        order.append("run-end")

    r = Router(run, 2)
    done = threading.Event()
    r.submit(_event(), on_done=lambda: (order.append("done"), done.set()))
    assert done.wait(timeout=3)
    assert order == ["run-start", "run-end", "done"]


def test_on_done_fires_even_when_the_run_raises():
    """A crashed run must still commit: declining to would make the message a
    poison pill the pod re-runs forever. RQ-1 already accepts at-least-once."""
    done = threading.Event()

    def boom(event):
        raise RuntimeError("agent blew up")

    r = Router(boom, 2)
    r.submit(_event(), on_done=done.set)
    assert done.wait(timeout=3), "on_done must fire on the failure path too"


def test_on_done_fires_exactly_once_per_submission():
    calls = []
    r = Router(lambda e: None, 2)
    for i in range(5):
        r.submit(_event(f"corr-{i}"), on_done=lambda: calls.append(1))
    deadline = time.monotonic() + 3
    while len(calls) < 5 and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.1)
    assert len(calls) == 5


def test_queued_work_is_abandoned_on_stop_so_kafka_redelivers_it():
    """Events still queued behind a running turn are deliberately dropped: their
    offsets were never completed, so the next pod to own the partition gets them.
    Dropping here is what makes that redelivery correct rather than duplicated."""
    gate = threading.Event()
    ran = []

    def run(event):
        ran.append(event["correlationid"])
        gate.wait(timeout=5)

    r = Router(run, 2)
    corr = "same-corr-1"
    dones = []
    # Same correlation => strictly serialized, so the second waits in the queue.
    r.submit({"correlationid": corr}, on_done=lambda: dones.append("first"))
    assert _until(lambda: r.in_flight_count() == 1), \
        "the worker never picked the event up"
    r.submit({"correlationid": corr}, on_done=lambda: dones.append("second"))
    assert r.pending_count(corr) == 1

    r.stop()
    gate.set()
    r.drain(3.0, log=lambda m: None)
    assert dones == ["first"], "the queued second turn must NOT be marked done"
    assert len(ran) == 1


def test_distinct_correlations_run_in_parallel_bounded_by_max_concurrent():
    live = []
    lock = threading.Lock()
    peak = {"n": 0}
    gate = threading.Event()

    def run(event):
        with lock:
            live.append(1)
            peak["n"] = max(peak["n"], len(live))
        gate.wait(timeout=5)
        with lock:
            live.pop()

    r = Router(run, 2)
    for i in range(6):
        r.submit(_event(f"corr-{i}"))
    time.sleep(0.3)
    assert peak["n"] <= 2, f"concurrency cap breached: {peak['n']}"
    gate.set()
    r.drain(3.0, log=lambda m: None)


def test_on_done_runs_before_the_run_stops_counting_as_in_flight():
    """Regression: `drain()` uses in_flight == 0 as "everything finished", and the
    consumer makes its final offset commit right after drain returns. If a run left
    the in-flight set before its on_done (which records the offset completion) had
    run, drain could return and the commit could happen with the ledger still
    believing work was outstanding.

    Surfaced as an intermittent failure of
    test_queued_work_is_abandoned_on_stop_so_kafka_redelivers_it.
    """
    observed: list[int] = []
    r = Router(lambda e: None, 2)

    def on_done():
        # At this instant the run must still be counted, or the ordering is wrong.
        observed.append(r.in_flight_count())

    for i in range(20):
        r.submit(_event(f"corr-{i}"), on_done=on_done)
    deadline = time.monotonic() + 5
    while len(observed) < 20 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(observed) == 20, f"only {len(observed)} callbacks fired"
    assert all(n >= 1 for n in observed), \
        f"on_done saw in_flight == 0 for its own run: {observed}"
