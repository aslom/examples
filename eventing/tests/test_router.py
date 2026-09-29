"""Per-correlationid Router: FIFO ordering, one-at-a-time per correlationid,
parallel across correlationids. This test does not touch Kafka or claude."""
import threading
import time

import pytest

from eventrunner.router import Router
from shared import ce


def _mk_event(corr: str, tag: str):
    return ce.new_event(
        type=ce.TYPE_REQUEST,
        source="test",
        datacontenttype="application/json",
        correlationid=corr,
        sessionuuid=ce.session_uuid(corr),
        mode="start",
        data={"prompt": tag},
    )


def test_same_correlationid_runs_serially_in_fifo_order():
    log: list[str] = []
    log_lock = threading.Lock()
    order_gate = threading.Event()

    def slow_run(event):
        with log_lock:
            log.append(f"start:{event.data['prompt']}")
        # The first invocation blocks until we release it, proving that the
        # second submission had to wait.
        if event.data["prompt"] == "A":
            order_gate.wait(timeout=5.0)
        time.sleep(0.02)
        with log_lock:
            log.append(f"end:{event.data['prompt']}")

    r = Router(slow_run, max_concurrent_correlations=4)
    corr = "same-corr-0001"
    r.submit(_mk_event(corr, "A"))
    time.sleep(0.05)  # ensure A is in-flight
    r.submit(_mk_event(corr, "B"))
    r.submit(_mk_event(corr, "C"))

    # B and C are queued behind A
    with log_lock:
        assert log == ["start:A"], log
    order_gate.set()

    # Wait for all to finish
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        with log_lock:
            if len(log) >= 6:
                break
        time.sleep(0.02)

    with log_lock:
        # Serialized: no interleave of start/end between A, B, C
        assert log == [
            "start:A", "end:A",
            "start:B", "end:B",
            "start:C", "end:C",
        ], log


def test_different_correlationids_run_in_parallel():
    in_flight_max = 0
    in_flight_lock = threading.Lock()
    in_flight = 0
    barrier = threading.Barrier(3, timeout=3.0)

    def run(event):
        nonlocal in_flight, in_flight_max
        with in_flight_lock:
            in_flight += 1
            in_flight_max = max(in_flight_max, in_flight)
        # Sync all three so they overlap
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass
        with in_flight_lock:
            in_flight -= 1

    r = Router(run, max_concurrent_correlations=4)
    for i, tag in enumerate("ABC"):
        r.submit(_mk_event(f"corr-{tag.lower()}-000{i}", tag))

    # Give workers time to spin up
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and in_flight_max < 3:
        time.sleep(0.01)
    assert in_flight_max == 3, f"expected 3 concurrent workers, got {in_flight_max}"


def test_concurrency_cap_is_enforced():
    active = 0
    active_lock = threading.Lock()
    peak = 0
    release = threading.Event()

    def run(event):
        nonlocal active, peak
        with active_lock:
            active += 1
            peak = max(peak, active)
        release.wait(timeout=2.0)
        with active_lock:
            active -= 1

    r = Router(run, max_concurrent_correlations=2)
    corrs = [f"cap-corr-000{i}" for i in range(5)]
    for c in corrs:
        r.submit(_mk_event(c, c))

    # Wait for peak to stabilize at the cap
    time.sleep(0.2)
    assert peak == 2, f"expected peak=2 (cap), got {peak}"
    release.set()


def test_slot_reuse_after_drain():
    """After a slot drains, a new submit for the same corr must re-arm a worker."""
    log = []
    log_lock = threading.Lock()

    def run(event):
        with log_lock:
            log.append(event.data["prompt"])

    r = Router(run, max_concurrent_correlations=4)
    corr = "reuse-corr-0001"
    r.submit(_mk_event(corr, "first"))

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with log_lock:
            if log == ["first"]:
                break
        time.sleep(0.02)

    # Wait for the worker thread to actually exit
    time.sleep(0.1)
    r.submit(_mk_event(corr, "second"))

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        with log_lock:
            if log == ["first", "second"]:
                return
        time.sleep(0.02)
    pytest.fail(f"second submit was not processed: log={log}")
