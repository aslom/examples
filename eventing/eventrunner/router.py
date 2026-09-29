"""Per-correlationid AgentSlot + FIFO. See DESIGN_PHASE0.md §4.5.

Guarantees:
 * At most one live claude subprocess per correlationid.
 * Requests for the same correlationid are dispatched in the order they were
   accepted (Kafka key = correlationid → single partition → in-order delivery,
   FIFO preserved here).
 * Different correlationids run in parallel, bounded by BoundedSemaphore.

Phase 1 additions (DESIGN_PHASE1.md §8.3 / RQ-1 / RQ-2):
 * `submit(event, on_done=…)` — the callback fires once the run has finished,
   which is what lets the consumer defer its offset commit until then.
 * `drain(timeout)` — SIGTERM must WAIT for in-flight `claude` runs rather than
   abandoning them, and say what it is waiting on. Phase 0's `stop()` only set a
   flag; it never joined, so a terminating pod dropped live work on the floor.

On a cluster the ordering guarantee now spans pods as well as threads: because
Phase 0 keys request messages by correlationid, all turns for one conversation
land on one partition, and Kafka assigns each partition to exactly one consumer
in the group. Hence `maxReplicaCount` ≤ partition count.
"""
from __future__ import annotations

import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable


def _elog(msg: str) -> None:
    """Drain progress goes to stderr: unbuffered by default, and it interleaves
    correctly with a crash trace. `kubectl logs` shows both streams."""
    print(f"[eventrunner] {msg}", file=sys.stderr, flush=True)


@dataclass
class AgentSlot:
    correlationid: str
    queue: deque = field(default_factory=deque)
    lock: threading.Lock = field(default_factory=threading.Lock)
    busy: bool = False
    worker: threading.Thread | None = None


@dataclass
class InFlight:
    correlationid: str
    started: float


class Router:
    def __init__(self, run_agent: Callable[[object], None],
                 max_concurrent_correlations: int) -> None:
        self._run_agent = run_agent
        self._slots: dict[str, AgentSlot] = {}
        self._map_lock = threading.Lock()
        self._concurrency = threading.BoundedSemaphore(max_concurrent_correlations)
        self._stopping = threading.Event()
        # Live runs, keyed by an id() unique per dispatch so two turns of the same
        # correlation (sequential by construction) are still counted correctly.
        self._inflight: dict[int, InFlight] = {}
        self._inflight_lock = threading.Lock()
        self._idle = threading.Condition()

    # ---- intake -------------------------------------------------------------
    def submit(self, event, on_done: Callable[[], None] | None = None) -> None:
        """Queue an event for its correlation's slot.

        `on_done` is invoked exactly once after the run finishes, whether it
        succeeded or raised. That "whether it raised" matters: an exception in
        run_agent is a bug in *our* code, not a transient broker problem, and
        declining to commit would make the message a poison pill that the pod
        re-runs forever. RQ-1 already accepts at-least-once, so commit and log.
        """
        corr = event["correlationid"]
        with self._map_lock:
            slot = self._slots.get(corr)
            if slot is None:
                slot = AgentSlot(correlationid=corr)
                self._slots[corr] = slot
        with slot.lock:
            slot.queue.append((event, on_done))
            if not slot.busy:
                slot.busy = True
                slot.worker = threading.Thread(
                    target=self._drain, args=(slot,), daemon=True,
                    name=f"agent-worker/{corr}",
                )
                slot.worker.start()

    # ---- execution ----------------------------------------------------------
    def _drain(self, slot: AgentSlot) -> None:
        self._concurrency.acquire()
        try:
            while True:
                with slot.lock:
                    if not slot.queue or self._stopping.is_set():
                        slot.busy = False
                        return
                    event, on_done = slot.queue.popleft()
                token = id(event)
                with self._inflight_lock:
                    self._inflight[token] = InFlight(slot.correlationid, time.monotonic())
                try:
                    self._run_agent(event)
                except Exception as e:  # noqa: BLE001
                    _elog(f"[router/{slot.correlationid}] run_agent raised: {e!r}")
                finally:
                    # ORDER MATTERS. `on_done` records the offset completion, and
                    # `drain()` treats in_flight == 0 as "everything finished". If
                    # the slot were cleared first, drain could return — and the
                    # consumer could make its final commit — before the ledger had
                    # been told this run completed. So: complete, THEN stop
                    # counting it, THEN wake the drain.
                    if on_done is not None:
                        try:
                            on_done()
                        except Exception as e:  # noqa: BLE001
                            _elog(f"[router/{slot.correlationid}] on_done raised: {e!r}")
                    with self._inflight_lock:
                        self._inflight.pop(token, None)
                    with self._idle:
                        self._idle.notify_all()
        finally:
            self._concurrency.release()

    # ---- shutdown (§8.3, RQ-2) ----------------------------------------------
    def stop(self) -> None:
        """Stop accepting *new* dispatches. Does not wait — call drain() for that.

        Events still sitting in a slot queue are deliberately abandoned: their
        offsets were never completed, so Kafka redelivers them to whichever pod
        picks up the partition next. Dropping them here is what makes that
        redelivery correct rather than duplicated.
        """
        self._stopping.set()

    def in_flight(self) -> list[InFlight]:
        with self._inflight_lock:
            return list(self._inflight.values())

    def in_flight_count(self) -> int:
        with self._inflight_lock:
            return len(self._inflight)

    def drain(self, timeout: float, *, progress_interval: float = 15.0,
              log: Callable[[str], None] = _elog) -> bool:
        """Wait for in-flight runs to finish. True if all completed in time.

        Prints what it is waiting on and for how long, so `kubectl logs` during a
        scale-down shows a pod finishing work rather than a pod hung on nothing.
        """
        n = self.in_flight_count()
        if n == 0:
            return True
        log(f"SIGTERM received; draining {n} in-flight run(s), "
            f"will not accept new work")
        started = time.monotonic()
        next_report = started + progress_interval
        while True:
            now = time.monotonic()
            with self._idle:
                remaining = max(0.0, timeout - (now - started))
                if self.in_flight_count() == 0:
                    break
                if remaining <= 0:
                    break
                # Bound the wait by the next report deadline, not a fixed slice.
                # Otherwise the only thing that wakes us is a run completing, and
                # by then in_flight is empty — so the progress lines, which exist
                # to show a LONG run being waited on, would never print.
                self._idle.wait(timeout=max(0.02, min(remaining, next_report - now)))
            now = time.monotonic()
            if now >= next_report:
                for f in self.in_flight():
                    log(f"still waiting on correlationid={f.correlationid} "
                        f"({now - f.started:.0f}s elapsed)")
                next_report = now + progress_interval
        left = self.in_flight_count()
        if left:
            log(f"drain TIMED OUT after {timeout:.0f}s with {left} run(s) still "
                f"going; their offsets stay uncommitted so the work will be "
                f"redelivered")
            return False
        log("drain complete, exiting")
        return True

    # Test/debug helpers
    def slots(self) -> dict[str, AgentSlot]:
        return dict(self._slots)

    def pending_count(self, corr: str) -> int:
        slot = self._slots.get(corr)
        return len(slot.queue) if slot else 0
