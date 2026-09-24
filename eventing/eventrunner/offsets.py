"""Deferred offset commits, so consumer lag means "work not finished".

Resolves DESIGN_PHASE1.md RQ-1 and, with it, the §7.1 scale-down race.

Phase 0 committed immediately after handing a message to the Router, so lag hit
zero the instant a message was *read* — while the agent was still streaming.
KEDA sees zero lag, the cooldown elapses, and the pod carrying a live `claude`
subprocess is selected for termination.

Moving the commit to after the terminal (`final=true`) event makes lag mean
"work not finished", which fixes the race at its root: KEDA cannot see zero lag
while a run is in flight. The cost is at-least-once delivery — a pod killed
mid-run causes redelivery — which RQ-1 accepts, assuming idempotent agent runs,
and which consumers dedupe on `(correlationid, sequence)`.

This ledger is the bookkeeping that makes it safe. Kafka commits a *position*,
not individual messages, so committing offset N asserts everything below N is
done. With several runs in flight concurrently they finish out of order, and
committing the newest completed offset would silently drop the older unfinished
ones. So only the **contiguous completed prefix** of each partition is ever
committed.
"""
from __future__ import annotations

import threading
from typing import Iterable

# (topic, partition) — a plain tuple so this module is testable without kafka-python.
TP = tuple[str, int]


class OffsetLedger:
    """Tracks submitted-vs-completed offsets per partition.

    Thread-safe: the consumer thread submits, Router worker threads complete.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # In Kafka delivery order per partition, so a list used as a queue is
        # already sorted and needs no heap.
        self._submitted: dict[TP, list[int]] = {}
        self._done: dict[TP, set[int]] = {}

    # ---- the consumer thread ----
    def submit(self, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._submitted.setdefault((topic, partition), []).append(offset)

    # ---- worker threads ----
    def complete(self, topic: str, partition: int, offset: int) -> None:
        """Mark one message's work finished. Idempotent."""
        with self._lock:
            self._done.setdefault((topic, partition), set()).add(offset)

    # ---- commit decision ----
    def committable(self) -> dict[TP, int]:
        """`{(topic, partition): next offset to commit}` for completed prefixes.

        Does **not** mutate — use `take_committable()` when actually committing,
        so a failed commit can be retried on the next poll.
        """
        out: dict[TP, int] = {}
        with self._lock:
            for tp, pending in self._submitted.items():
                done = self._done.get(tp, set())
                highest: int | None = None
                for off in pending:
                    if off not in done:
                        break
                    highest = off
                if highest is not None:
                    # Kafka commits the offset of the NEXT message to read.
                    out[tp] = highest + 1
        return out

    def take_committable(self) -> dict[TP, int]:
        """`committable()`, and forget what it covers.

        Called only once the commit has been accepted by the broker.
        """
        out = self.committable()
        if not out:
            return out
        with self._lock:
            for tp, next_off in out.items():
                pending = self._submitted.get(tp, [])
                done = self._done.get(tp, set())
                keep = [o for o in pending if o >= next_off]
                self._submitted[tp] = keep
                self._done[tp] = {o for o in done if o >= next_off}
                if not keep:
                    self._submitted.pop(tp, None)
                    self._done.pop(tp, None)
        return out

    # ---- introspection ----
    def in_flight(self) -> int:
        """Submitted but not yet completed, across all partitions."""
        with self._lock:
            return sum(len([o for o in offs if o not in self._done.get(tp, set())])
                       for tp, offs in self._submitted.items())

    def pending(self, topic: str, partition: int) -> list[int]:
        with self._lock:
            return list(self._submitted.get((topic, partition), []))

    def forget(self, tps: Iterable[TP]) -> None:
        """Drop state for revoked partitions — another consumer owns them now and
        committing on their behalf would be wrong."""
        with self._lock:
            for tp in tps:
                self._submitted.pop(tp, None)
                self._done.pop(tp, None)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "partitions": len(self._submitted),
                "submitted": sum(len(v) for v in self._submitted.values()),
                "done": sum(len(v) for v in self._done.values()),
            }
