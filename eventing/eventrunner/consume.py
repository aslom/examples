"""KafkaConsumer loop for the requests topic. Hands off to Router.

Phase 1 rewrote this module. Phase 0's version built its `KafkaConsumer` eagerly
inside `run()` with no retry and committed each offset immediately after handing
the event to the Router. Both are wrong in Kubernetes:

  * §8.1 — pod start order is not guaranteed. KEDA may start a pod while the
    broker is rolling; construction raises, the consumer **thread** dies, and the
    main thread keeps running. The pod stays `Running`, reports healthy, consumes
    nothing, and the demo hangs with no error anywhere. So: bounded-backoff retry
    around construction, and the loop body is wrapped so no exception can end the
    thread — it reconnects instead.
  * RQ-1 — committing on receipt makes lag hit zero while the agent is still
    streaming, and KEDA then scales the pod away mid-run. So commits are
    deferred until the terminal event, via OffsetLedger.
  * §8.4 — the loop touches a heartbeat file every poll so an `exec` liveness
    probe can see a wedged thread that a process check cannot.
  * §16 Gap C — requests older than `max_request_age_s` are dropped rather than
    re-run, so a group whose offsets expired after a week idle does not replay a
    day of agent work.
"""
from __future__ import annotations

import sys
import threading
import time
import traceback

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata

from eventrunner.config import Cfg
from eventrunner.offsets import OffsetLedger
from eventrunner.router import Router
from shared import ce
from shared.heartbeat import Heartbeat


def _log(msg: str) -> None:
    print(f"[consume] {msg}", flush=True)


def _elog(msg: str) -> None:
    # stderr for anything a human debugging a pod needs to see next to a traceback
    print(f"[consume] {msg}", file=sys.stderr, flush=True)


def event_age_s(event, *, now: float | None = None) -> float | None:
    """Seconds since the request event's `time` attribute, or None if unparseable."""
    raw = (event.get("time") or "").strip()
    if not raw:
        return None
    try:
        import datetime as dt
        ts = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        return (now if now is not None else time.time()) - ts.timestamp()
    except (ValueError, TypeError):
        return None


class Consumer(threading.Thread):
    """Long-lived requests-topic consumer. Reconnects forever; never dies."""

    def __init__(self, cfg: Cfg, router: Router, *,
                 heartbeat: Heartbeat | None = None,
                 ledger: OffsetLedger | None = None,
                 consumer_factory=None) -> None:
        super().__init__(daemon=True, name="kafka-requests-consumer")
        self._cfg = cfg
        self._router = router
        self._topic = cfg.request_topic
        self._stop = threading.Event()
        # Intake and shutdown are separate signals. On SIGTERM we want to stop
        # taking NEW work immediately but keep polling, because the poll loop is
        # what commits offsets as the in-flight runs finish draining (§8.3).
        self._intake_open = threading.Event()
        self._intake_open.set()
        self._hb = heartbeat or Heartbeat(cfg.heartbeat_path)
        self._ledger = ledger or OffsetLedger()
        # Injectable for tests: the retry-loop gate needs a factory that raises.
        self._factory = consumer_factory or self._build_consumer
        self._seeded: set = set()
        self._connect_attempts = 0
        self._skipped_stale = 0
        self._paused = False

    # ---- lifecycle ----
    def stop(self) -> None:
        self._stop.set()

    def stop_intake(self) -> None:
        """Stop accepting new records; keep polling so commits still flow."""
        self._intake_open.clear()

    @property
    def ledger(self) -> OffsetLedger:
        return self._ledger

    @property
    def skipped_stale(self) -> int:
        return self._skipped_stale

    def _build_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            bootstrap_servers=self._cfg.kafka_bootstrap,
            group_id=self._cfg.consumer_group,
            enable_auto_commit=False,        # §7.1: scale-to-zero needs explicit
            auto_offset_reset="earliest",    # must match the trigger's offsetResetPolicy
            consumer_timeout_ms=0,
        )

    # ---- §8.1: construction retry -------------------------------------------
    def _connect_with_retry(self) -> KafkaConsumer | None:
        """Build and subscribe, retrying for the life of the pod.

        Returns None only when asked to stop. Every failure is logged with the
        attempt number so `kubectl logs` shows a broker that never came up as a
        countdown rather than as silence.
        """
        delay = self._cfg.kafka_retry_initial_s
        while not self._stop.is_set():
            self._connect_attempts += 1
            try:
                c = self._factory()
                c.subscribe([self._topic])
                _log(f"connected to {self._cfg.kafka_bootstrap} "
                     f"group={self._cfg.consumer_group} topic={self._topic} "
                     f"(attempt {self._connect_attempts})")
                return c
            except Exception as e:  # noqa: BLE001 - any broker/DNS/topic error retries
                _elog(f"kafka connect attempt {self._connect_attempts} failed: "
                      f"{type(e).__name__}: {e}; retrying in {delay:.1f}s")
                # Keep the heartbeat fresh while retrying: a pod waiting for a
                # rolling broker is alive and must not be restarted for it.
                self._hb.touch()
                if self._stop.wait(timeout=delay):
                    return None
                delay = min(delay * 2, self._cfg.kafka_retry_max_s)
        return None

    def run(self) -> None:
        """Outer supervisor: the thread must survive anything the loop throws."""
        self._hb.touch()
        while not self._stop.is_set():
            c = self._connect_with_retry()
            if c is None:
                break
            try:
                self._consume_loop(c)
            except Exception:  # noqa: BLE001
                # The Phase 0 bug was exactly this path being absent. Log the
                # trace and reconnect rather than letting the thread end.
                _elog("consume loop raised; reconnecting:\n" + traceback.format_exc())
                self._hb.touch()
                self._stop.wait(timeout=self._cfg.kafka_retry_initial_s)
            finally:
                try:
                    c.close()
                except Exception:  # noqa: BLE001
                    pass
        _log("consumer thread exiting")

    # ---- the loop -----------------------------------------------------------
    def _consume_loop(self, c: KafkaConsumer) -> None:
        cap = max(1, self._cfg.max_concurrent) * 2
        while not self._stop.is_set():
            self._apply_backpressure(c, cap)
            batch = c.poll(timeout_ms=500)
            self._hb.touch()
            self._seed_offsets(c, batch)
            for tp, records in (batch or {}).items():
                for rec in records:
                    if self._stop.is_set() or not self._intake_open.is_set():
                        break
                    self._handle(rec)
            self._commit_ready(c)

        # Draining: the caller stopped us, but runs may still be in flight. Give
        # them the chance to complete and commit so their work is not redelivered.
        self._final_commit(c)

    def _apply_backpressure(self, c: KafkaConsumer, cap: int) -> None:
        """Stop fetching while too much work is in flight.

        Bounds memory, and keeps the un-fetched messages visible to KEDA as lag
        rather than buffering them invisibly inside this pod.
        """
        try:
            assignment = c.assignment()
            if not assignment:
                return
            draining = not self._intake_open.is_set()
            busy = draining or self._ledger.in_flight() >= cap
            if busy and not self._paused:
                c.pause(*assignment)
                self._paused = True
                _log("pausing fetch: draining" if draining else
                     f"pausing fetch: {self._ledger.in_flight()} run(s) in flight (cap {cap})")
            elif not busy and self._paused:
                c.resume(*assignment)
                self._paused = False
                _log("resuming fetch")
        except Exception as e:  # noqa: BLE001
            _elog(f"backpressure adjustment failed (continuing): {e}")

    # ---- §16 Gap C / T1.6: the group always has a committed offset -----------
    def _seed_offsets(self, c: KafkaConsumer, batch: dict | None) -> None:
        """Commit a starting offset for any newly assigned partition that has none.

        A group with zero members for longer than the broker's
        `offsets.retention.minutes` (7 days here) has its offsets deleted. Making
        sure an offset exists from the first poll onward means every wake inside
        that window resumes from a real position instead of falling back to
        `earliest`.
        """
        try:
            assignment = c.assignment()
        except Exception:  # noqa: BLE001
            return
        for tp in assignment:
            if tp in self._seeded:
                continue
            try:
                if c.committed(tp) is None:
                    recs = (batch or {}).get(tp) or []
                    # If this poll already fetched records, position() is past
                    # them — the true starting offset is the first record's.
                    start = recs[0].offset if recs else c.position(tp)
                    c.commit({tp: OffsetAndMetadata(start, "", -1)})
                    _log(f"seeded starting offset for {tp.topic}/{tp.partition} "
                         f"at {start} (group had none)")
                self._seeded.add(tp)
            except Exception as e:  # noqa: BLE001
                _elog(f"could not seed offset for {tp}: {e}")

    # ---- one record ---------------------------------------------------------
    def _handle(self, rec) -> None:
        topic, partition, offset = rec.topic, rec.partition, rec.offset
        try:
            evt = ce.from_kafka_binary(rec.headers or [], rec.value)
        except Exception as e:  # noqa: BLE001
            _elog(f"undecodable record at {topic}/{partition}/{offset}: {e}; skipping")
            self._skip(topic, partition, offset)
            return

        if "correlationid" not in evt.attrs:
            _log(f"skipping malformed event (no ce_correlationid) at offset={offset}")
            self._skip(topic, partition, offset)
            return

        corr = evt["correlationid"]

        # Gap C guard. Checked before signature verification because a replayed
        # day-old event is not an attack, just history.
        age = event_age_s(evt)
        if self._cfg.max_request_age_s > 0 and age is not None \
                and age > self._cfg.max_request_age_s:
            self._skipped_stale += 1
            _log(f"dropping stale request corr={corr} age={age:.0f}s "
                 f"(> ER_MAX_REQUEST_AGE_S={self._cfg.max_request_age_s:.0f}s) — "
                 f"offset committed, agent NOT re-run")
            self._skip(topic, partition, offset)
            return

        if self._cfg.require_signature:
            from eventrunner import signing
            ok, why = signing.verify_event(evt, self._cfg)
            if not ok:
                _elog(f"rejecting unsigned/badly-signed request corr={corr}: {why} "
                      f"(ER_REQUIRE_SIGNATURE=true) — offset committed, not retried")
                self._skip(topic, partition, offset)
                return

        self._ledger.submit(topic, partition, offset)

        def _done() -> None:
            self._ledger.complete(topic, partition, offset)

        if self._cfg.commit_after_terminal:
            self._router.submit(evt, on_done=_done)
        else:
            # Phase 0 behaviour, kept only so the difference is testable.
            self._router.submit(evt)
            _done()

    def _skip(self, topic: str, partition: int, offset: int) -> None:
        """Record a message we will not process, so its offset still advances."""
        self._ledger.submit(topic, partition, offset)
        self._ledger.complete(topic, partition, offset)

    # ---- commits ------------------------------------------------------------
    def _commit_ready(self, c: KafkaConsumer) -> None:
        ready = self._ledger.committable()
        if not ready:
            return
        payload = {tp: OffsetAndMetadata(off, "", -1) for tp, off in
                   ((_tp(t, p), o) for (t, p), o in ready.items())}
        try:
            c.commit(payload)
        except Exception as e:  # noqa: BLE001
            # Leave the ledger intact so the next poll retries the same commit.
            _elog(f"commit failed (will retry next poll): {e}")
            return
        self._ledger.take_committable()

    def _final_commit(self, c: KafkaConsumer) -> None:
        """Last chance to commit completed work during shutdown."""
        deadline = time.monotonic() + self._cfg.final_commit_wait_s
        while time.monotonic() < deadline:
            if self._ledger.in_flight() == 0:
                break
            time.sleep(0.2)
        try:
            self._commit_ready(c)
        except Exception as e:  # noqa: BLE001
            _elog(f"final commit failed: {e}")


def _tp(topic: str, partition: int):
    from kafka.structs import TopicPartition
    return TopicPartition(topic, partition)
