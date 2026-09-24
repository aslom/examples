"""KafkaConsumer that mirrors every request CloudEvent's prompt into
`prompts.sqlite`. This is the safety net that back-fills prompts for
correlations created before the prompts table existed, and also decouples
prompt persistence from EventBridge's HTTP path — if someone POSTs to
Kafka directly (or a Phase 1 signed-event producer does), we still see
the prompt.

The mirror reads from the requests topic with a dedicated consumer group
(`eventbridge-requests-mirror`) starting at `earliest` so it can pick up
whatever Kafka still retains. It uses `Store.backfill_prompt_if_missing`
so re-runs are idempotent — the same (correlationid, mode, prompt) is
never inserted twice.
"""
from __future__ import annotations

import json
import os
import threading

from kafka import KafkaConsumer
from shared import ce

from eventbridge.store import Store


class RequestsMirror(threading.Thread):
    def __init__(self, bootstrap: str, request_topic: str, store: Store,
                 group_id: str | None = None) -> None:
        super().__init__(daemon=True, name="kafka-requests-mirror")
        self._bootstrap_servers = bootstrap
        self._topic = request_topic
        self._store = store
        # Per-process group id so every EB start re-scans the whole topic.
        # `backfill_prompt_if_missing()` makes re-inserts a no-op, so this
        # is idempotent and cheap. A fixed group id would commit offsets
        # once and then never re-see the old events on the next start.
        self._group = group_id or f"eventbridge-requests-mirror-{os.getpid()}"
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        c = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,      # don't persist offsets — we re-scan every start
            consumer_timeout_ms=500,
        )
        try:
            while not self._stop.is_set():
                for rec in c:
                    if self._stop.is_set():
                        break
                    try:
                        evt = ce.from_kafka_binary(rec.headers or [], rec.value)
                    except Exception as e:  # noqa: BLE001
                        print(f"[requests-mirror] parse fail at offset={rec.offset}: {e}")
                        continue
                    corr = evt.attrs.get("correlationid")
                    if not corr:
                        continue
                    mode = evt.attrs.get("mode", "start") or "start"
                    data = evt.data
                    if isinstance(data, (bytes, bytearray)):
                        try: data = json.loads(data.decode())
                        except Exception: data = None
                    if isinstance(data, str):
                        try: data = json.loads(data)
                        except Exception: data = None
                    prompt = (data.get("prompt") if isinstance(data, dict) else None)
                    if not prompt:
                        continue
                    submitted = evt.attrs.get("time")
                    self._store.backfill_prompt_if_missing(
                        correlationid=corr, mode=mode, prompt=prompt,
                        submitted_utc=submitted,
                    )
        finally:
            c.close()
