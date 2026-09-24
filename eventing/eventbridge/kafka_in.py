"""KafkaConsumer thread — polls responses, writes SQLite, fans out to ntfy."""
from __future__ import annotations

import threading
from typing import Callable

from kafka import KafkaConsumer
from shared import ce

from eventbridge.store import Store


class Consumer(threading.Thread):
    def __init__(
        self,
        bootstrap: str,
        response_topic: str,
        store: Store,
        on_event: Callable[[dict], None] | None = None,
        group_id: str = "eventbridge-responses",
        on_group_event: Callable[[dict], None] | None = None,
        on_member_event: Callable[[dict], None] | None = None,
    ) -> None:
        super().__init__(daemon=True, name="kafka-responses-consumer")
        self._bootstrap_servers = bootstrap
        self._topic = response_topic
        self._store = store
        self._on_event = on_event
        self._on_group_event = on_group_event
        self._on_member_event = on_member_event
        self._group = group_id
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        c = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=500,
        )
        try:
            while not self._stop.is_set():
                for rec in c:
                    if self._stop.is_set():
                        break
                    evt = ce.from_kafka_binary(rec.headers or [], rec.value)
                    d = ce.envelope_dict(evt)
                    # §21.2: route on type. A group lifecycle event carries `groupid`
                    # but no `correlationid`, so handing it to insert_response would
                    # violate that table's (correlationid, sequence) primary key.
                    if ce.is_group_event(evt):
                        if self._on_group_event:
                            try: self._on_group_event(d)
                            except Exception as e: print(f"[kafka_in] group event: {e!r}")
                    else:
                        self._store.insert_response(d)
                        if self._on_member_event and d.get("groupid"):
                            try: self._on_member_event(d)
                            except Exception as e: print(f"[kafka_in] member event: {e!r}")
                    if self._on_event:
                        try: self._on_event(d)
                        except Exception: pass
        finally:
            c.close()
