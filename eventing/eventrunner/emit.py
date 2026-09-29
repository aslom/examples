"""Emit response CloudEvents on the responses topic. Single Producer instance."""
from __future__ import annotations

import threading
from typing import Any

from kafka import KafkaProducer

from shared import ce


class Emitter:
    def __init__(self, bootstrap: str, response_topic: str, source_uri: str) -> None:
        self._prod = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=5)
        self._topic = response_topic
        self._source = source_uri
        self._seq_lock = threading.Lock()
        self._seq_by_corr: dict[str, int] = {}

    def next_seq(self, corr: str, start: int | None = None) -> int:
        with self._seq_lock:
            if corr not in self._seq_by_corr and start is not None:
                self._seq_by_corr[corr] = start - 1
            self._seq_by_corr[corr] = self._seq_by_corr.get(corr, 0) + 1
            return self._seq_by_corr[corr]

    def seed_seq(self, corr: str, last: int) -> None:
        with self._seq_lock:
            self._seq_by_corr[corr] = max(last, self._seq_by_corr.get(corr, 0))

    def emit(self, *, correlationid: str, sessionuuid: str, sequence: int,
             phase: str, final: bool, data: Any,
             causationid: str | None = None, groupid: str | None = None) -> str:
        """Publish one response event.

        `causationid` (DESIGN_PHASE1.md §11) is the `id` of the request event that
        caused this one. Without it the only link is `correlationid`, which
        identifies a *conversation*, not a *turn* — so responses from a
        `/continue` turn could not be attributed to their specific triggering
        request, which is what makes a signed audit trail meaningful.
        """
        attrs: dict[str, Any] = {}
        if causationid:
            attrs["causationid"] = causationid
        if groupid:
            attrs["groupid"] = groupid
        event = ce.new_event(
            type=ce.TYPE_RESPONSE,
            source=self._source,
            datacontenttype="application/json",
            correlationid=correlationid,
            sessionuuid=sessionuuid,
            sequence=sequence,
            phase=phase,
            final="true" if final else "false",
            data=data,
            **attrs,
        )
        headers, value = ce.to_kafka_binary(event)
        self._prod.send(self._topic,
                        key=correlationid.encode(),
                        value=value,
                        headers=headers).get(timeout=5)
        return event["id"]

    def close(self) -> None:
        try:
            self._prod.flush(timeout=2)
        finally:
            self._prod.close(timeout=2)
