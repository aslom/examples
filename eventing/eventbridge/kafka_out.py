"""KafkaProducer wrapper — publishes request CloudEvents to the requests topic."""
from __future__ import annotations

from kafka import KafkaProducer

from shared import ce, signing


class Producer:
    """Publishes requests, and group lifecycle events onto the responses topic.

    §11: when a seed is configured, every event published here is signed under `kid`
    before serialisation, so EventRunner can refuse a request from anything that is
    not an approved submitter. `kid` rides in the JWS protected header, which is
    signed input — it cannot be swapped to impersonate another key.

    **Cost.** The Ed25519 here is pure Python (`cryptography` is a C extension and is
    banned by §1.1) and takes ~150-200 ms per signature. `publish_request` is called
    once per group member by `GroupService.submit_members`, in a loop, inside one HTTP
    request — so a 100-member batch spends ~20 s signing while the caller waits. That
    is accepted: a batch launch is an operator action, not a hot path. It is recorded
    here rather than discovered later, and a thread pool would not fix it (the GIL
    serialises pure-Python signing anyway). If signing ever becomes mandatory at
    scale, the fix is the `cryptography` dependency conversation, not concurrency.
    """

    def __init__(self, bootstrap: str, request_topic: str, source_uri: str,
                 response_topic: str | None = None,
                 seed: bytes | None = None, kid: str | None = None) -> None:
        self._prod = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=5)
        self._topic = request_topic
        self._response_topic = response_topic
        self._source = source_uri
        # Held on the instance so no call site has to know about signing.
        self._seed = seed
        self._kid = kid

    def publish_request(
        self,
        *,
        prompt: str,
        correlationid: str,
        sessionuuid: str,
        mode: str,
        model: str | None = None,
        max_turns: int = 3,
        subject: str = "agent-request",
        groupid: str | None = None,
        submitter: str | None = None,
        submitter_iss: str | None = None,
    ) -> str:
        event = ce.new_event(
            type=ce.TYPE_REQUEST,
            source=self._source,
            subject=subject,
            datacontenttype="application/json",
            correlationid=correlationid,
            sessionuuid=sessionuuid,
            mode=mode,
            data={"prompt": prompt, "model": model, "max_turns": max_turns},
            **({"groupid": groupid} if groupid else {}),
            **({ce.EXT_SUBMITTER: submitter} if submitter else {}),
            **({ce.EXT_SUBMITTER_ISS: submitter_iss} if submitter_iss else {}),
        )
        # After new_event (which fills `id` and `time`, both signed) and before
        # serialisation, so the signature covers exactly what goes on the wire.
        signing.sign_into(event, self._seed, self._kid)
        headers, value = ce.to_kafka_binary(event)
        future = self._prod.send(self._topic, key=correlationid.encode(), value=value, headers=headers)
        future.get(timeout=5)
        return event["id"]

    def publish_group_event(self, *, type_: str, groupid: str,
                            data: dict, subject: str = "group") -> str:
        """Publish a group lifecycle event to the RESPONSES topic (§21.2).

        Not requests: EventRunner consumes that topic and would treat a group event as
        an agent run to execute. Responses is also where EventBridge's own consumer and
        ntfy publisher already listen, so the event gets stored, notified and audited
        with no new plumbing.

        Signed under the same `kid` as requests, and the responses-side verifier
        accepts *only* that kid here. Skipping these instead would leave the one hole
        worth closing: a forged `group.completed` ends a batch early and fires a
        "finished" notification for work that never ran.
        """
        if not self._response_topic:
            raise RuntimeError("Producer has no response_topic; cannot publish group events")
        event = ce.new_event(
            type=type_,
            source=self._source,
            subject=subject,
            datacontenttype="application/json",
            groupid=groupid,
            data=data,
        )
        signing.sign_into(event, self._seed, self._kid)
        headers, value = ce.to_kafka_binary(event)
        self._prod.send(self._response_topic, key=groupid.encode(),
                        value=value, headers=headers).get(timeout=5)
        return event["id"]

    def close(self) -> None:
        try:
            self._prod.flush(timeout=2)
        finally:
            self._prod.close(timeout=2)
