"""Wire contract: CloudEvent binary-mode round-trip through headers+value."""
import json
import re
import uuid

from shared import ce


def test_request_event_round_trip():
    corr = "wake-brave-otter-4718"
    sess = ce.session_uuid(corr)
    evt = ce.new_event(
        type=ce.TYPE_REQUEST,
        source="rossoctl://eventbridge/local",
        subject="start",
        datacontenttype="application/json",
        correlationid=corr,
        sessionuuid=sess,
        mode="start",
        data={"prompt": "hi", "max_turns": 3},
    )
    headers, value = ce.to_kafka_binary(evt)
    hnames = {k for k, _ in headers}
    assert "ce_type" in hnames and "ce_correlationid" in hnames
    assert "ce_sessionuuid" in hnames and "ce_mode" in hnames
    # value is JSON of the data
    assert json.loads(value.decode()) == {"prompt": "hi", "max_turns": 3}

    evt2 = ce.from_kafka_binary(headers, value)
    assert evt2["type"] == ce.TYPE_REQUEST
    assert evt2["correlationid"] == corr
    assert evt2["sessionuuid"] == sess
    assert evt2["mode"] == "start"
    assert evt2.data == {"prompt": "hi", "max_turns": 3}


def test_response_event_round_trip():
    corr = "wake-brave-otter-4718"
    evt = ce.new_event(
        type=ce.TYPE_RESPONSE,
        source="rossoctl://eventrunner/local",
        datacontenttype="application/json",
        correlationid=corr,
        sessionuuid=ce.session_uuid(corr),
        sequence=7,
        phase="result",
        final="true",
        data={"final": True, "exit_code": 0, "duration_ms": 1000, "result": "OK"},
    )
    headers, value = ce.to_kafka_binary(evt)
    evt2 = ce.from_kafka_binary(headers, value)
    assert evt2["sequence"] == "7"
    assert evt2["phase"] == "result"
    assert evt2["final"] == "true"
    assert evt2.data["result"] == "OK"


def test_session_uuid_is_deterministic():
    corr = "wake-brave-otter-4718"
    a = ce.session_uuid(corr)
    b = ce.session_uuid(corr)
    assert a == b
    # must be a valid UUID (claude --session-id demands this)
    uuid.UUID(a)


def test_session_uuid_differs_per_corr():
    a = ce.session_uuid("wake-brave-otter-4718")
    b = ce.session_uuid("cold-lazy-panda-5678")
    assert a != b


def test_empty_value_decodes_to_none_data():
    evt = ce.new_event(
        type="dev.rossoctl.test.v1", source="test",
        datacontenttype="application/json", correlationid="x-y-0001",
    )
    headers, value = ce.to_kafka_binary(evt)
    assert value == b""
    evt2 = ce.from_kafka_binary(headers, value)
    assert evt2.data is None


def test_every_extension_attribute_name_is_cloudevents_compliant():
    """CloudEvents v1.0: attribute names MUST be lower-case [a-z0-9] only.

    No underscore, hyphen, dot or upper case. The spec restricts the set because
    an event traverses several hops and some protocols treat metadata as
    case-sensitive while others do not.

    This codec cannot catch a violation on its own: `to_kafka_binary` and
    `from_kafka_binary` only add and strip the `ce_` prefix, so a bad name
    round-trips happily here and is rejected — or silently dropped — by a
    spec-compliant SDK, an HTTP-binding gateway or a Knative broker further
    along. `submitter_iss` shipped in review for exactly that reason. Hence a
    test rather than a comment.
    """
    ext_names = [v for k, v in vars(ce).items()
                 if k.startswith("EXT_") and isinstance(v, str)]
    assert ext_names, "no EXT_* constants found — has ce.py been restructured?"
    bad = [n for n in ext_names if not re.fullmatch(r"[a-z][a-z0-9]*", n)]
    assert not bad, f"not CloudEvents-compliant attribute names: {bad}"


def test_extension_names_are_terse_enough_to_survive_a_gateway():
    """The spec SHOULD-limit is 20 characters. Not a hard failure upstream, but
    a name over it is a smell worth catching while renaming is still free."""
    long = [v for k, v in vars(ce).items()
            if k.startswith("EXT_") and isinstance(v, str) and len(v) > 20]
    assert not long, f"extension names over the 20-char SHOULD limit: {long}"
