"""The approved-key set, and the `kid` that selects from it.

Also covers the sign -> Kafka wire -> verify path, which had no test at all: the
existing signing tests sign and verify the same in-memory object, and the codec
tests never touch signing. So nothing checked that the signature travels as a
`ce_signature` header, that the `kid` survives, or that tampering between producer
and consumer is caught.
"""
from __future__ import annotations

import base64
import binascii
import json

import pytest

from eventrunner import signing as S
from shared import ce, keyset

# RFC 8032 test vector 1 — the same seed the signing tests use.
SEED = binascii.unhexlify(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
PUB = S.public_key(SEED)

SEED2 = binascii.unhexlify(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb")
PUB2 = S.public_key(SEED2)


def _write(tmp_path, obj) -> str:
    p = tmp_path / "agents.json"
    p.write_text(json.dumps(obj))
    return str(p)


def _event(**over):
    attrs = {"type": ce.TYPE_RESPONSE, "source": "rossoctl://eventrunner/test",
             "datacontenttype": "application/json",
             "correlationid": "brave-otter-4718",
             "sessionuuid": "8f1b6d2e-0000-5000-8000-000000000000",
             "sequence": 3, "phase": "result", "final": "true"}
    attrs.update(over)
    return ce.new_event(data={"text": "hello"}, **attrs)


# ---- loading ----------------------------------------------------------------

def test_load_accepts_hex_base64_and_base64url(tmp_path):
    ks = keyset.load(_write(tmp_path, {
        "hex": PUB.hex(),
        "b64": base64.b64encode(PUB).decode(),
        "b64u": base64.urlsafe_b64encode(PUB).decode().rstrip("="),
    }))
    assert len(ks) == 3
    assert ks.select("hex") == ks.select("b64") == ks.select("b64u") == PUB


def test_kids_are_sorted_and_loggable(tmp_path):
    ks = keyset.load(_write(tmp_path, {"z": PUB.hex(), "a": PUB2.hex()}))
    assert ks.kids == ("a", "z")


def test_membership(tmp_path):
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    assert "runner-01" in ks and "runner-99" not in ks


@pytest.mark.parametrize("bad", [
    {"k": "not-hex-at-all"},
    {"k": PUB.hex()[:40]},          # too short
    {"k": (PUB + b"\x00").hex()},   # 33 bytes
    {"k": 1234},                    # not a string
    {"": PUB.hex()},                # empty kid
])
def test_load_rejects_a_malformed_entry_rather_than_partially_loading(tmp_path, bad):
    """An authorization list that quietly dropped an entry would fail closed for a
    legitimate agent, which reads as a broken deploy rather than a control."""
    with pytest.raises(ValueError):
        keyset.load(_write(tmp_path, bad))


def test_load_rejects_a_non_object(tmp_path):
    with pytest.raises(ValueError):
        keyset.load(_write(tmp_path, ["not", "an", "object"]))


def test_load_if_set_returns_none_when_unconfigured():
    assert keyset.load_if_set(None) is None
    assert keyset.load_if_set("") is None


# ---- selection --------------------------------------------------------------

def test_select_returns_the_named_key(tmp_path):
    ks = keyset.load(_write(tmp_path, {"a": PUB.hex(), "b": PUB2.hex()}))
    assert ks.select("a") == PUB
    assert ks.select("b") == PUB2


def test_select_unknown_kid_returns_none(tmp_path):
    ks = keyset.load(_write(tmp_path, {"a": PUB.hex()}))
    assert ks.select("nope") is None


def test_select_none_kid_works_for_a_single_key_set(tmp_path):
    """A one-key deployment should not need every signer to name its key."""
    ks = keyset.load(_write(tmp_path, {"only": PUB.hex()}))
    assert ks.select(None) == PUB


def test_select_none_kid_is_ambiguous_with_several_keys(tmp_path):
    """Guessing would mean accepting a signature from ANY approved agent for an
    event that named none of them."""
    ks = keyset.load(_write(tmp_path, {"a": PUB.hex(), "b": PUB2.hex()}))
    assert ks.select(None) is None


# ---- kid in the token -------------------------------------------------------

def test_sign_event_without_kid_keeps_the_original_header():
    """Backward compatible: an unnamed token is byte-identical to before."""
    token = S.sign_event(_event(), SEED)
    header = json.loads(S._b64u_dec(token.split(".")[0]))
    assert header == {"alg": "EdDSA", "typ": "ce+jws"}
    assert S.token_kid(token) is None


def test_sign_event_with_kid_puts_it_in_the_protected_header():
    token = S.sign_event(_event(), SEED, kid="runner-01")
    header = json.loads(S._b64u_dec(token.split(".")[0]))
    assert header == {"alg": "EdDSA", "kid": "runner-01", "typ": "ce+jws"}
    assert S.token_kid(token) == "runner-01"


def test_a_kid_bearing_token_still_verifies():
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    ok, why = S.verify_signature(e, PUB)
    assert ok, why


def test_the_kid_is_covered_by_the_signature():
    """Swapping the kid to point at another approved key must not verify —
    otherwise an attacker could relabel an event as coming from someone else."""
    e = _event()
    token = S.sign_event(e, SEED, kid="runner-01")
    protected, _, sig = token.split(".")
    forged_header = S._b64u(json.dumps(
        {"alg": "EdDSA", "kid": "runner-02", "typ": "ce+jws"},
        separators=(",", ":"), sort_keys=True).encode())
    e.attrs["signature"] = f"{forged_header}..{sig}"
    ok, _ = S.verify_signature(e, PUB)
    assert not ok


@pytest.mark.parametrize("token", ["", "a.b", "not a token", "...", "a..b..c"])
def test_token_kid_tolerates_garbage(token):
    assert S.token_kid(token) is None


def test_token_kid_ignores_a_non_string_kid():
    header = S._b64u(json.dumps({"alg": "EdDSA", "kid": 7}).encode())
    assert S.token_kid(f"{header}..sig") is None


# ---- the end-to-end check: sign -> Kafka wire -> verify ---------------------

def test_signature_survives_the_kafka_binary_roundtrip():
    """The gap this file exists to close.

    Before this, nothing tested signing across the codec: the signing tests sign
    and verify the same in-memory object, and the codec tests never sign. So the
    signature travelling as a `ce_signature` header, the `kid` surviving the trip,
    and tampering between producer and consumer were all unverified.

    Type stability is the reason it works. `ce.new_event` stringifies attributes on
    construction, so `sequence` is already `"3"` when it is signed and is still
    `"3"` after `from_kafka_binary` — nothing is coerced in between. That is worth
    pinning: signing over values that changed representation on the wire would
    fail for every event, and only an end-to-end test catches it.
    """
    e = _event(sequence=3)
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, value = ce.to_kafka_binary(e)
    back = ce.from_kafka_binary(headers, value)

    assert S.token_kid(back.get("signature")) == "runner-01"
    ok, why = S.verify_signature(back, PUB)
    assert ok, why


def test_roundtrip_verification_fails_with_the_wrong_key():
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    ok, _ = S.verify_signature(back, PUB2)
    assert not ok


def test_tampering_on_the_wire_is_detected():
    """Rewrite an attribute after signing, as anything with topic write access
    could, and the signature must fail."""
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, value = ce.to_kafka_binary(e)
    headers = [(k, b"hostile-otter-0001" if k == "ce_correlationid" else v)
               for k, v in headers]
    back = ce.from_kafka_binary(headers, value)
    ok, _ = S.verify_signature(back, PUB)
    assert not ok


def test_payload_tampering_on_the_wire_is_detected():
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED, kid="runner-01")
    headers, _ = ce.to_kafka_binary(e)
    back = ce.from_kafka_binary(
        headers, json.dumps({"text": "Transfer approved."}).encode())
    ok, _ = S.verify_signature(back, PUB)
    assert not ok


def test_text_plain_payload_roundtrips():
    e = _event(datacontenttype="text/plain")
    e.data = "hello"
    e.attrs["signature"] = S.sign_event(e, SEED)
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    ok, why = S.verify_signature(back, PUB)
    assert ok, why


# ---- the two together: kid selects, then verification decides ---------------

def test_approved_agent_verifies_and_unapproved_does_not(tmp_path):
    """The intended mechanism, composed by hand: the keyset as authorization list.

    NB this wires `token_kid` -> `select` -> `verify_signature` itself, because no
    production path does yet — see the note in shared/keyset.py. It proves the
    primitives compose, not that the runner enforces anything.
    """
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))

    approved = _event()
    approved.attrs["signature"] = S.sign_event(approved, SEED, kid="runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(approved))
    pub = ks.select(S.token_kid(back.get("signature")))
    assert pub is not None
    assert S.verify_signature(back, pub)[0]

    # Same event, signed by a key nobody approved, naming a kid not in the set.
    rogue = _event()
    rogue.attrs["signature"] = S.sign_event(rogue, SEED2, kid="runner-99")
    back2 = ce.from_kafka_binary(*ce.to_kafka_binary(rogue))
    assert ks.select(S.token_kid(back2.get("signature"))) is None


def test_a_rogue_key_claiming_an_approved_kid_is_refused(tmp_path):
    """The nastier case: the attacker knows an approved kid but not its key."""
    ks = keyset.load(_write(tmp_path, {"runner-01": PUB.hex()}))
    e = _event()
    e.attrs["signature"] = S.sign_event(e, SEED2, kid="runner-01")
    back = ce.from_kafka_binary(*ce.to_kafka_binary(e))
    pub = ks.select(S.token_kid(back.get("signature")))
    assert pub == PUB                      # kid resolves...
    assert not S.verify_signature(back, pub)[0]   # ...but the signature does not
