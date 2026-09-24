"""Ed25519 + CloudEvent canonicalization. DESIGN_PHASE1.md §11.

`cryptography` is a C extension and banned by §1.1, so Ed25519 is implemented in
pure Python from RFC 8032. That is only defensible if it is checked against the
RFC's own vectors, which is what the first tests here do.

The canonicalization tests matter just as much: §11 says outright that signer and
verifier agreeing byte-for-byte "is the part that will bite".
"""
import binascii
import json

import pytest
from eventrunner import signing as S
from shared import ce

# ---- RFC 8032 §7.1 test vectors --------------------------------------------

RFC_VECTORS = [
    # (seed, public key, message, signature)
    ("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
     "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
     "",
     "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555"
     "fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"),
    ("4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
     "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c",
     "72",
     "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da"
     "085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"),
    ("c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7",
     "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025",
     "af82",
     "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac"
     "18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a"),
]


@pytest.mark.parametrize("seed_hex,pub_hex,msg_hex,sig_hex", RFC_VECTORS)
def test_rfc8032_vectors(seed_hex, pub_hex, msg_hex, sig_hex):
    seed = binascii.unhexlify(seed_hex)
    pub = binascii.unhexlify(pub_hex)
    msg = binascii.unhexlify(msg_hex)
    sig = binascii.unhexlify(sig_hex)
    assert S.public_key(seed) == pub, "public key derivation"
    assert S.sign(msg, seed) == sig, "signature is deterministic and matches the RFC"
    assert S.verify(msg, sig, pub) is True


def test_verify_rejects_a_tampered_message():
    seed = binascii.unhexlify(RFC_VECTORS[1][0])
    pub = S.public_key(seed)
    sig = S.sign(b"original", seed)
    assert S.verify(b"original", sig, pub) is True
    assert S.verify(b"tampered", sig, pub) is False


def test_verify_rejects_a_tampered_signature():
    seed = binascii.unhexlify(RFC_VECTORS[1][0])
    pub = S.public_key(seed)
    sig = bytearray(S.sign(b"msg", seed))
    sig[0] ^= 0x01
    assert S.verify(b"msg", bytes(sig), pub) is False


def test_verify_rejects_the_wrong_public_key():
    a = binascii.unhexlify(RFC_VECTORS[0][0])
    b = binascii.unhexlify(RFC_VECTORS[1][0])
    assert S.verify(b"msg", S.sign(b"msg", a), S.public_key(b)) is False


def test_verify_returns_false_for_malformed_input_rather_than_raising():
    pub = S.public_key(binascii.unhexlify(RFC_VECTORS[0][0]))
    assert S.verify(b"m", b"too-short", pub) is False
    assert S.verify(b"m", b"\x00" * 64, b"short-key") is False


def test_seed_must_be_32_bytes():
    with pytest.raises(ValueError, match="32 bytes"):
        S.public_key(b"\x01" * 31)


# ---- canonicalization -------------------------------------------------------

def _event(**over):
    attrs = {
        "specversion": "1.0", "type": ce.TYPE_REQUEST,
        "source": "rossoctl://eventbridge/test", "id": "evt-1",
        "time": "2026-09-22T12:00:00.000Z", "correlationid": "brave-otter-4718",
        "sessionuuid": ce.session_uuid("brave-otter-4718"),
        "mode": "start", "datacontenttype": "application/json",
    }
    attrs.update(over)
    return ce.CloudEvent(attrs=attrs, data={"prompt": "hello", "max_turns": 1})


def test_canonical_is_independent_of_attribute_order():
    e1 = _event()
    e2 = ce.CloudEvent(attrs=dict(reversed(list(e1.attrs.items()))), data=e1.data)
    assert S.canonical(e1.attrs, e1.data) == S.canonical(e2.attrs, e2.data)


def test_canonical_omits_absent_attributes_rather_than_writing_them_empty():
    canon = S.canonical(_event().attrs, None).decode()
    assert "sequence=" not in canon, "an absent attribute must not appear at all"
    assert "correlationid=brave-otter-4718" in canon


def test_canonical_binds_the_payload_by_digest():
    e = _event()
    with_payload = S.canonical(e.attrs, e.data)
    changed = S.canonical(e.attrs, {"prompt": "hello", "max_turns": 2})
    assert with_payload != changed, "changing data must change the canonical form"
    assert b"datadigest=sha256:" in with_payload


def test_canonical_changes_when_any_signed_attribute_changes():
    base = S.canonical(_event().attrs, None)
    for attr in ("id", "correlationid", "sessionuuid", "mode", "phase", "causationid"):
        other = S.canonical(_event(**{attr: "different"}).attrs, None)
        assert other != base, f"{attr} is in SIGNED_ATTRS but did not affect the digest"


def test_canonical_is_stable_across_calls():
    e = _event()
    assert S.canonical(e.attrs, e.data) == S.canonical(e.attrs, e.data)


# ---- detached JWS over an event ---------------------------------------------

def test_sign_and_verify_an_event_round_trip():
    seed = binascii.unhexlify(RFC_VECTORS[0][0])
    e = _event()
    e.attrs["signature"] = S.sign_event(e, seed)
    ok, why = S.verify_signature(e, S.public_key(seed))
    assert ok, why


def test_the_jws_is_detached():
    """Compact serialization with an empty payload segment (RFC 7515 App. F):
    the payload is the event itself, so carrying it again would double the size."""
    seed = binascii.unhexlify(RFC_VECTORS[0][0])
    token = S.sign_event(_event(), seed)
    parts = token.split(".")
    assert len(parts) == 3 and parts[1] == ""
    header = json.loads(S._b64u_dec(parts[0]))
    assert header == {"alg": "EdDSA", "typ": "ce+jws"}


def test_verification_fails_when_an_attribute_is_tampered_with():
    seed = binascii.unhexlify(RFC_VECTORS[0][0])
    e = _event()
    e.attrs["signature"] = S.sign_event(e, seed)
    e.attrs["correlationid"] = "attacker-chosen-corr"
    ok, why = S.verify_signature(e, S.public_key(seed))
    assert not ok and "does not verify" in why


def test_verification_fails_when_the_payload_is_tampered_with():
    seed = binascii.unhexlify(RFC_VECTORS[0][0])
    e = _event()
    e.attrs["signature"] = S.sign_event(e, seed)
    e.data = {"prompt": "rm -rf /", "max_turns": 1}
    ok, why = S.verify_signature(e, S.public_key(seed))
    assert not ok


def test_missing_signature_is_reported_not_accepted():
    ok, why = S.verify_signature(_event(), S.public_key(b"\x02" * 32))
    assert not ok and "no ce_signature" in why


def test_alg_confusion_is_rejected():
    """A `none`/HS256 header must not be accepted just because it parses."""
    seed = binascii.unhexlify(RFC_VECTORS[0][0])
    e = _event()
    bad_header = S._b64u(json.dumps({"alg": "none", "typ": "ce+jws"},
                                    separators=(",", ":"), sort_keys=True).encode())
    e.attrs["signature"] = f"{bad_header}..{S._b64u(b'x' * 64)}"
    ok, why = S.verify_signature(e, S.public_key(seed))
    assert not ok and "alg" in why


def test_non_detached_token_is_rejected():
    seed = binascii.unhexlify(RFC_VECTORS[0][0])
    e = _event()
    token = S.sign_event(e, seed)
    protected, _, sig = token.split(".")
    e.attrs["signature"] = f"{protected}.{S._b64u(b'smuggled')}.{sig}"
    ok, why = S.verify_signature(e, S.public_key(seed))
    assert not ok and "detached" in why


# ---- key loading ------------------------------------------------------------

def test_load_seed_accepts_hex_base64_and_raw(tmp_path):
    import base64
    seed = binascii.unhexlify(RFC_VECTORS[0][0])
    hex_f = tmp_path / "hex"
    hex_f.write_text(seed.hex() + "\n")
    b64_f = tmp_path / "b64"
    b64_f.write_text(base64.b64encode(seed).decode() + "\n")
    raw_f = tmp_path / "raw"
    raw_f.write_bytes(seed)
    assert S.load_seed(hex_f) == seed
    assert S.load_seed(b64_f) == seed
    assert S.load_seed(raw_f) == seed


def test_load_seed_rejects_a_wrong_length_key(tmp_path):
    f = tmp_path / "bad"
    f.write_text("deadbeef\n")
    with pytest.raises(ValueError, match="32-byte"):
        S.load_seed(f)


def test_verify_event_fails_loudly_when_no_key_is_configured():
    from eventrunner.config import Cfg
    ok, why = S.verify_event(_event(), Cfg())
    assert not ok and "ER_VERIFY_KEY_PATH" in why


def test_verify_event_uses_the_configured_key(tmp_path):
    from eventrunner.config import Cfg
    seed = binascii.unhexlify(RFC_VECTORS[2][0])
    key = tmp_path / "seed.hex"
    key.write_text(seed.hex())
    cfg = Cfg(verify_key_path=str(key))
    e = _event()
    e.attrs["signature"] = S.sign_event(e, seed)
    ok, why = S.verify_event(e, cfg)
    assert ok, why
