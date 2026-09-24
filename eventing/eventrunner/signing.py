"""Detached JWS over the CloudEvent envelope. DESIGN_PHASE1.md §11.

Feature-flagged and **disabled by default**, so the e2e path is unaffected and
signing can be enabled independently of causation binding.

Two design constraints shape this:

* **Canonicalization must be byte-identical between signer and verifier.** That
  is the part that bites. Rather than depend on JCS (a new dependency, which §1.1
  forbids), the canonical form is sorted `key=value` lines over a fixed signed
  attribute set plus `sha256(data-bytes)`. It is unambiguous, trivially
  reimplementable in another language, and diffable when it disagrees.
* **Pure Python.** `cryptography` is a C extension and is banned. Ed25519 is
  implemented here from RFC 8032 using only `hashlib` — about 70 lines, and
  verified against the RFC's own test vectors in `tests/test_signing.py`.

Key handling: an Ed25519 seed (32 bytes) in a read-only Secret, hex or base64
encoded. `ER_REQUIRE_SIGNATURE=true` makes EventRunner refuse unsigned or
badly-signed requests — logging and committing the offset rather than retrying
forever, because a bad signature will still be bad on redelivery.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import pathlib
from typing import Any

# ---- Ed25519 (RFC 8032), pure Python ---------------------------------------

_P = 2 ** 255 - 19
_L = 2 ** 252 + 27742317777372353535851937790883648493
_D = -121665 * pow(121666, _P - 2, _P) % _P
_I = pow(2, (_P - 1) // 4, _P)


def _sha512(b: bytes) -> bytes:
    return hashlib.sha512(b).digest()


def _sha512_int(b: bytes) -> int:
    return int.from_bytes(_sha512(b), "little")


def _x_recover(y: int) -> int:
    xx = (y * y - 1) * pow(_D * y * y + 1, _P - 2, _P)
    x = pow(xx, (_P + 3) // 8, _P)
    if (x * x - xx) % _P != 0:
        x = (x * _I) % _P
    if (x * x - xx) % _P != 0:
        raise ValueError("point is not on the curve")
    if x % 2 != 0:
        x = _P - x
    return x


def _edwards_add(p: tuple[int, int], q: tuple[int, int]) -> tuple[int, int]:
    x1, y1 = p
    x2, y2 = q
    k = _D * x1 * x2 * y1 * y2
    x3 = (x1 * y2 + x2 * y1) * pow(1 + k, _P - 2, _P)
    y3 = (y1 * y2 + x1 * x2) * pow(1 - k, _P - 2, _P)
    return x3 % _P, y3 % _P


def _scalar_mult(p: tuple[int, int], e: int) -> tuple[int, int]:
    if e == 0:
        return (0, 1)
    q = _scalar_mult(p, e // 2)
    q = _edwards_add(q, q)
    if e & 1:
        q = _edwards_add(q, p)
    return q


# The standard base point: y = 4/5 mod p, x recovered from it.
_BASE_Y = 4 * pow(5, _P - 2, _P) % _P
_BASE = (_x_recover(_BASE_Y), _BASE_Y)


def _encode_point(p: tuple[int, int]) -> bytes:
    x, y = p
    return ((y | ((x & 1) << 255)).to_bytes(32, "little"))


def _decode_point(b: bytes) -> tuple[int, int]:
    if len(b) != 32:
        raise ValueError("an Ed25519 point is 32 bytes")
    i = int.from_bytes(b, "little")
    y = i & ((1 << 255) - 1)
    sign = i >> 255
    x = _x_recover(y)
    if x & 1 != sign:
        x = _P - x
    return (x, y)


def _secret_scalar(seed: bytes) -> tuple[int, bytes]:
    h = _sha512(seed)
    a = int.from_bytes(h[:32], "little")
    a &= (1 << 254) - 8       # clamp per RFC 8032 §5.1.5
    a |= (1 << 254)
    return a, h[32:]


def public_key(seed: bytes) -> bytes:
    """Derive the 32-byte Ed25519 public key from a 32-byte seed."""
    if len(seed) != 32:
        raise ValueError(f"an Ed25519 seed is 32 bytes, got {len(seed)}")
    a, _ = _secret_scalar(seed)
    return _encode_point(_scalar_mult(_BASE, a))


def sign(message: bytes, seed: bytes) -> bytes:
    """64-byte Ed25519 signature over `message`."""
    a, prefix = _secret_scalar(seed)
    pub = _encode_point(_scalar_mult(_BASE, a))
    r = _sha512_int(prefix + message) % _L
    big_r = _scalar_mult(_BASE, r)
    enc_r = _encode_point(big_r)
    k = _sha512_int(enc_r + pub + message) % _L
    s = (r + k * a) % _L
    return enc_r + s.to_bytes(32, "little")


def verify(message: bytes, signature: bytes, pub: bytes) -> bool:
    """Constant-time-ish Ed25519 verification. False rather than raising."""
    try:
        if len(signature) != 64 or len(pub) != 32:
            return False
        big_r = _decode_point(signature[:32])
        a_point = _decode_point(pub)
        s = int.from_bytes(signature[32:], "little")
        if s >= _L:
            return False
        k = _sha512_int(signature[:32] + pub + message) % _L
        lhs = _scalar_mult(_BASE, s)
        rhs = _edwards_add(big_r, _scalar_mult(a_point, k))
        return lhs == rhs
    except (ValueError, OverflowError):
        return False


# ---- canonicalization -------------------------------------------------------

# The attribute set covered by the signature. Fixed and explicit: an attacker
# must not be able to shrink the signed set by omitting an attribute, and a
# verifier must not accept a signature that covered less than it thinks.
SIGNED_ATTRS = ("specversion", "type", "source", "id", "time", "subject",
                "datacontenttype", "correlationid", "sessionuuid", "sequence",
                "phase", "final", "mode", "causationid")


def data_bytes(data: Any) -> bytes:
    """The exact bytes that ride the Kafka value, so signer and verifier hash the
    same thing. Mirrors `shared.ce.to_kafka_binary`."""
    if data is None:
        return b""
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def canonical(attrs: dict[str, Any], data: Any) -> bytes:
    """Sorted `key=value` lines over SIGNED_ATTRS, plus a digest of the payload.

    Absent attributes are omitted rather than written empty, and the line set is
    sorted, so the encoding is independent of dict order. `datadigest` binds the
    payload without embedding it, which keeps the signed blob small — the point of
    pairing signing with `ER_INCLUDE_RAW=false` (§8.9).
    """
    lines = [f"{k}={attrs[k]}" for k in SIGNED_ATTRS
             if attrs.get(k) not in (None, "")]
    lines.append("datadigest=sha256:" + hashlib.sha256(data_bytes(data)).hexdigest())
    return "\n".join(sorted(lines)).encode("utf-8")


def _b64u(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode().rstrip("=")


def _b64u_dec(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def sign_event(event, seed: bytes) -> str:
    """Return the detached-JWS value for `ce_signature`.

    Detached: the payload is not carried inside the JWS (it is the event itself),
    so the compact serialization has an empty middle segment —
    `<protected>..<signature>`, as RFC 7515 Appendix F describes.
    """
    protected = _b64u(json.dumps({"alg": "EdDSA", "typ": "ce+jws"},
                                 separators=(",", ":"), sort_keys=True).encode())
    payload = _b64u(canonical(event.attrs, event.data))
    sig = sign(f"{protected}.{payload}".encode(), seed)
    return f"{protected}..{_b64u(sig)}"


def verify_signature(event, pub: bytes) -> tuple[bool, str]:
    """(ok, reason). Recomputes the canonical form locally — never trusts one
    supplied in the event."""
    token = event.get("signature")
    if not token:
        return False, "no ce_signature attribute"
    parts = token.split(".")
    if len(parts) != 3 or parts[1] != "":
        return False, f"not a detached JWS ({len(parts)} segments)"
    protected, _, sig_b64 = parts
    try:
        header = json.loads(_b64u_dec(protected))
    except (ValueError, binascii.Error):
        return False, "undecodable JWS header"
    if header.get("alg") != "EdDSA":
        return False, f"unexpected alg {header.get('alg')!r} (only EdDSA is accepted)"
    payload = _b64u(canonical(event.attrs, event.data))
    try:
        sig = _b64u_dec(sig_b64)
    except binascii.Error:
        return False, "undecodable signature segment"
    if not verify(f"{protected}.{payload}".encode(), sig, pub):
        return False, "signature does not verify over the canonical attributes"
    return True, "ok"


# ---- key loading ------------------------------------------------------------

def load_seed(path: str | pathlib.Path) -> bytes:
    """Read a 32-byte Ed25519 seed from a Secret-mounted file (hex, base64 or raw)."""
    raw = pathlib.Path(path).read_bytes().strip()
    if len(raw) == 32:
        return bytes(raw)
    text = raw.decode(errors="strict").strip()
    for decode in (bytes.fromhex, lambda s: base64.b64decode(s, validate=True),
                   _b64u_dec):
        try:
            out = decode(text)
        except Exception:  # noqa: BLE001
            continue
        if len(out) == 32:
            return out
    raise ValueError(f"{path}: not a 32-byte Ed25519 seed (hex, base64 or raw)")


def verify_event(event, cfg) -> tuple[bool, str]:
    """Verify using the key configured on the runner. Used by consume.py when
    ER_REQUIRE_SIGNATURE=true."""
    key_path = cfg.verify_key_path or cfg.signing_key_path
    if not key_path:
        return False, ("ER_REQUIRE_SIGNATURE=true but neither ER_VERIFY_KEY_PATH "
                       "nor ER_SIGNING_KEY_PATH is set")
    try:
        seed_or_pub = pathlib.Path(key_path).read_bytes().strip()
        pub = (bytes(seed_or_pub) if len(seed_or_pub) == 32
               else public_key(load_seed(key_path)))
    except Exception as e:  # noqa: BLE001
        return False, f"cannot load verification key from {key_path}: {e}"
    # A 32-byte file is ambiguous between seed and public key; try both.
    ok, why = verify_signature(event, pub)
    if ok:
        return True, why
    try:
        ok2, why2 = verify_signature(event, public_key(load_seed(key_path)))
        if ok2:
            return True, why2
    except Exception:  # noqa: BLE001
        pass
    return False, why
