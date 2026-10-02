"""The approved-key set: `kid` -> Ed25519 public key. Stdlib only.

`signing.verify_with_keyset` consults this module, so this file **is** the
authorization list: a signed event whose `kid` is not in the set is refused, and so is
one carrying no signature at all. Approving an agent means adding its public key here;
revoking it means removing the entry. That is a deliberate design choice rather than a
placeholder for something richer:

* It needs no issuer, no network call, and no clock, so it works identically on a
  laptop and in a cluster.
* It is the same shape as a JWKS, so the upgrade path — SPIRE issuing and rotating
  the keys, rooted in workload attestation — replaces *where the keys come from*
  without changing a line of verification logic.

What it does not do is attest anything. The set records which keys an operator
approved, not which workloads a platform vouched for. Say that plainly when
demoing it.

File format — a JSON object of `kid` to public key, each 32 bytes encoded as hex,
base64 or base64url:

    {
      "runner-01": "3b6a27bcceb6a42d62a3a8d02a6f0d73653215771de243a63ac048a18b59da29",
      "runner-02": "O2onvM8oCu..."
    }

Only public keys belong here. A 32-byte Ed25519 *seed* is indistinguishable from a
public key by length alone, so `load()` cannot tell you that you pasted a private
key by mistake — keep the set in a ConfigMap, never a Secret, so the distinction
stays obvious in review.
"""
from __future__ import annotations

import base64
import json
import pathlib


def _decode_key(value: str, kid: str) -> bytes:
    """Decode one 32-byte public key. Same ladder as `signing.load_seed`."""
    text = value.strip()
    for decode in (bytes.fromhex,
                   lambda s: base64.b64decode(s, validate=True),
                   lambda s: base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))):
        try:
            out = decode(text)
        except Exception:  # noqa: BLE001
            continue
        if len(out) == 32:
            return out
    raise ValueError(
        f"key for kid {kid!r} is not a 32-byte Ed25519 public key "
        f"(hex, base64 or base64url)")


class KeySet:
    """An immutable `kid` -> public key mapping.

    Loaded once at startup. Rotation means restarting with a new file; live reload
    is deliberately absent, because a verifier that silently widens the set it
    trusts is harder to reason about than one that restarts.
    """

    def __init__(self, keys: dict[str, bytes]) -> None:
        self._keys = dict(keys)

    def __len__(self) -> int:
        return len(self._keys)

    def __contains__(self, kid: object) -> bool:
        return kid in self._keys

    @property
    def kids(self) -> tuple[str, ...]:
        """Approved key ids, sorted. Safe to log — a public key id is not a secret."""
        return tuple(sorted(self._keys))

    def select(self, kid: str | None) -> bytes | None:
        """The key named by `kid`, or None if it is unknown or absent.

        A `None` kid returns the only key when the set holds exactly one, which
        keeps a single-key deployment working without every signer having to name
        its key. With two or more keys an unnamed token is ambiguous, and guessing
        would mean accepting a signature from any approved agent when the event
        claimed to be from none of them — so it returns None and the caller
        refuses.
        """
        if kid is None:
            if len(self._keys) == 1:
                return next(iter(self._keys.values()))
            return None
        return self._keys.get(kid)


def load(path: str | pathlib.Path) -> KeySet:
    """Read an approved-key set from a JSON file.

    Raises on a malformed file rather than starting with a partial set: an
    authorization list that silently lost an entry fails closed for a legitimate
    agent, which looks like a broken deployment rather than a security control.
    """
    p = pathlib.Path(path)
    raw = json.loads(p.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{p}: expected a JSON object of kid -> public key")
    keys: dict[str, bytes] = {}
    for kid, value in raw.items():
        if not isinstance(kid, str) or not kid:
            raise ValueError(f"{p}: every kid must be a non-empty string")
        if not isinstance(value, str):
            raise ValueError(f"{p}: key for kid {kid!r} must be a string")
        keys[kid] = _decode_key(value, kid)
    return KeySet(keys)


def load_if_set(path: str | pathlib.Path | None) -> KeySet | None:
    """`load()` when a path is configured, else None — meaning "not enabled"."""
    if not path:
        return None
    return load(path)
