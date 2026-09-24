"""Self-test: which candidate `EVENT_BRIDGE_PUBLIC_BASE_URL` values will work
from a smartphone that can't share this Mac's routing context.

What we can determine from the Mac alone:
  * Is EB actually **listening** on this address+port? (TCP-connect probe
    against ourselves.) If not listening, no external client will reach it.
  * Is the URL's host a **public tunnel** (ngrok / cloudflared quick tunnel /
    Tailscale MagicDNS)? Those are designed to work from anywhere.
  * Is the Mac's default route pointed at a **VPN interface** (`utun*`)?
    Under Cisco TUNNELALL, LAN candidates almost certainly won't reach a
    phone even though the Mac itself can hit them.

What we can NOT determine:
  * Whether the phone has a route to this address right now. That's the
    phone's problem, not ours. We surface a verdict + reasoning; the
    operator picks.
"""
from __future__ import annotations

import dataclasses
import socket
import urllib.parse

from eventbridge.netcands import (
    _default_route_source_ipv4,
    enumerate_candidates,
)

VERDICT_ORDER = {
    "recommended": 0,
    "reachable-same-host-only": 1,
    "reachable-lan-only": 2,
    "reachable-vpn-only": 3,
    "not-listening": 4,
    "unknown": 5,
}


_PUBLIC_TUNNEL_SUFFIXES = (
    ".ngrok.app", ".ngrok-free.app", ".ngrok.io", ".ngrok.dev",
    ".trycloudflare.com",
    ".ts.net",             # Tailscale MagicDNS
    ".lhr.rocks",          # localhost.run
    ".serveo.net",
    ".loca.lt",            # localtunnel
)


@dataclasses.dataclass(frozen=True)
class ProbeResult:
    url: str
    label: str                # from Candidate.label, or "public-tunnel" / "external"
    interface: str
    listening: bool
    verdict: str
    notes: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            "url": self.url, "label": self.label, "interface": self.interface,
            "listening": self.listening, "verdict": self.verdict,
            "notes": list(self.notes),
        }


def _is_public_tunnel_host(host: str) -> bool:
    host = (host or "").lower()
    return any(host.endswith(sfx) for sfx in _PUBLIC_TUNNEL_SUFFIXES)


def _probe_tcp(host: str, port: int, timeout: float = 0.7) -> bool:
    """Can we open a TCP connection to host:port from this process?

    For candidates whose host is an IP literal, this proves EB is listening
    on that interface. For DNS names (public tunnels), it proves the tunnel
    forwards back to us.
    """
    try:
        addrs = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return False
    for family, kind, proto, _, sockaddr in addrs:
        try:
            s = socket.socket(family, kind, proto)
            s.settimeout(timeout)
            try:
                s.connect(sockaddr)
                return True
            finally:
                s.close()
        except OSError:
            continue
    return False


def _decide_verdict(cand_label: str, url: str, listening: bool,
                    default_route_iface: str) -> tuple[str, list[str]]:
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    notes: list[str] = []

    if _is_public_tunnel_host(host):
        if listening:
            return "recommended", ["public tunnel — reachable from any phone with internet"]
        else:
            return "not-listening", [
                "public-tunnel hostname resolves but no back-end responds",
                "start the tunnel first (ngrok http 8080 / cloudflared tunnel --url http://127.0.0.1:8080)",
            ]

    if not listening:
        return "not-listening", [
            "TCP connect refused/timed out — EB isn't listening on this address:port",
            "check EB_HTTP_ADDR (bind 0.0.0.0 to reach from LAN)",
        ]

    if cand_label == "loopback":
        return "reachable-same-host-only", ["only reachable from this Mac; phones cannot use loopback"]

    if cand_label == "likely-vpn" or cand_label == "likely-lan-or-vpn":
        # 10/8 or utun*
        notes.append("VPN address — only reachable from inside that VPN")
        return "reachable-vpn-only", notes

    if cand_label == "likely-lan":
        # This is the tricky one — LAN + TUNNELALL trap.
        if default_route_iface.startswith(("utun", "tun", "ppp", "wg")):
            notes.append(f"⚠ default route is via {default_route_iface} (VPN) — under Cisco TUNNELALL a phone's inbound SYN gets a reply routed out the VPN, so the phone times out")
            notes.append("workaround: ngrok / cloudflared / Tailscale (see README §5.2–5.4)")
            return "reachable-lan-only", notes
        notes.append("reachable from phones on the same wifi/LAN")
        return "recommended", notes

    if cand_label == "likely-docker":
        return "reachable-vpn-only", ["docker bridge — only reachable from containers on that bridge"]

    return "unknown", ["no confident heuristic — try it and see"]


def _iface_holding_default_route() -> str:
    """Return the ifname of the interface currently holding the default route,
    or '' if we can't figure it out.
    """
    src = _default_route_source_ipv4()
    if not src:
        return ""
    for c in enumerate_candidates():
        if c.address == src:
            return c.interface
    return ""


def probe_candidates(port: int, current: str | None = None,
                     extra_urls: list[str] | None = None) -> list[ProbeResult]:
    """Build the full probe list — every enumerated interface address + the
    currently-configured URL + any extras the operator asks about."""
    cands = enumerate_candidates()
    default_iface = _iface_holding_default_route()
    seen_urls: set[str] = set()
    results: list[ProbeResult] = []

    def _add_url(url: str, label_hint: str = "external", iface_hint: str = "?"):
        u = url.rstrip("/")
        if u in seen_urls:
            return
        seen_urls.add(u)
        parsed = urllib.parse.urlparse(u)
        host = parsed.hostname or ""
        listen_port = parsed.port or port
        listening = _probe_tcp(host, listen_port)
        # Use a tighter label for public-tunnel hosts
        label = "public-tunnel" if _is_public_tunnel_host(host) else label_hint
        iface = iface_hint if label != "public-tunnel" else "public-tunnel"
        verdict, notes = _decide_verdict(label if label != "public-tunnel" else "public-tunnel",
                                          u, listening, default_iface)
        # For non-public-tunnel externals we re-run decision as label 'unknown'.
        results.append(ProbeResult(url=u, label=label, interface=iface,
                                   listening=listening, verdict=verdict,
                                   notes=tuple(notes)))

    for c in cands:
        _add_url(c.base_url(port), label_hint=c.label, iface_hint=c.interface)
    if current:
        _add_url(current)
    for extra in (extra_urls or []):
        _add_url(extra)

    return sorted(results, key=lambda r: (VERDICT_ORDER.get(r.verdict, 99), r.url))


def format_report(results: list[ProbeResult]) -> str:
    lines = ["[selftest] EVENT_BRIDGE_PUBLIC_BASE_URL candidates — verdict per URL:"]
    verdict_marker = {
        "recommended":               "✔",
        "reachable-same-host-only":  "•",
        "reachable-lan-only":        "◐",
        "reachable-vpn-only":        "◐",
        "not-listening":             "✗",
        "unknown":                   "?",
    }
    for r in results:
        mark = verdict_marker.get(r.verdict, "?")
        lines.append(f"    {mark} {r.url:<40}  [{r.verdict}]")
        for n in r.notes:
            lines.append(f"        · {n}")
    return "\n".join(lines)
