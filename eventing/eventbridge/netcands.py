"""Enumerate candidate IPv4 addresses bound on this machine so an operator
can tell at a glance which one to plug into `EVENT_BRIDGE_PUBLIC_BASE_URL`.

The Mac cannot know which of its addresses is routable *from a phone*.
That depends on the phone's own routing context, and — critically — on
whether a corporate VPN is running in `tunnel-all` mode (Cisco AnyConnect
/ Secure Client's TUNNELALL). Under TUNNELALL:

  - The Mac's default route is the VPN tunnel, so replies to a LAN SYN
    (say from a phone on the same wifi to 192.168.x.y) go out the tunnel
    instead of the LAN interface, and the phone times out.
  - Some VPN policies also drop inbound traffic on the LAN interface
    entirely, so the SYN never arrives.

So the best we can do is *label* candidates and let the operator pick.

Stdlib-only implementation:
  - `getaddrinfo(gethostname())` gives us the Mac's own advertised names,
    which usually includes the LAN address.
  - A UDP `connect` trick asks the kernel which source address it would
    pick for an outbound flow — that reveals the current default-route
    address (which under TUNNELALL is the VPN tunnel address).
  - `socket.if_nameindex` + `SIOCGIFADDR` would give all interfaces on
    Linux, but macOS doesn't expose SIOCGIFADDR via ioctl-in-Python. We
    fall back to `ifconfig` output when it's available (macOS + Linux);
    that's still stdlib-territory since we shell out.
"""
from __future__ import annotations

import dataclasses
import ipaddress
import re
import shutil
import socket
import subprocess


@dataclasses.dataclass(frozen=True)
class Candidate:
    address: str          # dotted-quad IPv4
    interface: str        # e.g. "en0", "utun3", "lo0"
    label: str            # "loopback" | "likely-lan" | "likely-vpn" | "likely-docker" | "other"

    def base_url(self, port: int) -> str:
        return f"http://{self.address}:{port}"


_PRIVATE_LAN_NETS = [
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    # 10.0.0.0/8 is *sometimes* LAN, but a corporate VPN almost always
    # hands out a 10/8 too. So 10/8 addresses are labeled "vpn-or-lan"
    # unless the interface name gives it away (utun*, tun*, tap*, ppp*).
]

_VPN_IFACE_HINTS = ("utun", "tun", "tap", "ppp", "wg", "gpd0")   # macOS + Linux
_DOCKER_IFACE_HINTS = ("docker", "br-", "bridge")


def _classify(addr: str, iface: str) -> str:
    try:
        ip = ipaddress.ip_address(addr)
    except ValueError:
        return "other"
    if ip.is_loopback:
        return "loopback"
    if iface.startswith(_VPN_IFACE_HINTS):
        return "likely-vpn"
    if iface.startswith(_DOCKER_IFACE_HINTS):
        return "likely-docker"
    if any(ip in net for net in _PRIVATE_LAN_NETS):
        return "likely-lan"
    # 10.0.0.0/8 with no VPN-hint interface — likely LAN, could be VPN
    if ip in ipaddress.ip_network("10.0.0.0/8"):
        return "likely-lan-or-vpn"
    if ip.is_private:
        return "private-other"
    return "public"


def _parse_ifconfig(text: str) -> list[Candidate]:
    """Cheap ifconfig parser: BSD (macOS) + Linux `iproute2` fallback."""
    out: list[Candidate] = []
    current_iface = ""
    for line in text.splitlines():
        # macOS: "en0: flags=...", "\tinet 192.168.68.9 netmask ..."
        # Linux `ifconfig` legacy: same shape.
        m_if = re.match(r"^([a-zA-Z0-9_\-.]+):\s", line)
        if m_if:
            current_iface = m_if.group(1)
            continue
        m_ip = re.search(r"\binet\s+(\d+\.\d+\.\d+\.\d+)\b", line)
        if m_ip and current_iface:
            addr = m_ip.group(1)
            out.append(Candidate(address=addr, interface=current_iface,
                                 label=_classify(addr, current_iface)))
    return out


def _parse_ip_addr(text: str) -> list[Candidate]:
    """`ip -o -4 addr show` — modern Linux (iproute2)."""
    out: list[Candidate] = []
    for line in text.splitlines():
        # "3: en0 inet 192.168.68.9/24 brd ... scope global en0"
        m = re.match(r"^\d+:\s+(\S+)\s+inet\s+(\d+\.\d+\.\d+\.\d+)/\d+", line)
        if m:
            iface, addr = m.group(1), m.group(2)
            out.append(Candidate(address=addr, interface=iface,
                                 label=_classify(addr, iface)))
    return out


def _default_route_source_ipv4() -> str | None:
    """The address the kernel would put in the src field for a public flow.

    Under Cisco TUNNELALL this returns the *VPN* tunnel address, which is
    exactly the interface that intercepts our replies. Useful as a "here's
    what's actually holding the default route" datapoint.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.settimeout(0.5)
            # UDP connect never sends a packet — just triggers the kernel's
            # source-selection logic.
            s.connect(("1.1.1.1", 53))
            return s.getsockname()[0]
        finally:
            s.close()
    except OSError:
        return None


def enumerate_candidates() -> list[Candidate]:
    """Return a de-duplicated, labeled list of candidate IPv4 addresses."""
    # 1) Prefer `ifconfig` on macOS; try `ip addr` on Linux; fall back to
    #    hostname + getaddrinfo for the minimal picture.
    seen: dict[tuple[str, str], Candidate] = {}

    def _add(c: Candidate) -> None:
        seen.setdefault((c.address, c.interface), c)

    def _try(cmd: list[str], parser) -> None:
        if not shutil.which(cmd[0]):
            return
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            if r.returncode == 0:
                for c in parser(r.stdout):
                    _add(c)
        except Exception:
            pass

    _try(["ifconfig"], _parse_ifconfig)
    _try(["ip", "-o", "-4", "addr", "show"], _parse_ip_addr)

    # 2) Fallback for stripped-down environments: use gethostbyname_ex.
    if not seen:
        try:
            _, _, addrs = socket.gethostbyname_ex(socket.gethostname())
            for a in addrs:
                _add(Candidate(address=a, interface="?", label=_classify(a, "?")))
        except socket.gaierror:
            pass

    # Always add loopback (some environments strip it from ifconfig output).
    _add(Candidate(address="127.0.0.1", interface="lo0", label="loopback"))

    # Rank candidates: LAN > VPN > docker > loopback > other.
    _order = {"likely-lan": 0, "likely-lan-or-vpn": 1, "likely-vpn": 2,
              "public": 3, "private-other": 4, "likely-docker": 5,
              "other": 6, "loopback": 7}
    return sorted(seen.values(), key=lambda c: (_order.get(c.label, 99), c.interface))


def format_startup_hint(cands: list[Candidate], port: int,
                        current: str, event_bridge_env: str = "EVENT_BRIDGE_PUBLIC_BASE_URL") -> str:
    """Return a multi-line block for EB's startup log."""
    default_route = _default_route_source_ipv4()
    lines = [
        f"[eventbridge] {event_bridge_env}={current!r} — used verbatim in HTML view + ntfy Click/Actions.",
        f"[eventbridge] candidate addresses on this host (pick one and re-run with {event_bridge_env}=<url>):",
    ]
    for c in cands:
        marker = " ← current default route" if c.address == default_route else ""
        lines.append(f"    {c.base_url(port):<32}  [{c.label:<18}] iface={c.interface}{marker}")
    lines.append("[eventbridge] note: under Cisco AnyConnect TUNNELALL, the phone will")
    lines.append("[eventbridge]       fail to reach a LAN address because the Mac's return")
    lines.append("[eventbridge]       traffic is routed via the VPN tunnel. Use a mesh (Tailscale)")
    lines.append("[eventbridge]       or public tunnel (ngrok, cloudflared) in that case.")
    return "\n".join(lines)
