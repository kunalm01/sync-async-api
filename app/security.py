"""Callback abuse protection (SSRF guard, signing) and per-client rate limiting."""
import asyncio
import hashlib
import hmac
import ipaddress
import socket
import time
from urllib.parse import urlparse

from . import config


class CallbackURLError(ValueError):
    pass


def _ip_is_private(ip: ipaddress._BaseAddress) -> bool:
    return (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
            or ip.is_reserved or ip.is_unspecified)


async def validate_callback_url(url: str) -> None:
    """Reject non-http(s), credentials in URL, disallowed hosts, and (in prod) private/metadata IPs."""
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        raise CallbackURLError("callback_url must be http or https")
    if not p.hostname:
        raise CallbackURLError("callback_url has no host")
    if p.username or p.password:
        raise CallbackURLError("credentials in callback_url are not allowed")
    if len(url) > 2048:
        raise CallbackURLError("callback_url too long")
    host = p.hostname.lower()
    if config.CALLBACK_HOST_ALLOWLIST and host not in config.CALLBACK_HOST_ALLOWLIST:
        raise CallbackURLError("callback host not in allowlist")
    if config.ALLOW_PRIVATE_CALLBACKS:
        return
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, p.port or 80, type=socket.SOCK_STREAM)
    except socket.gaierror:
        raise CallbackURLError("callback host does not resolve")
    for info in infos:
        if _ip_is_private(ipaddress.ip_address(info[4][0])):
            raise CallbackURLError("callback host resolves to a private/reserved address")


def sign(body: bytes, ts: str) -> str:
    """HMAC-SHA256 over 'timestamp.body' so receivers can verify origin and reject replays."""
    return hmac.new(config.CALLBACK_SECRET.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()


class RateLimiter:
    """Token bucket per client key. In-memory; a multi-instance deploy would use Redis."""

    def __init__(self, rate: float, burst: int):
        self.rate, self.burst = rate, burst
        self.buckets: dict[str, tuple[float, float]] = {}

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        tokens, last = self.buckets.get(key, (self.burst, now))
        tokens = min(self.burst, tokens + (now - last) * self.rate)
        if tokens < 1:
            self.buckets[key] = (tokens, now)
            return False
        self.buckets[key] = (tokens - 1, now)
        return True
