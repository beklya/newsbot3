"""Centralized Redis async client factory (Sprint 6).

All services route async Redis creation through `make_redis()` so we get
consistent TCP keepalive + connect-timeout settings.

Why this exists
---------------
Observed on 2026-06-01: every 10-30 minutes the SSH-tunneled Redis connection
on Windows clients drops with WinError 64 (ERROR_NETNAME_DELETED) / 1225
(WSAECONNREFUSED). Net ping to VPS is rock-solid (0% loss over 30 min, avg 0 ms),
so the issue is NOT network connectivity — it's a TCP-layer idle disconnect.

Likely causes:
  1. Memurai/Redis `timeout` config drops idle clients
  2. Intermediate NAT/firewall closes long-idle TCP sockets
  3. Python 3.14 ProactorEventLoop quirks with tunneled localhost TCP

Mitigation: force TCP keepalive on each Redis socket so idle bytes are sent
every 30 sec, preventing intermediate state from staleness.

Server-side fix (separately on VPS):
    redis-cli CONFIG SET timeout 0
    redis-cli CONFIG SET tcp-keepalive 60
    redis-cli CONFIG REWRITE
"""
from __future__ import annotations

import socket
import sys

from redis.asyncio import Redis


def make_redis(url: str, *, decode_responses: bool = False) -> Redis:
    """Build an async Redis client with TCP keepalive + sensible timeouts.

    Args:
        url: redis://host:port URL.
        decode_responses: passed to Redis.from_url. False (default) keeps bytes
            for stream payloads; some scripts want True for inspection.

    Returns:
        Configured `redis.asyncio.Redis` instance. NOT pre-pinged — callers
        should `await redis.ping()` to surface connection errors early.
    """
    # SO_KEEPALIVE on the socket — enabled by socket_keepalive=True.
    # Platform-specific tuning of WHEN keepalive probes fire:
    socket_keepalive_options: dict = {}
    if sys.platform == "win32":
        # On Windows, socket.TCP_KEEPALIVE (no "I") maps to TCP_KEEPIDLE in seconds.
        # Default Windows keepalive idle is 2 hours which is useless for our 10-30
        # min drop pattern. Override to 30 sec.
        if hasattr(socket, "TCP_KEEPALIVE"):
            socket_keepalive_options[socket.TCP_KEEPALIVE] = 30
    else:
        # Linux: full control over idle/interval/count.
        if hasattr(socket, "TCP_KEEPIDLE"):
            socket_keepalive_options[socket.TCP_KEEPIDLE] = 30
        if hasattr(socket, "TCP_KEEPINTVL"):
            socket_keepalive_options[socket.TCP_KEEPINTVL] = 10
        if hasattr(socket, "TCP_KEEPCNT"):
            socket_keepalive_options[socket.TCP_KEEPCNT] = 3

    return Redis.from_url(
        url,
        decode_responses=decode_responses,
        socket_keepalive=True,
        socket_keepalive_options=socket_keepalive_options,
        # Fail fast on tunnel hiccups — consumer retry loop kicks in cleanly.
        # Without this, asyncio.open_connection can hang for OS-default duration.
        socket_connect_timeout=5,
        # Per-command read timeout. Long enough for big XRANGE on heartbeat
        # stream, short enough that hangs surface as errors.
        socket_timeout=30,
    )
