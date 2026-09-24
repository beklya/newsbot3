"""Минимальный тест python-socks → SOCKS5 без Telethon.

Цель: понять, проблема в python-socks/asyncio или в интеграции Telethon.

Берёт PROXY_URL из .env (через ReceiverSettings), пытается через python-socks
открыть TCP-соединение до Telegram DC4 (149.154.167.50:443) и закрыть.

Usage:
    python scripts/test_socks5_python.py
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# Python 3.14 + Windows fix — см. scripts/redis_inspect.py
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


async def main() -> int:
    # Загружаем настройки через тот же модуль что receiver
    from src.services.receiver.config import load_settings

    s = load_settings()
    if not s.proxy_enabled:
        print("FATAL: PROXY_ENABLED=false in .env — nothing to test", file=sys.stderr)
        return 1
    if not s.proxy_url:
        print("FATAL: PROXY_URL is empty", file=sys.stderr)
        return 1

    print(f"proxy_url = {s.proxy_url[:30]}...[truncated]")
    tup = s.resolve_proxy_tuple()
    print(f"resolved tuple: type={tup[0]} host={tup[1]} port={tup[2]} rdns={tup[3]} "
          f"user={tup[4]!r} pass=<{len(tup[5])} chars>")
    print()

    # Импорт python-socks
    try:
        from python_socks.async_.asyncio import Proxy
    except ImportError as e:
        print(f"FATAL: python-socks not installed: {e}", file=sys.stderr)
        return 1

    # Тест: разные destinations чтобы понять что специфично для Telegram
    targets = [
        ("1.1.1.1", 443, "Cloudflare DNS"),
        ("8.8.8.8", 443, "Google DNS"),
        ("142.250.74.110", 443, "Google.com IP"),
        ("104.18.32.115", 443, "Cloudflare CDN"),
        ("87.250.250.242", 443, "Yandex"),
        ("149.154.167.50", 443, "Telegram DC4 (Amsterdam)"),
        ("149.154.175.54", 443, "Telegram DC2 (Amsterdam)"),
        ("149.154.167.91", 443, "Telegram DC1 (Amsterdam)"),
        ("91.108.56.130", 443, "Telegram DC5 (Singapore)"),
    ]

    for dest_host, dest_port, label in targets:
        print(f"--- Test: {label} ({dest_host}:{dest_port}) ---")
        proxy = Proxy.from_url(s.proxy_url)
        t0 = time.monotonic()
        try:
            sock = await proxy.connect(
                dest_host=dest_host,
                dest_port=dest_port,
                timeout=15,
            )
            elapsed = (time.monotonic() - t0) * 1000
            print(f"  OK connected in {elapsed:.0f}ms socket={sock}")
            sock.close()
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            print(f"  FAIL after {elapsed:.0f}ms: {type(e).__name__}: {e}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
