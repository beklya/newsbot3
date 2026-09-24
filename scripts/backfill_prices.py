"""Backfill missing 1-min OHLCV bars from MOEX ISS API (Sprint 6).

Scans `prices_dir` for `prices_<TICKER>.csv` files. For each ticker, reads the
last bar timestamp, fetches all bars from that point to --till (default: now)
from MOEX ISS API, appends new rows to the CSV.

Idempotent: starts from `last_ts + 1 minute`, so re-running the script doesn't
duplicate bars. Safe to schedule daily.

What it covers (v1):
  * Stocks (TQBR class): SBER, GAZP, LKOH, YDEX, ROSN, NVTK, VTBR, GMKN, MGNT,
    MTSS, TATN, PLZL — direct ticker → MOEX symbol mapping.
  * Currencies (CETS class): USDRUB → USD000UTSTOM, CNY → CNYRUB_TOM,
    GLDRUB → GLDRUB_TOM.

NOT covered yet (v1):
  * Futures (SPBFUT): BR, NG, SI, MIX. These have quarterly/monthly contract
    codes (BRM6, SiM6 etc.) that need rolling logic — see Sprint 6 backlog
    task #56. Skipped silently for now.

Usage:
    python scripts/backfill_prices.py                    # all stocks+currencies → now
    python scripts/backfill_prices.py --ticker SBER      # single ticker
    python scripts/backfill_prices.py --till 2026-06-05  # custom end date
    python scripts/backfill_prices.py --dry-run          # show what would happen
    python scripts/backfill_prices.py --verbose          # DEBUG output

Schedule (daily after MOEX close 23:50 MSK):
    See docs/SPRINT_6_BACKFILL_PRICES_DONE.md for Task Scheduler setup.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("requests not installed.  Activate venv first then:  pip install requests",
          file=sys.stderr)
    sys.exit(1)


def _make_session() -> requests.Session:
    """Session with connection pooling + automatic retry on transient errors.

    MOEX ISS occasionally drops connections under sustained load (observed
    SSL EOF after ~9000 bars).  Retry on 5xx + 429 + connection errors with
    exponential backoff handles this without our intervention.
    """
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,         # 1, 2, 4, 8, 16 sec
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=2,
        pool_maxsize=2,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "newsbot3-backfill/1.0",
        "Accept": "application/json",
        "Connection": "keep-alive",
    })
    return s


_SESSION: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = _make_session()
    return _SESSION

DEFAULT_PRICES_DIR = Path(r"D:\quik_sber\newsbot\prices")

# MOEX ISS API — free, no auth, official.  Docs: https://iss.moex.com/iss/reference/
MOEX_BASE = "https://iss.moex.com/iss"
ISS_PAGE_SIZE = 500          # MOEX returns up to 500 candles per request
FROM_SCRATCH_DT = datetime(2022, 1, 1)  # Sprint 9: backfill start for empty files
ISS_REQ_DELAY_SEC = 0.1      # be polite to MOEX

# canonical ticker → (engine, market, board, sec_code) for ISS endpoint:
#   /iss/engines/{engine}/markets/{market}/boards/{board}/securities/{sec_code}/candles
#
# `board` doesn't change the candle response (verified 2026-06-06 — ISS ignores
# board for the /candles endpoint), but we pass it for URL well-formedness.
# Use "RFUD" for futures (regular series).
TICKER_TO_MOEX: dict[str, tuple[str, str, str, str]] = {
    # Stocks T+1 (TQBR class) — direct ticker name match
    "SBER": ("stock", "shares", "TQBR", "SBER"),
    "GAZP": ("stock", "shares", "TQBR", "GAZP"),
    "LKOH": ("stock", "shares", "TQBR", "LKOH"),
    "YDEX": ("stock", "shares", "TQBR", "YDEX"),   # renamed from YNDX in 2024
    "ROSN": ("stock", "shares", "TQBR", "ROSN"),
    "NVTK": ("stock", "shares", "TQBR", "NVTK"),
    "VTBR": ("stock", "shares", "TQBR", "VTBR"),
    "GMKN": ("stock", "shares", "TQBR", "GMKN"),
    "MGNT": ("stock", "shares", "TQBR", "MGNT"),
    "MTSS": ("stock", "shares", "TQBR", "MTSS"),
    "TATN": ("stock", "shares", "TQBR", "TATN"),
    "PLZL": ("stock", "shares", "TQBR", "PLZL"),
    # Sprint 9 — MOEXBC blue-chip additions
    "SNGS": ("stock", "shares", "TQBR", "SNGS"),
    "MOEX": ("stock", "shares", "TQBR", "MOEX"),
    "T":    ("stock", "shares", "TQBR", "T"),      # Т-Технологии (ex-TCSG)
    "OZON": ("stock", "shares", "TQBR", "OZON"),
    "X5":   ("stock", "shares", "TQBR", "X5"),     # только с 2025-01 (редомициляция)
    # Currencies (CETS class)
    "USDRUB": ("currency", "selt", "CETS", "USD000UTSTOM"),
    "CNY":    ("currency", "selt", "CETS", "CNYRUB_TOM"),
    "GLDRUB": ("currency", "selt", "CETS", "GLDRUB_TOM"),
    # Futures (FORTS) — Sprint 6.1 task #56 partial implementation.
    # Each canonical ticker maps to ONE current "near-front" contract.  This
    # creates a price-jump splice with the Phase 2 continuous-stitched series
    # (which uses front-month-on-that-day rolling), but for replay windows
    # 1+ months past the splice the feature_builder's RSI/ATR have stabilized
    # so this is acceptable as a Sprint 6.1 unblock for the live replay
    # backtest.  TODO Sprint 6.2: proper roll-aware stitching with contract
    # multipliers + jump adjustment, AND auto-detection of which contract
    # is now the front month (today MXM6 expired so MXU6 is now front).
    #
    # As of 2026-06-06 the chosen "front-or-near-front" contracts are:
    "BR":  ("futures", "forts", "RFUD", "BRN6"),  # Brent Jul-2026 (expires 2026-07-01)
    "NG":  ("futures", "forts", "RFUD", "NGM6"),  # Natgas Jun-2026 (expires 2026-06-26)
    "SI":  ("futures", "forts", "RFUD", "SiM6"),  # USD/RUB Jun-2026 (expires 2026-06-18)
    "MIX": ("futures", "forts", "RFUD", "MXU6"),  # MOEX index Sep-2026 (MXM6 already expired)
}

# Phase 2 CSV uses Finam-Export format:
#   ticker,per,date,time,open,high,low,close,vol,datetime
# where date=YYYYMMDD, time=HHMMSS (leading zeros dropped, e.g. 70000 = 07:00:00),
# datetime='YYYY-MM-DD HH:MM:SS'.  Timestamp is the LAST column.
CSV_HEADER = "ticker,per,date,time,open,high,low,close,vol,datetime"

log = logging.getLogger("backfill_prices")


def setup_logging(verbose: bool, log_file: Optional[Path]) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def detect_csv_format(csv_path: Path) -> Optional[str]:
    """Read header line of CSV.  Returns the header string, or None if file
    doesn't exist or is empty."""
    if not csv_path.exists():
        return None
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        first = f.readline().strip()
    return first or None


def read_last_ts(csv_path: Path) -> Optional[datetime]:
    """Read the last data row's timestamp from the CSV (efficient tail read).

    Finam-export format: `ticker,per,date,time,open,high,low,close,vol,datetime`
    Timestamp is the LAST column, e.g. '2026-04-20 23:49:00' (naive MSK).
    """
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return None
    # Read last ~4KB from end
    with open(csv_path, "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        offset = max(0, size - 4096)
        f.seek(offset)
        tail = f.read().decode("utf-8", errors="ignore")
    lines = [ln for ln in tail.splitlines() if ln.strip()]
    # Last non-header line
    for ln in reversed(lines):
        if ln.startswith("ticker,") or ln.startswith("ts,"):
            continue
        parts = ln.split(",")
        # Finam format has 10 columns; timestamp is LAST column.
        # Fallback to first column for legacy formats.
        candidates = []
        if len(parts) >= 10:
            candidates.append(parts[-1])         # Finam: datetime column
        if len(parts) >= 7:
            candidates.append(parts[1])          # legacy phase-2 alt
        candidates.append(parts[0])              # very old: ts as 1st col
        for ts_str in candidates:
            ts_str = ts_str.strip().strip('"')
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
                try:
                    return datetime.strptime(ts_str, fmt)
                except ValueError:
                    continue
    return None


def fetch_iss_candles(
    engine: str, market: str, board: str, sec_code: str,
    from_dt: datetime, till_dt: datetime,
    interval: int = 1,
) -> Iterator[dict]:
    """Iterate 1-min candles from MOEX ISS API with pagination.

    Returns dicts with keys: open, close, high, low, value, volume, begin, end.
    """
    url = (f"{MOEX_BASE}/engines/{engine}/markets/{market}/boards/{board}"
           f"/securities/{sec_code}/candles.json")
    start = 0
    while True:
        params = {
            "from": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "till": till_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "interval": interval,
            "start": start,
        }
        log.debug("ISS GET %s params=%s", url, params)
        r = get_session().get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        candles = data.get("candles", {})
        cols = candles.get("columns", [])
        rows = candles.get("data", [])
        if not rows:
            return
        for row in rows:
            yield dict(zip(cols, row))
        if len(rows) < ISS_PAGE_SIZE:
            return
        start += len(rows)
        time.sleep(ISS_REQ_DELAY_SEC)


def backfill_ticker(
    canonical: str, prices_dir: Path, till_dt: datetime,
    *, dry_run: bool = False,
) -> int:
    """Backfill one ticker's CSV file. Returns count of new bars appended."""
    if canonical not in TICKER_TO_MOEX:
        log.info("ticker=%s — no MOEX mapping (futures, deferred to task #56)",
                 canonical)
        return 0

    engine, market, board, sec_code = TICKER_TO_MOEX[canonical]
    csv_path = prices_dir / f"prices_{canonical}.csv"

    if not csv_path.exists():
        log.warning("ticker=%s csv=%s does not exist — skip "
                    "(create empty file with header if you want to backfill from scratch)",
                    canonical, csv_path)
        return 0

    last_ts = read_last_ts(csv_path)
    if last_ts is None:
        # Empty/header-only file → backfill from scratch (Sprint 9 default 2022-01-01)
        log.info("ticker=%s csv=%s — empty, backfilling from scratch (%s)",
                 canonical, csv_path, FROM_SCRATCH_DT.date())
        from_dt = FROM_SCRATCH_DT
    else:
        from_dt = last_ts + timedelta(minutes=1)
    if from_dt >= till_dt:
        log.info("ticker=%-7s up to date  (last=%s)", canonical, last_ts)
        return 0

    log.info("ticker=%-7s backfilling %s -> %s  (%s = %s/%s/%s)",
             canonical, from_dt, till_dt, sec_code, engine, market, board)

    new_rows: list[str] = []
    for rec in fetch_iss_candles(engine, market, board, sec_code, from_dt, till_dt):
        ts_str = (rec.get("begin") or "").replace("T", " ")
        try:
            ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            log.debug("skip unparseable ts: %s", ts_str)
            continue
        try:
            o = float(rec["open"]); h = float(rec["high"])
            l_ = float(rec["low"]); c = float(rec["close"])
            v = float(rec["volume"])
        except (KeyError, TypeError, ValueError):
            log.debug("skip malformed row: %s", rec)
            continue
        # Finam format columns: ticker,per,date,time,open,high,low,close,vol,datetime
        # date = YYYYMMDD, time = HHMMSS with leading zeros DROPPED (int repr).
        date_int = ts_dt.year * 10000 + ts_dt.month * 100 + ts_dt.day
        time_int = ts_dt.hour * 10000 + ts_dt.minute * 100 + ts_dt.second
        new_rows.append(
            f"{canonical},1,{date_int},{time_int},"
            f"{o:.4f},{h:.4f},{l_:.4f},{c:.4f},{int(v)},{ts_str}"
        )

    if not new_rows:
        log.info("ticker=%-7s no new bars from MOEX in window", canonical)
        return 0

    if dry_run:
        log.info("[dry-run] ticker=%-7s would append %d bars (first=%s)",
                 canonical, len(new_rows), new_rows[0])
        return len(new_rows)

    # Append.  Ensure existing file ends with newline so we don't merge into last line.
    with open(csv_path, "rb") as f:
        f.seek(-1, 2)
        last_byte = f.read(1)
    sep = "" if last_byte in (b"\n", b"\r") else "\n"
    with open(csv_path, "a", encoding="utf-8") as f:
        f.write(sep + "\n".join(new_rows) + "\n")

    log.info("ticker=%-7s APPENDED %d bars to %s", canonical, len(new_rows), csv_path)
    return len(new_rows)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--prices-dir", default=str(DEFAULT_PRICES_DIR),
                   help=f"Dir with prices_TICKER.csv (default {DEFAULT_PRICES_DIR})")
    p.add_argument("--ticker", help="Backfill only this canonical ticker")
    p.add_argument("--till",
                   help="End date YYYY-MM-DD (default: now MSK)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would be appended without writing")
    p.add_argument("--verbose", action="store_true",
                   help="DEBUG level logging")
    p.add_argument("--log-file",
                   default=str(Path(__file__).resolve().parent.parent /
                               "logs" / "backfill_prices.log"),
                   help="Append run log to this file (default: logs/backfill_prices.log)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    log_file = Path(args.log_file) if args.log_file and args.log_file != "" else None
    setup_logging(args.verbose, log_file)

    prices_dir = Path(args.prices_dir)
    if not prices_dir.exists():
        log.error("prices dir not found: %s", prices_dir)
        return 1

    if args.till:
        try:
            till_dt = datetime.strptime(args.till, "%Y-%m-%d")
            # End-of-day to capture the full day
            till_dt = till_dt.replace(hour=23, minute=59, second=59)
        except ValueError:
            log.error("invalid --till format, expected YYYY-MM-DD")
            return 2
    else:
        till_dt = datetime.now()

    tickers = [args.ticker] if args.ticker else list(TICKER_TO_MOEX.keys())

    total = 0
    failures = 0
    for ticker in tickers:
        try:
            n = backfill_ticker(ticker, prices_dir, till_dt, dry_run=args.dry_run)
            total += n
        except requests.HTTPError as e:
            log.error("ticker=%s ISS HTTP error: %s", ticker, e)
            failures += 1
        except requests.RequestException as e:
            log.error("ticker=%s network error: %s", ticker, e)
            failures += 1
        except Exception:
            log.exception("ticker=%s unexpected error", ticker)
            failures += 1

    log.info("DONE  appended=%d bars  failures=%d  tickers=%d  dry_run=%s",
             total, failures, len(tickers), args.dry_run)
    return 0 if failures == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
