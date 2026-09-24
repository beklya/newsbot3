r"""
Sprint 4 / Commit 4.2.a — Dataset Discovery v4
================================================

Профилирование D:\quik_sber\newsbot\duble3\telegram_news.jsonl.

v4 fixes на основе обнаружения реальной схемы:
  - 'text' -> 'full_text' (+ fallback на 'headline')
  - 'message_id' -> 'tg_msg_id'
  - text_hash считаем сами через hashlib (нет в исходнике)
  - Добавлено профилирование поля 'analysis' (legacy LLM-enrichment):
      * процент присутствия
      * по годам
      * структура analysis (top-level keys)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import unicodedata
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


SOURCE_PATH = Path(r"D:\quik_sber\newsbot\duble3\telegram_news.jsonl")
OUT_DIR = Path(__file__).resolve().parent / "data"
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U0001F900-\U0001F9FF"
    "\U00002600-\U000026FF"
    "]",
    flags=re.UNICODE,
)


def _extract_text(record: dict[str, Any]) -> str:
    """Извлекает текст из записи. full_text приоритетнее headline."""
    text = record.get("full_text")
    if isinstance(text, str) and text:
        return text
    text = record.get("headline")
    if isinstance(text, str) and text:
        return text
    text = record.get("text")  # fallback на старое имя
    return text if isinstance(text, str) else ""


class StreamStats:
    def __init__(self) -> None:
        self.total_lines = 0
        self.total_records = 0
        self.json_errors = 0

        self.total_bytes = 0
        self.text_bytes_total = 0

        self.all_keys: set[str] = set()
        self.key_presence: Counter[str] = Counter()

        self.channels: Counter[str] = Counter()
        self.channel_first_seen: dict[str, datetime] = {}
        self.channel_last_seen: dict[str, datetime] = {}

        self.global_min_ts: Optional[datetime] = None
        self.global_max_ts: Optional[datetime] = None
        self.events_per_month: Counter[tuple[int, int]] = Counter()
        self.events_per_year: Counter[int] = Counter()

        self.text_lengths: list[int] = []
        self.text_lengths_cap = 100_000
        self.text_length_buckets: Counter[str] = Counter()
        self.empty_text = 0
        self.empty_or_very_short = 0
        self.very_long = 0

        # Duplicates: SHA-256 от текста
        self.text_hash_seen: set[str] = set()
        self.text_hash_duplicates = 0

        # Edits by (channel, tg_msg_id)
        self.channel_msgid_seen: set[tuple[str, int]] = set()
        self.likely_edits = 0

        self.tz_offsets_sample: list[int] = []
        self.tz_offset_buckets: Counter[int] = Counter()
        self.tz_mismatch_records = 0

        self.has_emoji = 0
        self.has_rtl = 0
        self.has_control_chars = 0
        self.bad_unicode = 0

        # Analysis field profiling (legacy enrichment)
        self.has_analysis = 0
        self.analysis_by_year: Counter[int] = Counter()
        self.analysis_keys: Counter[str] = Counter()  # top-level keys внутри analysis
        self.analysis_non_dict = 0  # если analysis это не dict

        # Headline vs full_text presence
        self.headline_present = 0
        self.full_text_present = 0

        self.start_time = time.time()

    def update(self, record: dict[str, Any], raw_line_bytes: int) -> None:
        self.total_records += 1
        self.total_bytes += raw_line_bytes
        self.all_keys.update(record.keys())
        for k, v in record.items():
            if v is not None and v != "":
                self.key_presence[k] += 1

        channel = record.get("channel") or record.get("source") or "<unknown>"
        self.channels[channel] += 1

        ts_utc = self._parse_utc_timestamp(record)
        if ts_utc is not None:
            year, month = ts_utc.year, ts_utc.month
            self.events_per_month[(year, month)] += 1
            self.events_per_year[year] += 1

            if self.global_min_ts is None or ts_utc < self.global_min_ts:
                self.global_min_ts = ts_utc
            if self.global_max_ts is None or ts_utc > self.global_max_ts:
                self.global_max_ts = ts_utc

            if channel not in self.channel_first_seen or ts_utc < self.channel_first_seen[channel]:
                self.channel_first_seen[channel] = ts_utc
            if channel not in self.channel_last_seen or ts_utc > self.channel_last_seen[channel]:
                self.channel_last_seen[channel] = ts_utc

            self._verify_tz(record, ts_utc)

        # === ТЕКСТ — фикс v4 ===
        text = _extract_text(record)

        # Presence stats
        if isinstance(record.get("full_text"), str) and record["full_text"]:
            self.full_text_present += 1
        if isinstance(record.get("headline"), str) and record["headline"]:
            self.headline_present += 1

        text_len = len(text)
        self.text_bytes_total += len(text.encode("utf-8", errors="replace"))

        if text_len == 0:
            self.empty_text += 1
        elif text_len < 10:
            self.empty_or_very_short += 1
        elif text_len >= 4000:
            self.very_long += 1

        if text_len == 0:
            bucket = "0"
        elif text_len < 30:
            bucket = "1-29"
        elif text_len < 100:
            bucket = "30-99"
        elif text_len < 300:
            bucket = "100-299"
        elif text_len < 1000:
            bucket = "300-999"
        elif text_len < 4000:
            bucket = "1000-3999"
        else:
            bucket = "4000+"
        self.text_length_buckets[bucket] += 1

        if len(self.text_lengths) < self.text_lengths_cap:
            self.text_lengths.append(text_len)

        if text:
            if EMOJI_PATTERN.search(text):
                self.has_emoji += 1
            if self._has_rtl_chars(text):
                self.has_rtl += 1
            if self._has_control_chars(text):
                self.has_control_chars += 1
            try:
                unicodedata.normalize("NFC", text)
            except Exception:
                self.bad_unicode += 1

            # Duplicates через SHA-256
            text_hash = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
            if text_hash in self.text_hash_seen:
                self.text_hash_duplicates += 1
            else:
                self.text_hash_seen.add(text_hash)

        # Edits — tg_msg_id вместо message_id (v4 fix)
        msg_id = record.get("tg_msg_id") or record.get("message_id") or record.get("msg_id")
        if msg_id is not None and channel != "<unknown>":
            try:
                key = (channel, int(msg_id))
                if key in self.channel_msgid_seen:
                    self.likely_edits += 1
                else:
                    self.channel_msgid_seen.add(key)
            except (TypeError, ValueError):
                pass

        # === Analysis field profiling ===
        analysis = record.get("analysis")
        if analysis is not None and analysis != "" and analysis != {}:
            self.has_analysis += 1
            if ts_utc is not None:
                self.analysis_by_year[ts_utc.year] += 1
            if isinstance(analysis, dict):
                for k in analysis.keys():
                    self.analysis_keys[k] += 1
            else:
                self.analysis_non_dict += 1

    def _parse_utc_timestamp(self, record: dict[str, Any]) -> Optional[datetime]:
        ts_raw = record.get("timestamp")
        if ts_raw is not None:
            try:
                ts_float = float(ts_raw)
                if 1262304000 < ts_float < 1893456000:
                    return datetime.fromtimestamp(ts_float, tz=timezone.utc)
            except (TypeError, ValueError):
                pass

        dt_raw = record.get("datetime") or record.get("date")
        if isinstance(dt_raw, str):
            try:
                naive = datetime.fromisoformat(dt_raw.replace("Z", ""))
                if naive.tzinfo is None:
                    msk = timezone(timedelta(hours=3))
                    return naive.replace(tzinfo=msk).astimezone(timezone.utc)
                return naive.astimezone(timezone.utc)
            except ValueError:
                pass
        return None

    def _verify_tz(self, record: dict[str, Any], ts_utc_from_ts: datetime) -> None:
        dt_raw = record.get("datetime") or record.get("date")
        ts_raw = record.get("timestamp")
        if not isinstance(dt_raw, str) or ts_raw is None:
            return
        try:
            naive_dt = datetime.fromisoformat(dt_raw.replace("Z", ""))
            ts_float = float(ts_raw)
            ts_utc = datetime.fromtimestamp(ts_float, tz=timezone.utc)
            expected_naive_msk = ts_utc.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)
            delta_sec = int((naive_dt - expected_naive_msk).total_seconds())
            self.tz_offsets_sample.append(delta_sec)
            bucket = delta_sec // 3600
            self.tz_offset_buckets[bucket] += 1
            if abs(delta_sec) > 60:
                self.tz_mismatch_records += 1
        except (ValueError, TypeError):
            pass

    @staticmethod
    def _has_rtl_chars(text: str) -> bool:
        for c in text:
            cat = unicodedata.bidirectional(c)
            if cat in ("R", "AL"):
                return True
        return False

    @staticmethod
    def _has_control_chars(text: str) -> bool:
        for c in text:
            if ord(c) < 32 and c not in ("\n", "\r", "\t"):
                return True
        return False

    def to_dict(self) -> dict[str, Any]:
        elapsed = time.time() - self.start_time
        sorted_lengths = sorted(self.text_lengths) if self.text_lengths else [0]
        n = len(sorted_lengths)

        def pct(p: float) -> int:
            if n == 0:
                return 0
            idx = min(int(n * p), n - 1)
            return sorted_lengths[idx]

        events_per_month_all = {
            f"{y}-{m:02d}": cnt
            for (y, m), cnt in sorted(self.events_per_month.items())
        }

        tz_offset_str: dict[str, int] = {}
        for h, cnt in sorted(self.tz_offset_buckets.items()):
            key = f"{h:+d}" if h != 0 else "0"
            tz_offset_str[key] = cnt

        return {
            "source": str(SOURCE_PATH),
            "discovery_started_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_sec": round(elapsed, 1),

            "scale": {
                "total_lines": self.total_lines,
                "total_records_parsed": self.total_records,
                "total_bytes_raw": self.total_bytes,
                "total_mb": round(self.total_bytes / (1024 * 1024), 1),
                "avg_kb_per_record": round(self.total_bytes / max(self.total_records, 1) / 1024, 2),
                "text_bytes_total": self.text_bytes_total,
                "text_mb_total": round(self.text_bytes_total / (1024 * 1024), 1),
            },

            "errors": {
                "json_parse_errors": self.json_errors,
                "empty_text": self.empty_text,
                "empty_or_very_short_under_10": self.empty_or_very_short,
                "bad_unicode": self.bad_unicode,
            },

            "temporal_coverage": {
                "global_min": self.global_min_ts.isoformat() if self.global_min_ts else None,
                "global_max": self.global_max_ts.isoformat() if self.global_max_ts else None,
                "events_per_year": {str(y): cnt for y, cnt in sorted(self.events_per_year.items())},
                "events_per_month_all": events_per_month_all,
            },

            "channels": {
                "total_unique": len(self.channels),
                "events_by_channel": dict(self.channels.most_common()),
                "first_seen": {ch: dt.isoformat() for ch, dt in self.channel_first_seen.items()},
                "last_seen": {ch: dt.isoformat() for ch, dt in self.channel_last_seen.items()},
            },

            "text_distribution": {
                "buckets": dict(self.text_length_buckets),
                "min": min(self.text_lengths) if self.text_lengths else None,
                "p25": pct(0.25),
                "p50_median": pct(0.50),
                "p75": pct(0.75),
                "p95": pct(0.95),
                "p99": pct(0.99),
                "max": max(self.text_lengths) if self.text_lengths else None,
                "empty_text_count": self.empty_text,
                "very_long_count_4000plus": self.very_long,
                "headline_present": self.headline_present,
                "full_text_present": self.full_text_present,
            },

            "duplicates": {
                "unique_text_hashes": len(self.text_hash_seen),
                "duplicate_count": self.text_hash_duplicates,
                "duplicate_rate_pct": round(
                    100 * self.text_hash_duplicates / max(self.total_records, 1), 2
                ),
            },

            "edits": {
                "unique_channel_msgid_pairs": len(self.channel_msgid_seen),
                "likely_edits_count": self.likely_edits,
                "likely_edits_rate_pct": round(
                    100 * self.likely_edits / max(self.total_records, 1), 2
                ),
            },

            "schema": {
                "all_keys_seen": sorted(self.all_keys),
                "key_presence_pct": {
                    k: round(100 * c / max(self.total_records, 1), 2)
                    for k, c in self.key_presence.most_common()
                },
            },

            "tz_verification": {
                "records_with_both_dt_and_ts": len(self.tz_offsets_sample),
                "tz_offset_hours_distribution": tz_offset_str,
                "tz_mismatch_records_over_1min": self.tz_mismatch_records,
                "tz_mismatch_rate_pct": round(
                    100 * self.tz_mismatch_records / max(len(self.tz_offsets_sample), 1), 2
                ),
            },

            "special_chars": {
                "has_emoji": self.has_emoji,
                "has_emoji_pct": round(100 * self.has_emoji / max(self.total_records, 1), 2),
                "has_rtl": self.has_rtl,
                "has_control_chars": self.has_control_chars,
            },

            "analysis_field": {
                "has_analysis_count": self.has_analysis,
                "has_analysis_pct": round(
                    100 * self.has_analysis / max(self.total_records, 1), 2
                ),
                "non_dict_count": self.analysis_non_dict,
                "by_year": {str(y): cnt for y, cnt in sorted(self.analysis_by_year.items())},
                "top_level_keys": dict(self.analysis_keys.most_common(20)),
            },
        }


def discover(source: Path, limit: Optional[int] = None) -> StreamStats:
    if not source.exists():
        raise FileNotFoundError(f"Source not found: {source}")

    stats = StreamStats()
    print(f"Reading: {source}")
    print(f"  Size: {source.stat().st_size / (1024*1024):.1f} MB")
    print()

    with source.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            stats.total_lines += 1
            if limit is not None and stats.total_lines > limit:
                break

            raw_bytes = len(line.encode("utf-8", errors="replace"))
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                stats.json_errors += 1
                continue

            stats.update(record, raw_bytes)

            if stats.total_records % 50_000 == 0 and stats.total_records > 0:
                elapsed = time.time() - stats.start_time
                rate = stats.total_records / elapsed
                print(f"  {stats.total_records:>9,d} records  rate={rate:>7.0f}/s  elapsed={elapsed:.1f}s")

    return stats


def write_human_report(data: dict[str, Any], out_path: Path) -> None:
    lines: list[str] = []
    add = lines.append

    add("=" * 70)
    add("  Sprint 4 / Commit 4.2.a — Telegram News Dataset Discovery (v4)")
    add("=" * 70)
    add(f"  Source: {data['source']}")
    add(f"  Started: {data['discovery_started_at']}")
    add(f"  Elapsed: {data['elapsed_sec']}s")
    add("")

    s = data["scale"]
    add("-- SCALE --")
    add(f"  Total lines:           {s['total_lines']:>12,d}")
    add(f"  Records parsed:        {s['total_records_parsed']:>12,d}")
    add(f"  Total size:            {s['total_mb']:>12,.1f} MB")
    add(f"  Text size (sum):       {s['text_mb_total']:>12,.1f} MB")
    add(f"  Avg per record:        {s['avg_kb_per_record']:>12,.2f} KB")
    add("")

    e = data["errors"]
    add("-- ERRORS / DATA QUALITY --")
    add(f"  JSON parse errors:           {e['json_parse_errors']:>9,d}")
    add(f"  Empty text:                  {e['empty_text']:>9,d}")
    add(f"  Empty or very short (<10):   {e['empty_or_very_short_under_10']:>9,d}")
    add(f"  Bad unicode:                 {e['bad_unicode']:>9,d}")
    add("")

    t = data["temporal_coverage"]
    add("-- TEMPORAL COVERAGE --")
    add(f"  Global min: {t['global_min']}")
    add(f"  Global max: {t['global_max']}")
    add(f"  Events by year:")
    for y, n in t["events_per_year"].items():
        add(f"    {y}: {n:>9,d}")
    add("")
    add(f"  Events by month (all):")
    for ym, n in t["events_per_month_all"].items():
        add(f"    {ym}: {n:>9,d}")
    add("")

    c = data["channels"]
    add("-- CHANNELS --")
    add(f"  Total unique: {c['total_unique']}")
    add(f"  Events by channel:")
    for ch, n in c["events_by_channel"].items():
        first = c["first_seen"].get(ch, "?")[:10]
        last = c["last_seen"].get(ch, "?")[:10]
        add(f"    {ch:<25s}  {n:>9,d}  first={first} last={last}")
    add("")

    td = data["text_distribution"]
    add("-- TEXT LENGTH DISTRIBUTION --")
    add(f"  full_text present:  {td['full_text_present']:>9,d}")
    add(f"  headline present:   {td['headline_present']:>9,d}")
    add(f"  Buckets:")
    for bucket, n in td["buckets"].items():
        add(f"    {bucket:<12s}: {n:>9,d}")
    add(f"  Percentiles:")
    add(f"    min  = {td['min']}")
    add(f"    p25  = {td['p25']}")
    add(f"    p50  = {td['p50_median']}")
    add(f"    p75  = {td['p75']}")
    add(f"    p95  = {td['p95']}")
    add(f"    p99  = {td['p99']}")
    add(f"    max  = {td['max']}")
    add("")

    d = data["duplicates"]
    add("-- DUPLICATES (by SHA-256 of text) --")
    add(f"  Unique hashes:         {d['unique_text_hashes']:>9,d}")
    add(f"  Duplicate occurrences: {d['duplicate_count']:>9,d}")
    add(f"  Rate:                  {d['duplicate_rate_pct']}%")
    add("")

    ed = data["edits"]
    add("-- EDITED MESSAGES (by (channel, tg_msg_id) repeat) --")
    add(f"  Unique (channel, tg_msg_id) pairs:     {ed['unique_channel_msgid_pairs']:>9,d}")
    add(f"  Likely edits (subsequent same-key):    {ed['likely_edits_count']:>9,d}")
    add(f"  Rate:                                  {ed['likely_edits_rate_pct']}%")
    add("")

    sch = data["schema"]
    add("-- SCHEMA --")
    add(f"  All keys ({len(sch['all_keys_seen'])}): {sch['all_keys_seen']}")
    add(f"  Key presence (%):")
    for k, pct_v in sch["key_presence_pct"].items():
        add(f"    {k:<25s}: {pct_v:>6.2f}%")
    add("")

    tz = data["tz_verification"]
    add("-- TZ VERIFICATION (datetime vs timestamp) --")
    add(f"  Records with both fields: {tz['records_with_both_dt_and_ts']:>9,d}")
    add(f"  TZ offset distribution (hours):")
    for h_str, n in tz["tz_offset_hours_distribution"].items():
        marker = " <-- expected (datetime is MSK, timestamp is UTC)" if h_str == "0" else ""
        add(f"    {h_str:>4s}h: {n:>9,d}{marker}")
    add(f"  Mismatch records (>1min): {tz['tz_mismatch_records_over_1min']:>9,d} ({tz['tz_mismatch_rate_pct']}%)")
    add("")

    sc = data["special_chars"]
    add("-- SPECIAL CHARS --")
    add(f"  Has emoji:         {sc['has_emoji']:>9,d} ({sc['has_emoji_pct']}%)")
    add(f"  Has RTL chars:     {sc['has_rtl']:>9,d}")
    add(f"  Has control chars: {sc['has_control_chars']:>9,d}")
    add("")

    # NEW v4
    a = data["analysis_field"]
    add("-- ANALYSIS FIELD (legacy LLM enrichment) --")
    add(f"  Records with analysis:   {a['has_analysis_count']:>9,d} ({a['has_analysis_pct']}%)")
    add(f"  Non-dict analysis:       {a['non_dict_count']:>9,d}")
    add(f"  By year:")
    for y, n in a["by_year"].items():
        add(f"    {y}: {n:>9,d}")
    add(f"  Top-level keys (top 20):")
    for k, c_ in a["top_level_keys"].items():
        add(f"    {k:<25s}: {c_:>9,d}")
    add("")

    add("=" * 70)
    add("  Discovery complete. Next: commit 4.2.b — sampling plan.")
    add("=" * 70)

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Human report: {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--source", type=Path, default=SOURCE_PATH)
    args = parser.parse_args()

    OUT_DIR.mkdir(exist_ok=True, parents=True)

    stats = discover(args.source, limit=args.limit)
    report = stats.to_dict()

    json_path = OUT_DIR / "discovery_telegram_news.json"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\nJSON report: {json_path}")

    txt_path = OUT_DIR / "discovery_telegram_news_report.txt"
    write_human_report(report, txt_path)

    print()
    print("=" * 70)
    print("  Quick summary (v4)")
    print("=" * 70)
    print(f"  Total records:    {report['scale']['total_records_parsed']:,d}")
    print(f"  File size:        {report['scale']['total_mb']:.1f} MB")
    print(f"  Text content:     {report['scale']['text_mb_total']:.1f} MB")
    print(f"  Time period:      {report['temporal_coverage']['global_min']} .. {report['temporal_coverage']['global_max']}")
    print(f"  Channels:         {report['channels']['total_unique']}")
    print(f"  Median text len:  {report['text_distribution']['p50_median']} chars")
    print(f"  Duplicate rate:   {report['duplicates']['duplicate_rate_pct']}%")
    print(f"  Likely edits:     {report['edits']['likely_edits_rate_pct']}%")
    print(f"  Has analysis:     {report['analysis_field']['has_analysis_pct']}% ({report['analysis_field']['has_analysis_count']:,d})")
    print(f"  Elapsed:          {report['elapsed_sec']}s")


if __name__ == "__main__":
    main()