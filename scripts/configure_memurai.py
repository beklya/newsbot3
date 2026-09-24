"""
Configure Memurai for production trading workload.

Применяет production-настройки к запущенному Memurai/Redis:
    - appendonly yes               (AOF persistence — данные не теряются при крэше)
    - appendfsync everysec         (fsync раз в сек — баланс скорость/надёжность)
    - maxmemory 2gb                (лимит памяти)
    - maxmemory-policy noeviction  (КРИТИЧНО: не выкидывать сообщения молча!)
    - save 300 1                   (RDB snapshot каждые 5 мин при ≥1 изменении)

Применение через CONFIG SET (без рестарта сервиса).
Сохранение в memurai.conf через CONFIG REWRITE (best-effort).

Usage:
    python scripts\\configure_memurai.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import redis


# Production settings для торгового бота
PRODUCTION_SETTINGS = [
    # Persistence
    ("appendonly", "yes"),
    ("appendfsync", "everysec"),

    # Memory
    ("maxmemory", "2gb"),
    ("maxmemory-policy", "noeviction"),

    # RDB snapshots (дополнительно к AOF)
    ("save", "300 1 60 10000"),  # каждые 5 мин при ≥1 измен., каждую мин при ≥10k
]


def show_current_settings(r: redis.Redis) -> dict:
    """Получить и распечатать текущие настройки."""
    print("\n--- Current settings ---")
    current = {}
    for key, _ in PRODUCTION_SETTINGS:
        value = r.config_get(key)
        # config_get возвращает {key: value}
        current[key] = value.get(key, "<not set>")
        print(f"  {key:25s} = {current[key]}")
    return current


def apply_settings(r: redis.Redis) -> int:
    """Применить production-настройки. Возвращает количество успешных."""
    print("\n--- Applying production settings ---")
    applied = 0
    for key, target_value in PRODUCTION_SETTINGS:
        try:
            r.config_set(key, target_value)
            print(f"  [OK] {key:25s} -> {target_value}")
            applied += 1
        except redis.RedisError as e:
            print(f"  [FAIL] {key:25s} -> {target_value} ({e})")
    return applied


def verify_settings(r: redis.Redis) -> int:
    """Проверить, что настройки реально применены."""
    print("\n--- Verifying applied settings ---")
    correct = 0
    for key, expected in PRODUCTION_SETTINGS:
        actual = r.config_get(key).get(key, "<missing>")
        # save может возвращаться в немного другом формате — нормализуем
        match = (str(actual).strip() == str(expected).strip())
        marker = "[OK]" if match else "[MISMATCH]"
        print(f"  {marker} {key:25s} expected={expected!r:30s} actual={actual!r}")
        if match:
            correct += 1
    return correct


def try_rewrite(r: redis.Redis) -> bool:
    """Попытаться сохранить настройки в memurai.conf."""
    print("\n--- Persisting to memurai.conf via CONFIG REWRITE ---")
    try:
        r.config_rewrite()
        print("  [OK] CONFIG REWRITE succeeded — settings will survive restart")
        return True
    except redis.RedisError as e:
        print(f"  [WARN] CONFIG REWRITE failed: {e}")
        print("  Settings are LIVE, but will RESET on service restart.")
        print("  Manual fix below.")
        return False


def print_manual_instructions():
    """Инструкции для ручного редактирования memurai.conf."""
    print("\n--- Manual fix (if REWRITE failed) ---")
    print("  1. Stop service (admin cmd):  sc stop Memurai")
    print("  2. Open file in editor:")
    print("     notepad D:\\quik_sber\\Memurai\\memurai.conf")
    print("  3. Find/add these lines (anywhere in file):")
    for key, value in PRODUCTION_SETTINGS:
        print(f"     {key} {value}")
    print("  4. Save & close")
    print("  5. Start service:  sc start Memurai")


def main() -> int:
    print("=" * 70)
    print("Memurai Production Configuration")
    print("=" * 70)

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    try:
        pong = r.ping()
        print(f"\nMemurai PING: {pong}")
        if not pong:
            print("[ERROR] Memurai not responding. Run: sc start Memurai")
            return 1

        # Server info
        info = r.info("server")
        print(f"Memurai version: {info.get('memurai_version', 'unknown')}")
        print(f"Redis compat:    {info.get('redis_version', 'unknown')}")
        print(f"Uptime:          {info.get('uptime_in_seconds', 0)} sec")

        show_current_settings(r)
        applied = apply_settings(r)
        correct = verify_settings(r)

        rewrite_ok = try_rewrite(r)

        # Summary
        print("\n" + "=" * 70)
        print(f"Applied: {applied}/{len(PRODUCTION_SETTINGS)}")
        print(f"Verified correct: {correct}/{len(PRODUCTION_SETTINGS)}")
        print(f"Persisted to file: {'YES' if rewrite_ok else 'NO (manual edit needed)'}")
        print("=" * 70)

        if not rewrite_ok:
            print_manual_instructions()
            return 2

        if correct < len(PRODUCTION_SETTINGS):
            print("[WARN] Some settings did not apply correctly. Check output above.")
            return 3

        print("\n[OK] Memurai is configured for production trading workload.")
        return 0

    finally:
        r.close()


if __name__ == "__main__":
    sys.exit(main())
