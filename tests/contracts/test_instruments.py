"""
tests/contracts/test_instruments.py — Unit-тесты Sprint 4 / Commit 4.1
=========================================================================

Покрытие:
  - normalize_ticker: легаси -> canonical, idempotent, неизвестный -> KeyError
  - try_normalize_ticker: soft версия с None
  - is_known_ticker, get_instrument_meta, get_lot_size
  - Инварианты реестра: уникальность legacy_phase2, отсутствие коллизий
  - lot_size значения (из Phase 2 backtest_mfe.py)
  - is_usd_denominated для BR/NG/GLDRUB
"""

from __future__ import annotations

import pytest

from src.contracts.instruments import (
    ALL_KNOWN_NAMES,
    CANONICAL_TICKERS,
    INSTRUMENTS,
    InstrumentMeta,
    all_canonical_tickers,
    get_instrument_meta,
    get_lot_size,
    is_known_ticker,
    is_usd_denominated,
    normalize_ticker,
    rouble_denominated_tickers,
    try_normalize_ticker,
)


# =============================================================================
# Legacy → Canonical normalization
# =============================================================================
class TestNormalizeTicker:

    @pytest.mark.parametrize("legacy,canonical", [
        ("Si", "SI"),       # case-sensitive!
        ("MX", "MIX"),
        ("YNDX", "YDEX"),
        ("GOLD", "GLDRUB"),
    ])
    def test_legacy_phase2_names_normalize(self, legacy, canonical):
        assert normalize_ticker(legacy) == canonical

    @pytest.mark.parametrize("canonical", [
        "SBER", "GAZP", "LKOH", "YDEX", "MIX", "SI", "BR", "NG",
        "GLDRUB", "USDRUB", "CNY", "VTBR", "MGNT",
    ])
    def test_canonical_names_are_idempotent(self, canonical):
        """Передача canonical имени должна возвращать его же."""
        assert normalize_ticker(canonical) == canonical

    def test_unknown_ticker_raises_keyerror(self):
        with pytest.raises(KeyError, match="Unknown ticker"):
            normalize_ticker("XYZW")

    def test_unknown_ticker_message_has_hint(self):
        with pytest.raises(KeyError) as exc:
            normalize_ticker("MOEX")
        # сообщение содержит подсказки про canonical и legacy
        msg = str(exc.value)
        assert "Canonical" in msg
        assert "Legacy" in msg

    def test_empty_string_raises(self):
        with pytest.raises(KeyError):
            normalize_ticker("")

    def test_case_sensitive_si_not_SI(self):
        """Si (легаси) -> SI; sber (lowercase) -> KeyError."""
        assert normalize_ticker("Si") == "SI"
        with pytest.raises(KeyError):
            normalize_ticker("sber")  # lowercase != "SBER"


class TestTryNormalizeTicker:

    def test_known_returns_canonical(self):
        assert try_normalize_ticker("Si") == "SI"
        assert try_normalize_ticker("SI") == "SI"
        assert try_normalize_ticker("GAZP") == "GAZP"

    def test_unknown_returns_none(self):
        """Soft version не бросает исключение."""
        assert try_normalize_ticker("XYZW") is None
        assert try_normalize_ticker("MOEX") is None
        assert try_normalize_ticker("") is None


class TestIsKnownTicker:

    @pytest.mark.parametrize("ticker", ["SI", "Si", "MX", "MIX", "YDEX", "YNDX", "GAZP"])
    def test_known_tickers(self, ticker):
        assert is_known_ticker(ticker) is True

    @pytest.mark.parametrize("ticker", ["XYZW", "AAPL", "TSLA", "", "MOEX"])
    def test_unknown_tickers(self, ticker):
        assert is_known_ticker(ticker) is False


# =============================================================================
# Phase 2 lot_size values (источник: backtest_mfe.py)
# =============================================================================
class TestLotSize:
    """Зафиксированные lot_size из Phase 2."""

    @pytest.mark.parametrize("ticker,expected_lot", [
        ("SBER", 10),
        ("GAZP", 10),
        ("ROSN", 10),
        ("MTSS", 10),
        ("VTBR", 10000),     # особый случай!
        ("USDRUB", 1000),    # особый случай!
        ("LKOH", 1),
        ("GMKN", 1),
        ("YDEX", 1),
        ("MIX", 1),
        ("SI", 1),
        ("BR", 1),
        ("NG", 1),
        ("GLDRUB", 1),
        ("CNY", 1),
    ])
    def test_lot_size_by_canonical(self, ticker, expected_lot):
        assert get_lot_size(ticker) == expected_lot

    @pytest.mark.parametrize("legacy,expected_lot", [
        ("Si", 1),
        ("MX", 1),
        ("YNDX", 1),
        ("GOLD", 1),
    ])
    def test_lot_size_works_with_legacy_names(self, legacy, expected_lot):
        """get_lot_size принимает и legacy имена."""
        assert get_lot_size(legacy) == expected_lot


# =============================================================================
# USD-denominated flag
# =============================================================================
class TestIsUsdDenominated:

    @pytest.mark.parametrize("ticker", ["BR", "NG", "GLDRUB"])
    def test_usd_denominated_assets(self, ticker):
        assert is_usd_denominated(ticker) is True

    @pytest.mark.parametrize("ticker", ["SBER", "GAZP", "USDRUB", "CNY", "MIX", "SI"])
    def test_rouble_denominated_assets(self, ticker):
        """USDRUB сам — рублёвый фьючерс на USD (PnL в рублях), не USD-denominated."""
        assert is_usd_denominated(ticker) is False

    def test_usd_denominated_via_legacy_name(self):
        """GOLD (легаси) -> GLDRUB -> usd_denominated."""
        assert is_usd_denominated("GOLD") is True


# =============================================================================
# get_instrument_meta
# =============================================================================
class TestGetInstrumentMeta:

    def test_returns_instrumentmeta(self):
        meta = get_instrument_meta("SBER")
        assert isinstance(meta, InstrumentMeta)
        assert meta.canonical == "SBER"
        assert meta.legacy_phase2 == "SBER"
        assert meta.csv_file == "prices_SBER.csv"
        assert meta.asset_class == "equity"

    def test_legacy_returns_canonical_meta(self):
        """get_instrument_meta('Si') возвращает мету для SI."""
        meta = get_instrument_meta("Si")
        assert meta.canonical == "SI"
        assert meta.legacy_phase2 == "Si"

    def test_unknown_raises(self):
        with pytest.raises(KeyError):
            get_instrument_meta("XYZW")


# =============================================================================
# Инварианты реестра
# =============================================================================
class TestRegistryInvariants:

    def test_19_instruments_total(self):
        assert len(INSTRUMENTS) == 19

    def test_canonical_names_unique(self):
        assert len(INSTRUMENTS) == len(set(INSTRUMENTS.keys()))

    def test_legacy_phase2_names_unique(self):
        """Не должно быть двух canonical с одинаковым legacy_phase2."""
        legacy_names = [m.legacy_phase2 for m in INSTRUMENTS.values()]
        assert len(legacy_names) == len(set(legacy_names))

    def test_no_canonical_legacy_collision(self):
        """Никакое canonical != legacy_phase2 НЕ должно быть также canonical другого."""
        for canonical, meta in INSTRUMENTS.items():
            if meta.legacy_phase2 != canonical:
                assert meta.legacy_phase2 not in INSTRUMENTS, (
                    f"Collision: legacy '{meta.legacy_phase2}' of '{canonical}' "
                    f"is also a canonical name"
                )

    def test_canonical_tickers_set_matches_dict(self):
        assert CANONICAL_TICKERS == frozenset(INSTRUMENTS.keys())

    def test_all_known_includes_both_canonical_and_legacy(self):
        for canonical in INSTRUMENTS.keys():
            assert canonical in ALL_KNOWN_NAMES
        for canonical, meta in INSTRUMENTS.items():
            if meta.legacy_phase2 != canonical:
                assert meta.legacy_phase2 in ALL_KNOWN_NAMES

    def test_lot_sizes_positive_integers(self):
        for canonical, meta in INSTRUMENTS.items():
            assert isinstance(meta.lot_size, int)
            assert meta.lot_size > 0

    def test_asset_class_values_known(self):
        valid_classes = {"equity", "futures", "commodity", "currency"}
        for canonical, meta in INSTRUMENTS.items():
            assert meta.asset_class in valid_classes


# =============================================================================
# Helper functions
# =============================================================================
class TestHelperFunctions:

    def test_all_canonical_tickers_returns_19(self):
        assert len(all_canonical_tickers()) == 19

    def test_all_canonical_tickers_only_canonical(self):
        """Не возвращает legacy имена."""
        canonicals = all_canonical_tickers()
        assert "YNDX" not in canonicals   # legacy
        assert "YDEX" in canonicals       # canonical
        assert "MX" not in canonicals
        assert "MIX" in canonicals
        assert "Si" not in canonicals
        assert "SI" in canonicals

    def test_rouble_denominated_excludes_usd(self):
        """USD-denominated БР/НГ/GLDRUB не в списке rouble."""
        rouble = rouble_denominated_tickers()
        assert "BR" not in rouble
        assert "NG" not in rouble
        assert "GLDRUB" not in rouble
        assert "SBER" in rouble
        assert "GAZP" in rouble
        # USDRUB — рублёвый (lot=1000, но это разница в множителе, не в денежной единице PnL)
        assert "USDRUB" in rouble

    def test_rouble_denominated_has_16(self):
        """19 total — 3 USD = 16 rouble."""
        assert len(rouble_denominated_tickers()) == 16


# =============================================================================
# Smoke-test особых случаев
# =============================================================================
class TestEdgeCases:

    def test_ydex_available_from_2024(self):
        """YDEX появился после reorganization 2024-07-24."""
        from datetime import date
        meta = get_instrument_meta("YDEX")
        assert meta.available_from == date(2024, 7, 24)

    def test_gldrub_available_from_2023(self):
        from datetime import date
        meta = get_instrument_meta("GLDRUB")
        assert meta.available_from == date(2023, 7, 12)

    def test_yndx_legacy_routes_to_ydex(self):
        """Legacy YNDX -> canonical YDEX."""
        assert normalize_ticker("YNDX") == "YDEX"
        meta_yndx = get_instrument_meta("YNDX")
        meta_ydex = get_instrument_meta("YDEX")
        assert meta_yndx is meta_ydex  # Тот же объект (один и тот же реестр)

    def test_normalize_idempotent_double_call(self):
        """normalize(normalize(x)) == normalize(x)."""
        for name in ["Si", "MX", "GOLD", "YNDX", "SBER", "GAZP"]:
            once = normalize_ticker(name)
            twice = normalize_ticker(once)
            assert once == twice
