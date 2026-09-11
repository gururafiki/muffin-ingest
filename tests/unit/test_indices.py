"""The index-return rules — the two conversions that are silent when wrong, and the label map.

Both of the numeric traps here produce plausible-looking values rather than errors: a fraction
stored as a percent is a hundred times too small, and reading `performance_1d` yields no day return
at all because finviz populates `Change %` instead and leaves the obvious field null.
"""

from __future__ import annotations

from datetime import date

import pytest

from muffin_ingest.facets import indices


def test_performance_is_a_FRACTION_and_must_be_converted() -> None:
    """openbb reports `-0.0366` for -3.66%. Storing it raw gives numbers a hundred times too
    small that still render as plausible — the confusion that once put NVIDIA at a 46% dividend
    yield on a deployed page."""
    assert indices.to_percent(-0.0366) == pytest.approx(-3.66)
    assert indices.to_percent(0.1918) == pytest.approx(19.18)
    assert indices.to_percent(0) == 0.0


def test_a_non_number_is_nothing_rather_than_a_zero() -> None:
    """A zero is a RETURN. Coercing an absent field into one states that a sector did not move,
    which is a different claim from not knowing."""
    for value in (None, "0.1", True, float("nan")):
        assert indices.to_percent(value) is None, value


def test_the_day_change_is_under_Change_percent_and_performance_1d_is_always_null() -> None:
    """MEASURED ON ALL ELEVEN SECTORS: `performance_1d` populated on **0 of 11**, `Change %` on
    **11 of 11**.

    A field that exists, is named exactly what you want, and is never filled is the worst shape
    there is — reading it yields no 1d return at all, with nothing anywhere saying why.
    """
    assert indices.FINVIZ_PERIODS["Change %"] == "1d"
    assert "performance_1d" not in indices.FINVIZ_PERIODS


def test_there_is_no_3y_or_5y_for_a_sector_and_that_is_deliberate() -> None:
    """finviz publishes 1d/1w/1m/3m/6m/ytd/1y and nothing longer. A 3y computed from a shorter
    window would be a different number wearing the period's name."""
    assert set(indices.FINVIZ_PERIODS.values()) == {"1d", "1w", "1m", "3m", "6m", "ytd", "1y"}


def test_every_finviz_sector_name_maps_and_none_is_GICS() -> None:
    """finviz does NOT use GICS names, which is the entire reason the map exists. All eleven of the
    labels the provider actually returned are covered."""
    observed = [
        "Basic Materials",
        "Communication Services",
        "Consumer Cyclical",
        "Consumer Defensive",
        "Energy",
        "Financial",
        "Healthcare",
        "Industrials",
        "Real Estate",
        "Technology",
        "Utilities",
    ]
    assert all(label in indices.SECTOR_LABELS for label in observed)
    assert len({indices.SECTOR_LABELS[label] for label in observed}) == 11, "1:1, no collisions"


def test_yfinance_s_label_for_the_same_sector_lands_in_the_same_bucket() -> None:
    """The two vocabularies differ by exactly one label: yfinance says "Financial Services" where
    finviz says "Financial". One map means a security classified from a profile and a sector read
    from the sector endpoint end up in the same bucket — the whole point of having sector ids."""
    assert indices.SECTOR_LABELS["Financial Services"] == indices.SECTOR_LABELS["Financial"]


def test_an_unmapped_label_is_REPORTED_and_never_filed_under_a_guess() -> None:
    """A provider rename should degrade one sector, not file its performance under a neighbour's
    name — which is worse than losing it, because the wrong number looks right."""
    rows = [
        {"provider_label": "Technology", "period_code": "1m", "fraction": 0.021},
        {"provider_label": "Cybernetics", "period_code": "1m", "fraction": 0.5},
    ]
    out, unmapped = indices.normalise_sectors(rows, as_of=date(2026, 9, 10))

    assert unmapped == ["Cybernetics"]
    assert [r["index_code"] for r in out] == ["sector:information-technology"]
    assert out[0]["price_return_pct"] == pytest.approx(2.1)


def test_a_sector_s_total_return_is_NULL_rather_than_its_price_return() -> None:
    """finviz publishes price performance only. Filling the column with the price return erases the
    difference between "paid no income" and "we did not measure it" — and a reader comparing a
    sector's total return with a security's would be comparing two different quantities."""
    out, _ = indices.normalise_sectors(
        [{"provider_label": "Energy", "period_code": "1y", "fraction": 0.3}],
        as_of=date(2026, 9, 10),
    )
    assert out[0]["total_return_pct"] is None
    assert out[0]["price_return_pct"] == pytest.approx(30.0)


def test_the_raw_artifact_keeps_the_PROVIDER_S_label_not_our_id() -> None:
    """Mapping is interpretation and belongs in stage 2. Keeping the raw label means a rename is
    diagnosable from the bytes on disk rather than by asking the provider again."""
    rows = indices.sector_rows(
        [{"name": "Basic Materials", "Change %": 0.0042, "performance_1y": 0.2978}], run_id="r1"
    )
    assert {r["provider_label"] for r in rows} == {"Basic Materials"}
    assert {r["period_code"] for r in rows} == {"1d", "1y"}
    assert all(r["run_id"] == "r1" for r in rows)


def test_a_row_with_no_name_is_skipped_rather_than_attributed_to_nothing() -> None:
    assert indices.sector_rows([{"Change %": 0.01}], run_id="r1") == []
