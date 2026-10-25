"""The chart lane, driven end to end over Yahoo's own bytes and a fake database.

Driven rather than inspected, like the other price tests: each defect this lane exists to prevent —
a label guessed rather than read, a throttle recorded as an absence, a history extended across a
symbol change, a split left in the stored closes — is invisible to a reading of the code. The
bodies are the captures in the library's `tests/fixtures` (`capture.md`, 2026-10-10).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.providers import yahoo_chart
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.prices import checks as prices_checks
from muffin_ingest_dagster.defs.prices import core as prices_core
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.defs.prices import raw as prices_raw
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from tests import FIXTURES

from . import test_price_assets as fakes

AAPL = ("11111111-1111-1111-1111-111111111111", "AAPL", 5.0)
VOD = ("22222222-2222-2222-2222-222222222222", "VOD.L", 4.0)
ALG = ("33333333-3333-3333-3333-333333333333", "ALG.KW", 3.0)
TOYOTA = ("44444444-4444-4444-4444-444444444444", "7203.T", 2.0)
DEAD = ("55555555-5555-5555-5555-555555555555", "BDMS-F.BK", 1.0)
QUOTE_ONLY = ("66666666-6666-6666-6666-666666666666", "PCTN.L", 0.5)

BODIES = {
    "AAPL": "aapl_ext",
    "VOD.L": "vod_l_ext",
    "ALG.KW": "alg_kw_week",
    "7203.T": "7203_t_ext",
    "BDMS-F.BK": "bdms_f_bk_404",
    "PCTN.L": "pctn_l_quote_only",
}


def body(name: str) -> bytes:
    return (FIXTURES / f"yahoo_chart_{name}.body").read_bytes()


def with_split(content: bytes, day: date) -> bytes:
    """A captured body with a split event added on `day`, stamped like Yahoo's: at the session's
    open, 09:30 in New York. A midnight-UTC stamp would fall on the day before in UTC-4."""
    payload = json.loads(content)
    stamp = int(datetime(day.year, day.month, day.day, 13, 30, tzinfo=UTC).timestamp())
    payload["chart"]["result"][0]["events"] = {
        "splits": {str(stamp): {"date": stamp, "numerator": 2, "denominator": 1}}
    }
    return json.dumps(payload).encode()


Answer = bytes | Exception | Callable[[dict[str, Any]], "bytes | Exception"]


@contextmanager
def yahoo(answers: dict[str, Answer]) -> Iterator[list[tuple[str, dict[str, Any]]]]:
    """Yahoo answering each symbol from `answers`, recording every request it was sent."""
    calls: list[tuple[str, dict[str, Any]]] = []

    def fetch(symbol: str, **kwargs: Any) -> Document:
        calls.append((symbol, kwargs))
        answer = answers[symbol]
        if callable(answer) and not isinstance(answer, Exception):
            answer = answer(kwargs)
        if isinstance(answer, Exception):
            raise answer
        return Document(
            url=f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?p={kwargs.get('period1')}",
            body=answer,
            content_type="application/json",
            fetched_at=datetime.now(UTC),
        )

    saved = yahoo_chart.fetch
    yahoo_chart.fetch = fetch
    try:
        yield calls
    finally:
        yahoo_chart.fetch = saved


class _Written(dg.ConfigurableIOManager):
    """Stands in for `PostgresIOManager`: what stage 2 produced, captured rather than written."""

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        WRITTEN.extend(obj)

    def load_input(self, context: dg.InputContext) -> Any:
        raise NotImplementedError


WRITTEN: list[dict[str, Any]] = []


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(prices_partitions, "CHART_SECONDS_BETWEEN_REQUESTS", 0.0)
    WRITTEN.clear()
    fakes.PROBES.clear()
    fakes.RETRACTIONS.clear()
    fakes.STORED_LABELS.clear()
    fakes.ACTION_RETRACTIONS.clear()
    saved = list(fakes.UNIVERSE)
    yield
    fakes.UNIVERSE[:] = saved
    fakes.STORED_LABELS.clear()


def night(
    tmp_path: Path,
    subjects: list[tuple[str, str, float]],
    *,
    stage_2: bool = True,
    check: bool = False,
    actions: bool = False,
    askable: list[tuple[str, str, float]] | None = None,
    as_range: bool = False,
) -> dg.ExecuteInProcessResult:
    """One run of the nightly job over `subjects` as a range, the shape a night's slice takes.

    `askable` narrows the universe the run may ask about (default: every subject), and `as_range`
    sends a single partition as a range of one, which is how a scattered backfill sends it.
    """
    fakes.UNIVERSE[:] = subjects if askable is None else askable
    keys = [sid for sid, _, _ in subjects]
    single = len(keys) == 1 and not as_range
    assets: list[Any] = [prices_raw.raw_price_chart]
    if stage_2:
        assets.append(prices_core.price_bar_history)
    if actions:
        assets.append(prices_core.yahoo_corporate_action)
    if check:
        assets.append(prices_checks.a_bar_s_label_is_its_listing_s)
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, keys)
        tags = {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]}
        return dg.materialize(
            assets,
            instance=instance,
            partition_key=keys[0] if single else None,
            tags=None if single else tags,
            resources={
                "postgres": fakes.FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres_io": _Written(),
            },
        )


def meta(result: dg.ExecuteInProcessResult, node: str) -> dict[str, Any]:
    events = result.asset_materializations_for_node(node)
    return {k: v.value for k, v in events[0].metadata.items()}


def stored(tmp_path: Path, sid: str) -> list[tuple[Any, ...]]:
    path = tmp_path / "raw_price_chart" / f"{sid}.parquet"
    return duckdb.sql(
        f"select asked_symbol, period1 from read_parquet('{path}') order by fetched_at"
    ).fetchall()


def outcomes_sum(m: dict[str, Any]) -> int:
    return sum(
        m[k]
        for k in (
            "answered",
            "empty",
            "absent",
            "quote_only",
            "not_daily",
            "unreadable",
            "transport",
            "unasked",
            "not_askable",
        )
    )


# --- a first visit, and the one after it ----------------------------------------------------------


def test_a_first_visit_asks_for_the_whole_history_by_dates(tmp_path: Path) -> None:
    subjects = [AAPL, VOD, ALG, TOYOTA]
    with yahoo({s: body(BODIES[s]) for _, s, _ in subjects}) as calls:
        result = night(tmp_path, subjects)

    assert result.success
    assert sorted(symbol for symbol, _ in calls) == sorted(s for _, s, _ in subjects)
    for _, kwargs in calls:
        # `period1=0`, NEVER `range=max`, which Yahoo answers quarterly.
        assert kwargs["period1"] == 0 and "range_" not in kwargs
        assert kwargs["period2"] == yahoo_chart.day_start(date.today())
        assert (kwargs["interval"], kwargs["events"], kwargs["adjusted"]) == (
            "1d",
            "div,split",
            True,
        )
        # No URL is asked twice, so http-cache is told not to keep the body.
        assert kwargs["store"] is False

    m = meta(result, "raw_price_chart")
    assert (m["plan_never_loaded"], m["answered"], m["documents"]) == (4, 4, 4)
    assert outcomes_sum(m) == m["requested"] == 4, "every subject is in exactly one outcome"
    for sid, symbol, _ in subjects:
        assert stored(tmp_path, sid) == [(symbol, 0)], "one document, filed under its security"
    assert sorted((p["asked_with"], p["outcome"]) for p in fakes.PROBES) == sorted(
        (s, "hit") for _, s, _ in subjects
    )


def test_each_security_s_bars_carry_its_own_quote_currency(tmp_path: Path) -> None:
    """The guess this replaces said EUR for VOD.L and KWD for ALG.KW (measured 2026-10-10)."""
    subjects = [AAPL, VOD, ALG, TOYOTA]
    with yahoo({s: body(BODIES[s]) for _, s, _ in subjects}):
        result = night(tmp_path, subjects)

    by_security: dict[str, set[str | None]] = {}
    for row in WRITTEN:
        by_security.setdefault(row["security_id"], set()).add(row["currency_code"])
    assert by_security == {AAPL[0]: {"USD"}, VOD[0]: {"GBX"}, ALG[0]: {"KWF"}, TOYOTA[0]: {"JPY"}}
    assert meta(result, "price_bar_history")["bars"] == len(WRITTEN) == 10 + 10 + 15 + 10


def test_a_subject_the_sweep_cannot_ask_is_counted_so_the_outcomes_sum_to_requested(
    tmp_path: Path,
) -> None:
    """EVERY PARTITION IN THE RUN LANDS IN ONE COUNTER, including one nobody asks about.

    The nightly sweep names a contiguous slice of the grid, and `askable_subjects` leaves out a
    security whose symbol was rejected alone within 30 days. Measured 2026-09-30 on the openbb lane:
    `requested 2500`, `answered 2463`, and no other outcome, so 37 securities were accounted for
    nowhere. That hole looks like the throttle-branch defect, so without its own counter a correct
    exclusion reads as a counting bug and a real one can hide inside it. Moved here when that lane
    was deleted (2026-10-24); no chart test had a partition outside the askable universe.
    """
    with yahoo({"AAPL": body(BODIES["AAPL"])}) as calls:
        result = night(tmp_path, [AAPL, VOD], stage_2=False, askable=[AAPL])

    assert result.success
    assert [symbol for symbol, _ in calls] == ["AAPL"], "a subject it may not ask is not asked"
    m = meta(result, "raw_price_chart")
    assert (m["requested"], m["answered"], m["not_askable"]) == (2, 1, 1)
    assert outcomes_sum(m) == m["requested"]


def test_a_range_of_exactly_one_partition_is_written_as_one_partition(tmp_path: Path) -> None:
    """A RANGE OF ONE IS NOT A RANGE, AND THE I/O MANAGER DISAGREES ABOUT WHICH IT IS.

    `BackfillPolicy.multi_run` groups CONTIGUOUS partitions, so a backfill whose keys are scattered
    through the partition set produces ONE RUN PER PARTITION, each carrying a `partition_key_range`
    whose start equals its end. `partitioned.by_partition` once saw the range and returned a
    mapping while `UPathIOManager` saw one partition and handed the mapping straight to the writer:
    `AttributeError: 'str' object has no attribute 'get'`, naming neither the partition nor the
    shape. Measured in production, 250 requested partitions became 250 single-partition runs and
    the first three failed this way. Moved here from the openbb lane's replay (2026-10-24).
    """
    with yahoo({"AAPL": body(BODIES["AAPL"])}):
        result = night(tmp_path, [AAPL], stage_2=False, as_range=True)

    assert result.success, "a one-partition range must write as a single partition"
    written = sorted(p.stem for p in (tmp_path / "raw_price_chart").glob("*.parquet"))
    assert written == [AAPL[0]]
    assert stored(tmp_path, AAPL[0]) == [("AAPL", 0)], "the document, not a serialised mapping"


def test_the_next_visit_extends_from_the_newest_point_less_a_week(tmp_path: Path) -> None:
    with yahoo({"AAPL": body("aapl_ext")}):
        night(tmp_path, [AAPL])
    with yahoo({"AAPL": body("aapl_ext")}) as calls:
        result = night(tmp_path, [AAPL])

    newest = date(2026, 10, 9)  # the fixture's last bar
    assert calls[0][1]["period1"] == yahoo_chart.day_start(newest - timedelta(days=7))
    assert meta(result, "raw_price_chart")["plan_extend"] == 1
    assert stored(tmp_path, AAPL[0]) == [
        ("AAPL", 0),
        ("AAPL", yahoo_chart.day_start(newest - timedelta(days=7))),
    ], "an extension is appended, never written over the history"


def test_a_symbol_change_loads_the_new_listing_whole_and_replaces_the_old(tmp_path: Path) -> None:
    otc = (AAPL[0], "ASMLF", AAPL[2])
    with yahoo({"ASMLF": body("aapl_ext")}):
        night(tmp_path, [otc])
    with yahoo({"AAPL": body("aapl_ext")}) as calls:
        result = night(tmp_path, [AAPL])

    assert calls[0][1]["period1"] == 0
    assert meta(result, "raw_price_chart")["plan_symbol_changed"] == 1
    assert stored(tmp_path, AAPL[0]) == [("AAPL", 0)], "the other listing's history is gone"


def test_a_reload_that_names_an_absence_replaces_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ONLY A FULL HISTORY THAT ANSWERED REPLACES THE FILE. A reload is due, Yahoo answers with its
    404, and writing that over the stored history would delete years of documents to record one
    bad answer. It is appended instead, and the history stands."""
    from muffin_ingest.facets import price_chart

    with yahoo({"AAPL": body("aapl_ext")}):
        night(tmp_path, [AAPL])
    monkeypatch.setattr(price_chart, "RELOAD_AFTER", timedelta(0))
    WRITTEN.clear()
    with yahoo({"AAPL": body("bdms_f_bk_404")}) as calls:
        result = night(tmp_path, [AAPL])

    assert calls[0][1]["period1"] == 0, "the reload asked for everything"
    assert meta(result, "raw_price_chart")["plan_reload_due"] == 1
    assert stored(tmp_path, AAPL[0]) == [("AAPL", 0), ("AAPL", 0)], "kept, then the 404 beside it"
    assert len(WRITTEN) == 10, "and stage 2 still publishes the stored history"


# --- what may and may not become a miss -----------------------------------------------------------


def test_a_named_absence_beside_an_answer_is_a_miss(tmp_path: Path) -> None:
    with yahoo({"AAPL": body("aapl_ext"), "BDMS-F.BK": body("bdms_f_bk_404")}):
        result = night(tmp_path, [AAPL, DEAD])

    m = meta(result, "raw_price_chart")
    assert (m["absent"], m["dead"], m["control_calls"]) == (1, 1, 0)
    assert sorted((p["asked_with"], p["outcome"]) for p in fakes.PROBES) == [
        ("AAPL", "hit"),
        ("BDMS-F.BK", "miss"),
    ]


def test_an_absence_alone_is_a_miss_only_once_the_control_answers(tmp_path: Path) -> None:
    with yahoo({"BDMS-F.BK": body("bdms_f_bk_404"), "AAPL": body("aapl_ext")}) as calls:
        result = night(tmp_path, [DEAD])

    assert [s for s, _ in calls] == ["BDMS-F.BK", "AAPL"], "the control is asked after the subject"
    m = meta(result, "raw_price_chart")
    assert (m["dead"], m["control_calls"]) == (1, 1)
    assert [(p["asked_with"], p["outcome"]) for p in fakes.PROBES] == [("BDMS-F.BK", "miss")]


def test_a_listing_answered_only_with_its_live_quote_is_a_miss(tmp_path: Path) -> None:
    """Picton's document, as Yahoo returned it on 2026-10-22: one point, stamped at
    `regularMarketTime`. Counted as answered, it stayed in the rotation for ever with nothing
    published; it is an absence, a miss once the provider is shown healthy (here by AAPL)."""
    with yahoo({"AAPL": body("aapl_ext"), "PCTN.L": body("pctn_l_quote_only")}):
        result = night(tmp_path, [AAPL, QUOTE_ONLY])

    m = meta(result, "raw_price_chart")
    assert (m["answered"], m["quote_only"], m["dead"]) == (1, 1, 1)
    assert sorted((p["asked_with"], p["outcome"]) for p in fakes.PROBES) == [
        ("AAPL", "hit"),
        ("PCTN.L", "miss"),
    ]
    assert outcomes_sum(m) == m["requested"]


def test_an_absence_with_a_control_that_does_not_answer_marks_nothing(tmp_path: Path) -> None:
    refused = yahoo_chart.YahooRefused("HTTP 503 for AAPL")
    with yahoo({"BDMS-F.BK": body("bdms_f_bk_404"), "AAPL": refused}):
        result = night(tmp_path, [DEAD])

    assert meta(result, "raw_price_chart")["dead"] == 0
    assert fakes.PROBES == [], "an unhealthy provider is evidence about the provider"


def test_a_throttle_stops_the_night_and_marks_nothing(tmp_path: Path) -> None:
    refused = yahoo_chart.YahooRefused("HTTP 429 for AAPL")
    with yahoo({"AAPL": refused, "VOD.L": body("vod_l_ext")}) as calls:
        result = night(tmp_path, [AAPL, VOD])

    m = meta(result, "raw_price_chart")
    assert len(calls) == 1, "nothing is asked after a refusal"
    assert (m["throttled"], m["unasked"], m["answered"]) == (1, 2, 0)
    assert outcomes_sum(m) == m["requested"]
    assert fakes.PROBES == []


# --- a split restates the stored closes -----------------------------------------------------------


def test_a_split_since_the_load_reloads_the_whole_history(tmp_path: Path) -> None:
    with yahoo({"AAPL": body("aapl_ext")}):
        night(tmp_path, [AAPL])

    def answer(kwargs: dict[str, Any]) -> bytes:
        return (
            body("aapl_ext")
            if kwargs["period1"] == 0
            else with_split(body("aapl_ext"), date.today())
        )

    with yahoo({"AAPL": answer}) as calls:
        result = night(tmp_path, [AAPL])

    assert [kwargs["period1"] != 0 for _, kwargs in calls] == [True, False], "extend, then reload"
    assert meta(result, "raw_price_chart")["reloaded_after_split"] == 1
    assert stored(tmp_path, AAPL[0]) == [("AAPL", 0)], "the reload replaced every stale document"


def test_a_refused_reload_stores_nothing_so_the_next_visit_sees_the_split_again(
    tmp_path: Path,
) -> None:
    with yahoo({"AAPL": body("aapl_ext")}):
        night(tmp_path, [AAPL])

    def answer(kwargs: dict[str, Any]) -> bytes | Exception:
        if kwargs["period1"] == 0:
            return yahoo_chart.YahooRefused("HTTP 502 for AAPL")
        return with_split(body("aapl_ext"), date.today())

    with yahoo({"AAPL": answer}):
        result = night(tmp_path, [AAPL])

    m = meta(result, "raw_price_chart")
    assert (m["transport"], m["answered"]) == (1, 0)
    assert stored(tmp_path, AAPL[0]) == [("AAPL", 0)], "the extension was dropped, not appended"


# --- stage 2 --------------------------------------------------------------------------------------


def test_stage_2_counts_the_securities_whose_label_it_corrected(tmp_path: Path) -> None:
    fakes.STORED_LABELS.update({AAPL[0]: "USD", VOD[0]: "EUR"})
    with yahoo({"AAPL": body("aapl_ext"), "VOD.L": body("vod_l_ext")}):
        result = night(tmp_path, [AAPL, VOD])

    assert meta(result, "price_bar_history")["labels_changed"] == 1, "VOD.L: EUR -> GBX"


def test_stage_2_retracts_within_each_document_range_and_only_where_one_answered(
    tmp_path: Path,
) -> None:
    """A security whose only document names an absence holds no dates, so nothing of its stored
    history is retracted — an absence is never a statement that no bar exists."""
    with yahoo({"AAPL": body("aapl_ext"), "BDMS-F.BK": body("bdms_f_bk_404")}):
        night(tmp_path, [AAPL, DEAD])

    assert len(fakes.RETRACTIONS) == 1
    ids, firsts, lasts, _pair_ids, pair_dates = fakes.RETRACTIONS[0]
    assert ids == [AAPL[0]]
    assert (firsts, lasts) == ([date(2026, 9, 28)], [date(2026, 10, 9)])
    assert len(pair_dates) == 10


def test_a_range_run_publishes_every_partition_s_bars(tmp_path: Path) -> None:
    subjects = [AAPL, VOD, ALG, TOYOTA]
    with yahoo({s: body(BODIES[s]) for _, s, _ in subjects}):
        result = night(tmp_path, subjects)

    assert result.success
    assert {r["security_id"] for r in WRITTEN} == {sid for sid, _, _ in subjects}


# --- the check ------------------------------------------------------------------------------------


def test_the_check_passes_when_each_newest_bar_carries_its_provider_s_label(tmp_path: Path) -> None:
    fakes.STORED_LABELS.update({AAPL[0]: "USD", VOD[0]: "GBX"})
    with yahoo({"AAPL": body("aapl_ext"), "VOD.L": body("vod_l_ext")}):
        result = night(tmp_path, [AAPL, VOD], check=True)

    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed
    assert evaluation.metadata["securities_checked"].value == 2


def test_the_check_names_a_bar_whose_label_disagrees_with_its_provider(tmp_path: Path) -> None:
    """A second writer, or a write that did not land, leaves the guess in place."""
    fakes.STORED_LABELS.update({AAPL[0]: "USD", VOD[0]: "GBP"})
    with yahoo({"AAPL": body("aapl_ext"), "VOD.L": body("vod_l_ext")}):
        result = night(tmp_path, [AAPL, VOD], check=True)

    [evaluation] = result.get_asset_check_evaluations()
    assert not evaluation.passed
    assert VOD[0] in str(evaluation.metadata["disagreeing"].value)


# --- dividends and splits, from the same documents ------------------------------------------------


def test_the_night_files_dividends_from_its_own_documents_and_retracts_only_yahoo_s(
    tmp_path: Path,
) -> None:
    """NO REQUEST OF ITS OWN: the chart documents already carry the events. Toyota's extension
    holds one dividend, dated in Tokyo; the retraction is scoped to Yahoo's rows and to the span
    the documents cover, so a Tiingo row on the same key is never touched."""
    with yahoo({"7203.T": body("7203_t_ext")}) as calls:
        result = night(tmp_path, [TOYOTA], stage_2=False, actions=True)
    assert result.success
    assert len(calls) == 1, "the actions cost no request"
    actions_written = [r for r in WRITTEN if "kind" in r]
    assert actions_written == [
        {
            "security_id": TOYOTA[0],
            "ex_date": "2026-09-29",
            "kind": "dividend",
            "value": 50.0,
            "observed_symbol": "7203.T",
            "source_code": "yfinance",
            "as_of": actions_written[0]["as_of"],
        }
    ]
    (retraction,) = fakes.ACTION_RETRACTIONS
    assert retraction[0] == [TOYOTA[0]] and retraction[3] == "yfinance"
    spec = prices_core.yahoo_corporate_action.specs_by_key[dg.AssetKey("yahoo_corporate_action")]
    assert spec.metadata["only_replacing"] == {"source_code": ["yfinance", "tiingo"]}
