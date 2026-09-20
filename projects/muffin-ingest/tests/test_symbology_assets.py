"""The identity ladder, driven end to end over the captured provider bytes.

The rung assets store OpenFIGI's and Yahoo's real answers; `security_symbology` resolves one
security's materialised rung files onto the identity tables. The positional guarantee is exercised
here too: the mapping fixture is a three-entry body, and the asset's `position` column is what
keeps "which entry answers THIS security" honest.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import dagster as dg
from muffin_ingest.providers import openfigi, yahoo_search
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.symbology import core as symbology_core
from muffin_ingest_dagster.defs.symbology import raw as symbology_raw
from muffin_ingest_dagster.defs.symbology.partitions import SYMBOLOGY_PARTITIONS
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

FIX = FIXTURES

SID = "11111111-1111-1111-1111-111111111111"
#: A second subject in the same run, used to make the population rules disagree.
OTHER_SID = "22222222-2222-2222-2222-222222222222"
MAPPING_BODY = (FIX / "openfigi_mapping.json").read_bytes()
YAHOO_BODY = (FIX / "yahoo_search_aapl.json").read_bytes()


class FakeCursor:
    """Answers the ladder's reader queries and records every write.

    THE BRANCH ORDER IS LOAD-BEARING AND THIS FILE HAS PAID FOR IT BEFORE. Four of these queries
    contain `kind_code = 'isin'` — the two population queries, the attribute lookup, and nothing
    else — so a single `elif "isin" in text` answers all of them identically, and the per-rung
    population filter would then be untestable: every rung would see every subject whatever the
    rule said. Each branch therefore matches the clause that is UNIQUE to its query, most specific
    first.
    """

    writes: ClassVar[list[tuple[str, tuple[Any, ...]]]] = []

    def __init__(self, rows: list[tuple[Any, ...]], state: _State) -> None:
        self._rows = rows
        self._state = state
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        if not text.startswith("select"):
            FakeCursor.writes.append((text, tuple(params)))
            self.rows = []
            return
        if "from market.exchange" in text:
            self.rows = [("US", "US", "")]
        elif "from market.identifier_probe" in text:
            self.rows = [(sid,) for sid in self._state.stale_misses]
        elif "market.security_identifier t" in text:  # subjects_needing(NEEDS_TICKER)
            self.rows = [(sid,) for sid in self._state.needing_ticker]
        elif "market.security_provider_symbol p" in text:  # subjects_needing(NEEDS_SYMBOL)
            self.rows = [(sid,) for sid in self._state.needing_symbol]
        elif "i.security_id::text = any(%s)" in text:  # attributes_for
            asked = set(params[0]) if params else set()
            self.rows = [r for r in self._rows if r[0] in asked]
        else:
            self.rows = []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


@dataclass
class _State:
    """Which securities the database says still need which evidence."""

    needing_ticker: set[str] = field(default_factory=lambda: {SID})
    needing_symbol: set[str] = field(default_factory=lambda: {SID})
    stale_misses: set[str] = field(default_factory=set)


STATE = _State()


#: The ISINs the fake database holds. MODULE LEVEL, NOT AN INSTANCE ATTRIBUTE, and that is not
#: tidiness: `ReAskAfter` gets no resources, so it constructs `Postgres()` itself and a test
#: covering it has to patch the CLASS's `connect`. Bound to `self.attributes`, that method is then
#: called with a real `Postgres` as `self` and dies with `'Postgres' object has no attribute
#: 'attributes'` — which happened, and passed alone while failing in the suite, because the branch
#: that reaches the database only runs once a cron tick has passed.
ATTRIBUTE_ROWS: list[tuple[Any, ...]] = [
    (SID, "US0378331005", "US"),
    (OTHER_SID, "IE00B4BNMY34", "US"),
]


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield _Conn()


class _Conn:
    def cursor(self) -> FakeCursor:
        return FakeCursor(ATTRIBUTE_ROWS, STATE)

    def commit(self) -> None:
        return None


def _doc(body: bytes) -> Document:
    return Document(
        url="https://provider/x",
        body=body,
        content_type="application/json",
        fetched_at=datetime.now(UTC),
    )


def materialise(tmp_path: Path, instance: dg.DagsterInstance) -> dg.ExecuteInProcessResult:
    saved_map, saved_search = openfigi.mapping, yahoo_search.search
    # The mapping rungs get the SAME three-entry body; `position` says who it answers.
    openfigi.mapping = lambda jobs, **kw: _doc(MAPPING_BODY)
    yahoo_search.search = lambda isin, **kw: _doc(YAHOO_BODY)
    try:
        return dg.materialize(
            [
                symbology_raw.raw_figi_ticker,
                symbology_raw.raw_figi_local_symbol,
                symbology_raw.raw_yahoo_symbol,
                symbology_core.security_symbology,
            ],
            partition_key=SID,
            instance=instance,
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
        )
    finally:
        openfigi.mapping = saved_map
        yahoo_search.search = saved_search


def test_the_ladder_adopts_a_ticker_and_a_symbol_and_records_probes(
    tmp_path: Path,
) -> None:
    """AAPL's mapping hit (position 0 of the captured three-entry body) and Yahoo's `AAPL` search
    line become a ticker identifier, a provider symbol and two hit probes — in one transaction."""
    FakeCursor.writes.clear()
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID])
        result = materialise(tmp_path, instance)
    assert result.success

    writes = FakeCursor.writes
    assert any("market.security_identifier" in w and "AAPL" in params for w, params in writes), (
        writes
    )
    assert any("market.security_provider_symbol" in w for w, _ in writes), writes
    assert any("market.identifier_probe" in w for w, _ in writes), writes

    # The probe rows carry the HIT outcome — the grid says this security was asked and answered.
    probe_writes = [(w, p) for w, p in writes if "market.identifier_probe" in w]
    assert probe_writes and "hit" in probe_writes[0][1], probe_writes


def test_the_position_column_keeps_the_positional_guarantee_on_the_way_back_in() -> None:
    """The captured mapping body answers job 0 as AAPL and job 1 as ACN. A security whose evidence
    row sits at position 1 must resolve to ACN, never to AAPL — a reorder would attach one
    company's ticker to another's, which is worse than none."""
    from muffin_ingest_dagster.defs.symbology import core as symbology_core

    evidence = [{"body": bytearray(MAPPING_BODY), "position": 1, "asked_with": "IE00B4BNMY34"}]
    entry = symbology_core._entry_from_evidence(evidence, isin="IE00B4BNMY34")
    assert entry is not None and entry.hits[0]["ticker"] == "ACN", entry


# --- the population, the grid, and the re-ask ----------------------------------------------------


def _materialise_range(
    tmp_path: Path, instance: dg.DagsterInstance, keys: list[str]
) -> tuple[dg.ExecuteInProcessResult, list[str]]:
    """Materialise the Yahoo rung over a partition RANGE, recording the ISINs it actually asked.

    The recorder lives here rather than in the test because both patch the same module attribute,
    and a test that patched it around this call had its recorder silently replaced by this
    function's own stub — the assertion then read "nothing was asked" for a reason unrelated to the
    rule under test.
    """
    searched: list[str] = []

    def search(isin: str, **kw: Any) -> Document:
        searched.append(isin)
        return _doc(YAHOO_BODY)

    saved_map, saved_search = openfigi.mapping, yahoo_search.search
    openfigi.mapping = lambda jobs, **kw: _doc(MAPPING_BODY)
    yahoo_search.search = search
    try:
        result = dg.materialize(
            [symbology_raw.raw_yahoo_symbol],
            instance=instance,
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
            },
            tags={
                "dagster/asset_partition_range_start": keys[0],
                "dagster/asset_partition_range_end": keys[-1],
            },
        )
    finally:
        openfigi.mapping = saved_map
        yahoo_search.search = saved_search
    return result, searched


def test_a_rung_does_not_spend_a_request_on_a_subject_whose_evidence_we_hold(
    tmp_path: Path,
) -> None:
    """THE POPULATION IS PER RUNG, AND YAHOO IS WHY IT MATTERS. Its search is ONE REQUEST PER
    SUBJECT where the mapping rungs get ten, so a security whose provider symbol we already hold
    costs a whole request to be told what is already written down. The rungs shipped asking about
    `is_tradeable = false` — 23,341 securities, 15,159 of them bonds, with no clause excluding a
    security that already had the evidence.

    THE FIXTURE MAKES THE RULES DISAGREE: both subjects are in the run and only one still needs a
    symbol, so a rung ignoring the population asks twice and a rung honouring it asks once.
    """
    STATE.needing_symbol = {OTHER_SID}
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID, OTHER_SID])
            result, searched = _materialise_range(tmp_path, instance, [SID, OTHER_SID])
    finally:
        STATE.needing_symbol = {SID}

    assert result.success
    assert searched == ["IE00B4BNMY34"], (
        f"asked about a subject whose symbol we already hold: {searched}"
    )

    # AND THE SKIPPED SUBJECT STILL GETS A FILE. "We looked and this rung had nothing to ask" and
    # "we never looked" must stay distinguishable on disk, which is what the partition claims.
    for key in (SID, OTHER_SID):
        assert (tmp_path / "raw_yahoo_symbol" / f"{key}.parquet").exists(), key


def test_the_sensor_only_adds_and_never_touches_the_price_grid() -> None:
    """THE DEFECT THIS FAMILY SHIPPED WITH, and the reason it never bit: the sensor was stopped.

    `new_symbols_needed` re-asked a stale miss by DELETING the security's partition so the next
    tick would queue it again — from `prices.partitions.security_partitions`, the grid the nightly
    price sweep walks and `no_security_is_far_behind_the_sweep` reads. The bars would have been
    untouched and the record of having collected them gone.

    Two assertions, because either alone passes while the other fails: the sensor issues no DELETE
    at all, and what it does issue names the SYMBOLOGY partition set.
    """
    from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION
    from muffin_ingest_dagster.defs.symbology.automation import new_symbols_needed

    STATE.needing_ticker = {SID}
    STATE.needing_symbol = {OTHER_SID}
    try:
        with dg.instance_for_test() as instance:
            context = dg.build_sensor_context(
                instance=instance, resources={"postgres": FakePostgres()}
            )
            result = new_symbols_needed(context)
    finally:
        STATE.needing_ticker = {SID}
        STATE.needing_symbol = {SID}

    assert isinstance(result, dg.SensorResult)
    requests = list(result.dynamic_partitions_requests or ())
    assert requests, "the sensor seeded nothing"
    for request in requests:
        assert isinstance(request, dg.AddDynamicPartitionsRequest), (
            f"the sensor issued {type(request).__name__}; a grid is not a queue you pop from, and "
            f"deleting a key destroys the only state that says a subject was collected"
        )
        assert request.partitions_def_name != SECURITY_PARTITION, (
            "the ladder is seeding the PRICE lane's grid"
        )
        assert request.partitions_def_name == SYMBOLOGY_PARTITIONS
    seeded = {k for r in requests for k in r.partition_keys}
    assert seeded == {SID, OTHER_SID}, f"the seeded population is not the union: {seeded}"


def test_the_re_ask_requests_a_stale_miss_and_leaves_everything_else_alone(
    tmp_path: Path,
) -> None:
    """A MISS IS AN ANSWER WITH A SHELF LIFE. `missing()` covers a subject never asked; this covers
    one asked 31 days ago that the provider had nothing for — and must cover NOTHING else, or a
    rate-limited provider is re-asked about answers already written down."""
    STATE.stale_misses = {OTHER_SID}
    # AN AUTOMATION CONDITION GETS NO RESOURCES, so it opens its own connection through the same
    # `Postgres` class every asset uses — which is what makes it patchable here without reaching
    # into the conditions module's re-exported name (mypy refuses that, and rightly: a re-exported
    # attribute is not an export).
    saved_connect = Postgres.connect
    Postgres.connect = FakePostgres.connect  # type: ignore[method-assign,assignment]
    defs = dg.Definitions(
        assets=[symbology_raw.raw_yahoo_symbol],
        resources={
            "postgres": FakePostgres(),
            "parquet_io": ParquetIOManager(str(tmp_path)),
        },
    )
    # THE CLOCK IS PART OF THE FIXTURE. The re-ask sits behind a daily cron gate, so two
    # evaluations microseconds apart would both find no tick has passed and the test would assert
    # zero re-asks for a reason unrelated to the rule. The second reading is the next morning.
    before = datetime(2026, 9, 20, 2, 0, tzinfo=UTC)
    after = datetime(2026, 9, 21, 4, 0, tzinfo=UTC)
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID, OTHER_SID])
            cold = dg.evaluate_automation_conditions(
                defs=defs, instance=instance, evaluation_time=before
            )
            # `missing()` covers both on a cold grid. Correct, and not what this tests.
            assert cold.total_requested == 2, cold.total_requested

            _materialise_range(tmp_path, instance, [SID, OTHER_SID])
            again = dg.evaluate_automation_conditions(
                defs=defs, instance=instance, cursor=cold.cursor, evaluation_time=after
            )
    finally:
        Postgres.connect = saved_connect  # type: ignore[method-assign]
        STATE.stale_misses = set()

    assert again.total_requested == 1, (
        f"expected only the stale miss to be re-asked, got {again.total_requested}"
    )
