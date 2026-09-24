"""The identity ladder, driven end to end over the captured provider bytes.

The rung assets store OpenFIGI's and Yahoo's real answers; `security_symbology` resolves one
security's materialised rung files onto the identity tables. The positional guarantee is exercised
here too: the mapping fixture is a three-entry body, and the asset's `position` column is what
keeps "which entry answers THIS security" honest.
"""

from __future__ import annotations

import json
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
#: THE PROVIDERS ANSWERING WITH NOTHING, both captured rather than typed out. The mapping body is
#: the third entry of the same capture — OpenFIGI's own refusal — sliced to a one-job answer, so
#: the refusal wording stays the provider's. `yahoo_search_nothing.json` is a real empty `quotes`.
NOTHING_MAPPING = json.dumps([json.loads(MAPPING_BODY)[2]]).encode()
NOTHING_SEARCH = (FIX / "yahoo_search_nothing.json").read_bytes()


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
        elif "from market.security_provider_symbol where provider_code = any(%s)" in text:
            asked = set(params[1]) if params else set()
            self.rows = [
                ("yfinance", symbol, holder)
                for symbol, holder in self._state.symbol_holders.items()
                if symbol in asked
            ]
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
    #: symbol → the security already holding it in `security_provider_symbol`.
    symbol_holders: dict[str, str] = field(default_factory=dict)


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


def materialise(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    *,
    mapping_body: bytes = MAPPING_BODY,
    search_body: bytes = YAHOO_BODY,
) -> dg.ExecuteInProcessResult:
    """The whole ladder for one subject. The bodies are arguments because "the providers answered
    with nothing" is a case this family has to get right and cannot reach with the hit captures."""
    saved_map, saved_search = openfigi.mapping, yahoo_search.search
    # The mapping rungs get the SAME body; `position` says who it answers.
    openfigi.mapping = lambda jobs, **kw: _doc(mapping_body)
    yahoo_search.search = lambda isin, **kw: _doc(search_body)
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


def _probes(writes: list[tuple[str, tuple[Any, ...]]]) -> list[tuple[Any, Any]]:
    """Every `identifier_probe` row a run wrote, as (scheme, outcome).

    The upsert flattens its rows into one params tuple, so the rows are recovered by chunking on
    the column count — and the column list is READ OUT OF THE STATEMENT rather than restated here.
    `upsert` sorts its columns, which a hand-written list got wrong on the first run: it chunked
    correctly and read `observed_at` as the scheme, so the assertion failed for a reason unrelated
    to the rule. A second copy of another module's ordering is the shape that drifts.

    Asserting on the SCHEME rather than on a substring is the other half: `"symbol" in params` is
    also true of a ticker row whose value happens to contain it.
    """
    out: list[tuple[Any, Any]] = []
    for sql, params in writes:
        if "market.identifier_probe" not in sql:
            continue
        columns = [c.strip() for c in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
        scheme, outcome = columns.index("scheme"), columns.index("outcome")
        for i in range(0, len(params), len(columns)):
            row = params[i : i + len(columns)]
            out.append((row[scheme], row[outcome]))
    return out


def test_a_skipped_rung_writes_no_observation_and_the_asset_is_what_says_so(
    tmp_path: Path,
) -> None:
    """THE LIVE DEFECT, AT THE CALL SITE. `plan_symbols` obeying `asked_symbol` is proven in the
    library suite; nothing there can prove this asset PASSES it honestly, and a rule written at one
    call site is not a rule — this codebase has paid for that distinction repeatedly.

    Measured in production 2026-09-22 on the first three subjects ever run: each needed a ticker
    and already held a symbol, so both symbol rungs correctly wrote EMPTY files — and
    `identifier_probe` still gained three `scheme=symbol, outcome=miss` rows. `stale_misses` would
    then have paid Yahoo, one request per subject, to re-ask each of them in 30 days.

    THE FIXTURE MAKES THE TWO RULES DISAGREE by running the SAME subject and the same captured
    bytes twice, changing only what the database says it still needs. A ladder recording whatever
    it can see writes a symbol probe both times; one recording what it ASKED writes it once. The
    adopted symbol row is written either way, because that is a finding rather than an observation.
    """
    # THE SYMBOL RUNGS SKIP IT: the database says this security already has a provider symbol.
    STATE.needing_symbol = {OTHER_SID}
    FakeCursor.writes.clear()
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID])
            assert materialise(tmp_path, instance).success
        skipped = _probes(FakeCursor.writes)
        skipped_writes = list(FakeCursor.writes)
    finally:
        STATE.needing_symbol = {SID}

    assert skipped == [("ticker", "hit")], (
        f"recorded an answer to a question nobody asked: {skipped}"
    )
    # AND THE VALUE IS STILL ADOPTED. The ticker rung's own hits name the US line, so the ladder
    # holds a real symbol — it is written, it is simply not claimed as an observation.
    assert any("market.security_provider_symbol" in w for w, _ in skipped_writes), skipped_writes

    # THE CONTROL IS THE NEXT TEST, NOT HERE, AND THE MUTATION IS WHY. Re-running this subject
    # with the rungs asking DOES write a symbol hit — and it would write one with the flag pinned
    # false too, because the local pick below `plan_symbols` records its own hit whenever the
    # unfiltered rung names a line. That control passes through a different code path from the
    # rule, so it proves nothing: pinning `asked_symbol=False` was MISSED by it.


def test_a_rung_that_asked_and_got_nothing_still_records_the_miss(tmp_path: Path) -> None:
    """THE OTHER HALF, AND THE ONLY CASE THE FLAG ALONE DECIDES. A miss is an observation: it is
    what `stale_misses` reads to re-ask in 30 days, and a lane that stopped recording them would
    never revisit a security that has since gained a listing.

    Both provider answers here are captures of NOTHING, which is what makes the case discriminating
    — with no hits anywhere, the local pick writes no probe of its own, so the flag is the only
    thing left that can decide whether a row appears. Run twice on identical bytes, changing only
    what the database says the subject still needs: asked earns a miss, skipped earns silence.
    """
    keys = {"asked": {SID}, "skipped": {OTHER_SID}}
    seen: dict[str, list[tuple[Any, Any]]] = {}
    for label, needing in keys.items():
        STATE.needing_symbol = needing
        FakeCursor.writes.clear()
        try:
            with dg.instance_for_test() as instance:
                instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID])
                assert materialise(
                    tmp_path / label,
                    instance,
                    mapping_body=NOTHING_MAPPING,
                    search_body=NOTHING_SEARCH,
                ).success
        finally:
            STATE.needing_symbol = {SID}
        seen[label] = _probes(FakeCursor.writes)

    assert ("symbol", "miss") in seen["asked"], seen["asked"]
    assert ("symbol", "miss") not in seen["skipped"], seen["skipped"]
    # The ticker rung asked in both, and its own miss is recorded either way — so the difference
    # above is the symbol rule rather than the run having done nothing.
    assert ("ticker", "miss") in seen["asked"] and ("ticker", "miss") in seen["skipped"], seen


def test_the_ladder_closes_without_the_rung_that_is_not_automated(tmp_path: Path) -> None:
    """WHAT LEAVING A RUNG UNAUTOMATED DOES TO THE STEP THAT ADOPTS ITS ANSWERS. `eager()` will not
    fire while ANY upstream partition is missing, and `raw_yahoo_symbol` has no condition — so if
    `security_symbology` waited on it, the two mapping rungs would collect answers into Parquet for
    ever and nothing would ever reach the identity tables. Every run stays green while the lane
    delivers nothing, which is precisely the shape that kept `security_return` from ever
    materialising and went unseen for a whole history load.

    Driven rather than reasoned about: the two mapping rungs land, the Yahoo one never does, and
    the daemon is asked what it would request.
    """
    defs = dg.Definitions(
        assets=[
            symbology_raw.raw_figi_ticker,
            symbology_raw.raw_figi_local_symbol,
            symbology_raw.raw_yahoo_symbol,
            symbology_core.security_symbology,
        ],
        resources={
            "postgres": FakePostgres(),
            "parquet_io": ParquetIOManager(str(tmp_path)),
            "postgres_io": ParquetIOManager(str(tmp_path)),
        },
    )
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID])
        # A FIRST TICK BEFORE ANYTHING LANDS, because `eager()` triggers on
        # `(newly_missing | any_deps_updated).since_last_handled()` and the initial evaluation
        # COUNTS AS HANDLED. Evaluating once against a fresh cursor therefore reports zero for
        # every asset whatever the rule says — the same shape already measured for `on_missing()`,
        # which requested 0 of 2 partitions that were in the grid at its first tick.
        first = dg.evaluate_automation_conditions(defs=defs, instance=instance)
        for rung in (symbology_raw.raw_figi_ticker, symbology_raw.raw_figi_local_symbol):
            _materialise_range(tmp_path, instance, [SID], asset=rung)
        requested = {
            str(r.key.to_user_string()): r.true_subset.size
            for r in dg.evaluate_automation_conditions(
                defs=defs, instance=instance, cursor=first.cursor
            ).results
        }
    assert requested.get("security_symbology") == 1, (
        "the adopting step waits for a rung nothing will ever materialise, so the ladder's "
        f"answers never reach the identity tables: {requested}"
    )


def test_the_adopting_step_runs_when_the_yahoo_rung_never_did(tmp_path: Path) -> None:
    """THE CONDITION PERMITTING A RUN IS HALF OF IT; THE RUN SURVIVING IS THE OTHER. With the Yahoo
    rung unautomated its partition file does not exist, and an input that cannot tolerate that
    dies loading it — measured 2026-09-22, six runs failed with
    `FileNotFoundError: .../raw_figi_local_symbol/<uuid>.parquet` for exactly this reason, and the
    test above cannot see it because asking the daemon what it WOULD request executes nothing.

    Driven the way production reaches it: the two mapping rungs materialise, the Yahoo one is
    in the graph but NOT selected, so its input is loaded from disk and finds no file. Yahoo is
    wired to refuse, so a run that asked it anyway fails for that reason instead.
    """

    def refuse(isin: str, **kw: Any) -> Document:
        raise AssertionError(f"the unautomated Yahoo rung was asked about {isin}")

    FakeCursor.writes.clear()
    saved_map, saved_search = openfigi.mapping, yahoo_search.search
    openfigi.mapping = lambda jobs, **kw: _doc(MAPPING_BODY)
    yahoo_search.search = refuse
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID])
            result = dg.materialize(
                [
                    symbology_raw.raw_figi_ticker,
                    symbology_raw.raw_figi_local_symbol,
                    symbology_raw.raw_yahoo_symbol,
                    symbology_core.security_symbology,
                ],
                selection=[
                    symbology_raw.raw_figi_ticker,
                    symbology_raw.raw_figi_local_symbol,
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

    assert result.success
    assert not (tmp_path / "raw_yahoo_symbol" / f"{SID}.parquet").exists(), (
        "the fixture is meant to reach the adopting step with NO Yahoo file on disk"
    )
    # AND IT ADOPTED WHAT THE MAPPING RUNGS FOUND — a run that succeeded by writing nothing would
    # pass the two assertions above and deliver exactly the silent outage this guards against.
    assert any(
        "market.security_identifier" in w and "AAPL" in params for w, params in FakeCursor.writes
    ), FakeCursor.writes


def test_a_listing_held_by_another_security_is_withheld_and_counted_not_crashed_on(
    tmp_path: Path,
) -> None:
    """ONE LISTING, ONE SECURITY. `(provider_code, symbol)` is unique and the upsert's `on conflict`
    names the other key, so on 2026-09-24 a symbol another security already held failed a whole
    batch of up to 200 with `UniqueViolation` — `WLN.PA`, which OpenFIGI correctly names for both of
    Worldline's ISINs. The fake cannot enforce the key, so this asserts the DECISION: the listing
    stays with its holder, the refusal is counted, and the probe — a true observation — is written.

    TWO SUBJECTS, BECAUSE ONE CANNOT TELL "WRITE THE ADOPTABLE ROWS" FROM "WRITE EVERY ROW". With a
    single withheld subject nothing is left to write, the write is skipped either way, and a
    mutation writing every row it built passed clean. Here AAPL is held elsewhere and ACN is free,
    so the write happens and must carry exactly one of them.
    """
    holder = "33333333-3333-3333-3333-333333333333"
    STATE.symbol_holders = {"AAPL": holder}
    STATE.needing_ticker = {SID, OTHER_SID}
    STATE.needing_symbol = {SID, OTHER_SID}
    FakeCursor.writes.clear()
    saved_map, saved_search = openfigi.mapping, yahoo_search.search
    openfigi.mapping = lambda jobs, **kw: _doc(MAPPING_BODY)
    yahoo_search.search = lambda isin, **kw: _doc(NOTHING_SEARCH)
    try:
        with dg.instance_for_test() as instance:
            instance.add_dynamic_partitions(SYMBOLOGY_PARTITIONS, [SID, OTHER_SID])
            result = dg.materialize(
                [
                    symbology_raw.raw_figi_ticker,
                    symbology_raw.raw_figi_local_symbol,
                    symbology_raw.raw_yahoo_symbol,
                    symbology_core.security_symbology,
                ],
                instance=instance,
                resources={
                    "postgres": FakePostgres(),
                    "parquet_io": ParquetIOManager(str(tmp_path)),
                },
                tags={
                    "dagster/asset_partition_range_start": SID,
                    "dagster/asset_partition_range_end": OTHER_SID,
                },
            )
    finally:
        openfigi.mapping = saved_map
        yahoo_search.search = saved_search
        STATE.symbol_holders = {}
        STATE.needing_ticker = {SID}
        STATE.needing_symbol = {SID}

    assert result.success
    written = [
        v
        for w, params in FakeCursor.writes
        if w.startswith("insert into market.security_provider_symbol")
        for v in params
    ]
    assert "ACN" in written, f"the free listing was not adopted: {written}"
    assert "AAPL" not in written, f"wrote a listing another security holds: {written}"
    assert ("symbol", "hit") in _probes(FakeCursor.writes), _probes(FakeCursor.writes)

    meta = result.asset_materializations_for_node("security_symbology")[0].metadata
    assert meta["symbols_held_elsewhere"].value == 1, meta
    assert meta["symbols_ambiguous"].value == 0, meta


def test_the_yahoo_rung_deliberately_carries_no_automation_condition() -> None:
    """ITS ABSENCE IS A DECISION, AND AN ABSENCE CANNOT BE READ FROM THE CODE AS ONE — adding the
    condition back is a one-line change that would look like completing an oversight.

    The two mapping rungs spend OpenFIGI's budget, which nothing else in this deployment competes
    for. Yahoo's search is one request per subject on the budget the NIGHTLY PRICE SWEEP lives on,
    and that provider refused the sweep at call 138 of ~601 on 2026-09-19. 1,618 subjects is more
    than a whole night's measured allowance, so this rung stays an operator's backfill until the
    trade has been measured rather than assumed.

    Asserted beside its siblings rather than alone: "no condition anywhere" would also pass if the
    ladder stopped being automated at all, which is a different and much larger regression.
    """
    assert symbology_raw.raw_yahoo_symbol.automation_conditions_by_key == {}, (
        "raw_yahoo_symbol spends the price lane's Yahoo budget; automating it is a measurement, "
        "not a tidy-up"
    )
    for rung in (symbology_raw.raw_figi_ticker, symbology_raw.raw_figi_local_symbol):
        assert rung.automation_conditions_by_key, rung.key


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
    tmp_path: Path,
    instance: dg.DagsterInstance,
    keys: list[str],
    asset: Any = None,
) -> tuple[dg.ExecuteInProcessResult, list[str]]:
    """Materialise a rung over a partition RANGE, recording the ISINs it actually asked.

    Defaults to the Yahoo rung, which is what most of these tests are about. The re-ask test passes
    a MAPPING rung instead, because the Yahoo one deliberately carries no automation condition.

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
            [asset or symbology_raw.raw_yahoo_symbol],
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
    # THE TICKER RUNG, NOT THE YAHOO ONE. This test is about `SYMBOLOGY_AUTOMATION`'s re-ask, and
    # `raw_yahoo_symbol` deliberately carries no condition — it spends the budget the nightly price
    # sweep lives on, so it stays an operator's backfill. Pointed at the Yahoo rung this asserts
    # zero for a reason unrelated to the rule.
    defs = dg.Definitions(
        assets=[symbology_raw.raw_figi_ticker],
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

            _materialise_range(
                tmp_path, instance, [SID, OTHER_SID], asset=symbology_raw.raw_figi_ticker
            )
            again = dg.evaluate_automation_conditions(
                defs=defs, instance=instance, cursor=cold.cursor, evaluation_time=after
            )
    finally:
        Postgres.connect = saved_connect  # type: ignore[method-assign]
        STATE.stale_misses = set()

    assert again.total_requested == 1, (
        f"expected only the stale miss to be re-asked, got {again.total_requested}"
    )
