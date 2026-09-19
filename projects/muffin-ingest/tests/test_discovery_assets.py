"""The discovery assets, driven end to end with a patched provider and a fake database.

Driven rather than inspected, because the defect this family exists to prevent is silent: a
placeholder CUSIP accepted as real collapses four companies into one with no error anywhere. So
these materialise the real assets through the real I/O manager over the captured XLK filing and
assert what reached the other side — securities, identifiers, issuers and holdings, with the
placeholder skips counted.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import dagster as dg
import pytest
from muffin_ingest.facets import nport
from muffin_ingest.providers import openfigi as figi_provider
from muffin_ingest.providers import sec_nport
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.discovery import partitions as discovery_partitions
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

FIX = FIXTURES

XLK_BODY = (FIX / "sec_nport_primary_xlk.xml").read_bytes()
XLK_KEY = "1064641:000141036826075254"
FUND_ID = "99999999-9999-4999-8999-999999999999"

#: XLK's holdings resolved the way discovered_security would resolve them on a fresh database.
XLK_HOLDINGS = nport.parse_holdings(XLK_BODY)
_XLK_SECS, _XLK_IDENTS, _XLK_ISS, _XLK_IDS = nport.plan_holdings(
    XLK_HOLDINGS, known_identifiers={}, countries={"US", "JP"}, source="sec-nport"
)
XLK_KNOWN = {f"{i['kind_code']}:{i['value']}": i["security_id"] for i in _XLK_IDENTS}


class _Capture(dg.ConfigurableIOManager):
    """postgres_io stand-in: capture the rows the asset returned, like test_price_assets."""

    last: ClassVar[list[dict[str, Any]]] = []
    meta: ClassVar[dict[str, Any]] = {}

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        _Capture.last = list(obj)
        _Capture.meta = dict(context.definition_metadata)

    def load_input(self, context: dg.InputContext) -> Any:
        raise NotImplementedError


class FakeCursor:
    """Answers the discovery reader queries from seeded state, and records every write that
    follows — the writes are how the placeholder-guard chain is asserted."""

    writes: ClassVar[list[tuple[str, tuple[Any, ...]]]] = []

    def __init__(
        self,
        *,
        countries: Sequence[tuple[str, str]],
        tracked: Sequence[tuple[str, Any]],
        known_identifiers: dict[str, str],
        fund_by_series: dict[str, str],
        fund_by_symbol: dict[str, str],
    ) -> None:
        self._countries = countries
        self._tracked = tracked
        self._known = known_identifiers
        self._fund_by_series = fund_by_series
        self._fund_by_symbol = fund_by_symbol
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        # READ queries are answered by shape; anything else is a WRITE and is recorded for the
        # assertions on what reached the database. Guarded with `startswith("select")` because the
        # identifier INSERT contains the words of the identifier SELECT.
        if text.startswith("select"):
            if "from market.countries" in text:
                self.rows = [(iso2,) for iso2, _ in self._countries]
            # BOTH "kind_code = 'ticker'" queries contain that phrase, so the more specific
            # (series join) must be matched before the generic one.
            elif "t.series_id, i.security_id" in text:
                self.rows = [(series, sid) for series, sid in self._fund_by_series.items()]
            elif "kind_code = 'ticker'" in text:
                self.rows = [(symbol, sid) for symbol, sid in self._fund_by_symbol.items()]
            elif "from market.tracked_fund" in text:
                self.rows = list(self._tracked)
            elif "market.security_identifier" in text:
                self.rows = []
                for key, sid in self._known.items():
                    kind, value = key.split(":", 1)
                    self.rows.append((kind, value, sid))
            else:
                self.rows = []
        else:
            FakeCursor.writes.append((text, tuple(params)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class FakeConn:
    def __init__(
        self,
        *,
        countries: Sequence[tuple[str, str]] = (("US", "United States"), ("JP", "Japan")),
        tracked: Sequence[tuple[Any, ...]] = (
            ("XLK", "Technology Select Sector SPDR Fund", "1064641", "S000006415"),
        ),
        known_identifiers: dict[str, str] | None = None,
        fund_by_series: dict[str, str] | None = None,
        fund_by_symbol: dict[str, str] | None = None,
    ) -> None:
        self._countries = countries
        self._tracked = tracked
        self._known = known_identifiers or {}
        self._fund_by_series = fund_by_series or {}
        self._fund_by_symbol = fund_by_symbol or {}

    def cursor(self) -> FakeCursor:
        return FakeCursor(
            countries=self._countries,
            tracked=self._tracked,
            known_identifiers=self._known,
            fund_by_series=self._fund_by_series,
            fund_by_symbol=self._fund_by_symbol,
        )

    def commit(self) -> None:
        return None


class FakePostgres(Postgres):
    known: ClassVar[dict[str, str]] = {}
    fund_by_series: ClassVar[dict[str, str]] = {}
    fund_by_symbol: ClassVar[dict[str, str]] = {}

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield FakeConn(
            known_identifiers=self.known,
            fund_by_series=self.fund_by_series,
            fund_by_symbol=self.fund_by_symbol,
        )


def _doc(body: bytes, url: str = "https://www.sec.gov/x") -> Document:
    return Document(
        url=url, body=body, content_type="application/xml", fetched_at=datetime.now(UTC)
    )


def materialise(
    tmp_path: Path,
    assets: list[Any],
    key: str,
    *,
    instance: dg.DagsterInstance,
    provider: Any,
) -> dg.ExecuteInProcessResult:

    saved = sec_nport.primary_doc
    sec_nport.primary_doc = provider
    try:
        return dg.materialize(
            assets,
            partition_key=key,
            instance=instance,
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "postgres_io": _Capture(),
            },
        )
    finally:
        sec_nport.primary_doc = saved


def _serve(cik: str, accession: str, **kw: Any) -> Document:
    return _doc(
        XLK_BODY,
        url=f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/primary_doc.xml",
    )


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    with dg.instance_for_test() as inst:
        yield inst


def test_raw_nport_filing_stores_one_document_row_per_partition(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [XLK_KEY])
    result = materialise(
        tmp_path, [discovery_raw.raw_nport_filing], XLK_KEY, instance=instance, provider=_serve
    )
    assert result.success

    from pyarrow import parquet as pq

    rows = pq.read_table(str(tmp_path / "raw_nport_filing" / f"{XLK_KEY}.parquet")).to_pylist()
    assert len(rows) == 1
    # THE PROVENANCE TRAVELS AS COLUMNS BESIDE THE BODY, and the body is byte-identical to the
    # captured filing — the raw-fidelity rule, asserted on the bytes, not a round trip.
    assert bytes(rows[0]["body"]) == XLK_BODY
    assert rows[0]["cik"] == "1064641"
    assert rows[0]["accession"] == "000141036826075254"


def test_discovery_resolution_writes_securities_identifiers_and_issuers(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """The captured XLK filing: 79 holdings, all resolvable by ISIN. The writes reach three tables
    in one transaction, and a re-run would be a no-op because the database now knows them."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    FakeCursor.writes.clear()
    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [XLK_KEY])
    result = materialise(
        tmp_path,
        [discovery_raw.raw_nport_filing, discovery_core.discovered_security],
        XLK_KEY,
        instance=instance,
        provider=_serve,
    )
    assert result.success
    events = result.asset_materializations_for_node(discovery_core.discovered_security.op.name)
    meta: dict[str, Any] = {k: v.value for k, v in events[0].metadata.items()}
    # THE CAPTURED FILING HAS 79 HOLDINGS AND 76 DISTINCT ISINs — the three duplicates are LOTS
    # of one security, which is the dedupe working, not a loss. Every holding resolves by ISIN.
    assert meta["securities"] == 76, meta
    assert meta["issuers"] > 0, meta
    # The fund ITSELF is a security: the tracked fund's ticker identifier is created too.
    assert meta["funds"] == 1, meta

    insert_sql = [sql for sql, _ in FakeCursor.writes if "insert into market." in sql]
    assert any("insert into market.security " in s for s in insert_sql)
    assert any("insert into market.security_identifier" in s for s in insert_sql)
    assert any("insert into market.issuer" in s for s in insert_sql)


def test_a_re_run_of_a_filed_filing_creates_nothing(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """Resolution reads back what it wrote: with `known` holding every XLK identifier, a rerun
    of the same filing resolves everything against the database and writes NO new security."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    FakeCursor.writes.clear()
    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [XLK_KEY])
    FakePostgres.known = dict(XLK_KNOWN)
    try:
        result = materialise(
            tmp_path,
            [discovery_raw.raw_nport_filing, discovery_core.discovered_security],
            XLK_KEY,
            instance=instance,
            provider=_serve,
        )
    finally:
        FakePostgres.known = {}
    assert result.success
    events = result.asset_materializations_for_node(discovery_core.discovered_security.op.name)
    meta: dict[str, Any] = {k: v.value for k, v in events[0].metadata.items()}
    assert meta["securities"] == 0, meta
    assert meta["identifiers"] == 0, meta


def test_fund_holding_publishes_each_partition_s_own_snapshot(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """`fund_holding` resolves AGAINST the identifiers on the database (as if discovered_security
    ran) and writes the snapshot keyed (fund, security, as_of). The captured XLK filing has 79
    holdings; `as_of` is the DOCUMENT's own repPdDate, never the run's clock."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    _Capture.last = []
    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [XLK_KEY])
    FakePostgres.known = dict(XLK_KNOWN)
    FakePostgres.fund_by_series = {"S000006415": FUND_ID}
    try:
        result = materialise(
            tmp_path,
            [
                discovery_raw.raw_nport_filing,
                discovery_core.discovered_security,
                discovery_core.fund_holding,
            ],
            XLK_KEY,
            instance=instance,
            provider=_serve,
        )
    finally:
        FakePostgres.known = {}
        FakePostgres.fund_by_series = {}

    assert result.success
    rows = _Capture.last
    # 79 holdings, 76 distinct securities: three funds hold two LOTS of one company, and the
    # snapshot combines them under one key rather than dropping or duplicating.
    assert len(rows) == 76, f"expected 76 holdings after combining lots, got {len(rows)}"
    assert {r["as_of"] for r in rows} == {"2026-06-30"}
    assert {r["fund_id"] for r in rows} == {FUND_ID}


def test_a_filing_for_an_untracked_series_publishes_no_holdings(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """A filing whose series the operator has not enabled cannot place its holdings — there is no
    fund_id, and `fund_holding` must say zero rather than invent a fund."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    _Capture.last = []
    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [XLK_KEY])
    FakePostgres.known = dict(XLK_KNOWN)
    try:
        result = materialise(
            tmp_path,
            [
                discovery_raw.raw_nport_filing,
                discovery_core.discovered_security,
                discovery_core.fund_holding,
            ],
            XLK_KEY,
            instance=instance,
            provider=_serve,
        )
    finally:
        FakePostgres.known = {}

    assert result.success
    assert _Capture.last == []


def test_the_placeholder_collapse_is_impossible_at_the_planning_layer() -> None:
    """THE MUTATION THE WHOLE FAMILY EXISTS FOR: two companies whose only identifier is
    `000000000` resolve to no security — they cannot be ingested, so they cannot collapse into
    whichever was seen first, the thing that made XLK's weights sum to 97.1%."""
    xml = (
        b"<nport>"
        b"<invstOrSec><name>Accenture Plc</name><identifiers><cusip>000000000</cusip>"
        b"</identifiers></invstOrSec>"
        b"<invstOrSec><name>Seagate Technology</name><identifiers><cusip>000000000</cusip>"
        b"</identifiers></invstOrSec>"
        b'<invstOrSec><name>Apple Inc</name><identifiers><isin value="US0378331005"/>'
        b"</identifiers></invstOrSec>"
        b"</nport>"
    )
    _, _, _, ids = nport.plan_holdings(
        nport.parse_holdings(xml), known_identifiers={}, countries=set(), source="sec-nport"
    )
    assert ids[0] is None and ids[1] is None
    assert ids[2] is not None


def test_the_exchange_sweep_resumes_from_the_cursor_inside_its_own_file(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """The captured AU page (100 rows, a real `next`) followed by an exhausted page. The venue
    partition's file holds both pages; the cursor passed to the second request is the first page's
    own `next` — the sweep resumes from its own record, not from a control table."""
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    page1 = (FIX / "openfigi_filter_au_page1.json").read_bytes()
    page2 = json.dumps(
        {
            "data": [{"figi": "BBG000B9XFU1", "ticker": "LAST", "securityType2": "Common Stock"}],
            "next": None,
            "total": 2117,
        }
    ).encode()
    cursor1 = json.loads(page1)["next"]

    calls: list[str | None] = []
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU"])

    def sweeper(exch_code: str, *, cursor: str | None = None, **kw: Any) -> Document:
        calls.append(cursor)
        return _doc(page1 if cursor is None else page2)

    saved = figi_provider.filter_exchange
    figi_provider.filter_exchange = sweeper
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep],
            partition_key="AU",
            instance=instance,
            resources={"parquet_io": ParquetIOManager(str(tmp_path))},
        )
    finally:
        figi_provider.filter_exchange = saved

    assert result.success
    # First request starts fresh; the second carries the first page's own cursor, and the venue
    # ended (next=None) so it stopped.
    assert calls == [None, cursor1], calls

    from pyarrow import parquet as pq

    rows = pq.read_table(str(tmp_path / "raw_exchange_sweep" / "AU.parquet")).to_pylist()
    assert len(rows) == 2
    assert rows[-1]["cursor_at"] is None
    assert bytes(rows[0]["body"]) == page1  # the response body is stored whole
