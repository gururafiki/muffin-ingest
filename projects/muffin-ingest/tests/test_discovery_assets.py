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
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

FIX = FIXTURES

XLK_BODY = (FIX / "sec_nport_primary_xlk.xml").read_bytes()
XLK_KEY = "1064641:000141036826075254"
FUND_ID = "99999999-9999-4999-8999-999999999999"

#: XLK's holdings resolved the way discovered_security would resolve them on a fresh database.
XLK_HOLDINGS = nport.parse_holdings(XLK_BODY)
_XLK_SECS, _XLK_IDENTS, _XLK_ISS, _XLK_IDS = nport.plan_holdings(
    XLK_HOLDINGS,
    known_identifiers={},
    known_issuers={},
    countries={"US", "JP"},
    source="sec-nport",
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
        known_issuers: dict[str, str] | None = None,
        directory_query: Sequence[tuple[Any, ...]] = (),
        exchanges: Sequence[tuple[Any, ...]] = (),
    ) -> None:
        self._directory_query = directory_query
        self._exchanges = exchanges
        self._countries = countries
        self._tracked = tracked
        self._known = known_identifiers
        self._fund_by_series = fund_by_series
        self._fund_by_symbol = fund_by_symbol
        self._issuers = known_issuers or {}
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        # A `uuid` COLUMN COMES BACK AS `uuid.UUID` FROM PSYCOPG, unless the query casts it. The
        # fake used to hand back strings, so no test could see a read id reach `json.dumps` — the
        # lane's third production run died on exactly that.
        uncast = "security_id::text" not in text
        # AN RPC IS A WRITE THAT STARTS WITH "select". Recorded like any other write, and answered
        # with the number of rows it was handed, which is what the real one returns on a database
        # holding none of these bonds' terms yet.
        if text.startswith("select market.set_debt_terms"):
            FakeCursor.writes.append((text, tuple(params)))
            self.rows = [(len(json.loads(params[0])),)]
            return
        # READ queries are answered by shape; anything else is a WRITE and is recorded for the
        # assertions on what reached the database. Guarded with `startswith("select")` because the
        # identifier INSERT contains the words of the identifier SELECT.
        if text.startswith("select"):
            if "from market.directory_query" in text:
                self.rows = list(self._directory_query)
            elif "from market.exchange where enabled" in text:
                self.rows = list(self._exchanges)
            elif "from market.countries" in text:
                self.rows = [(iso2,) for iso2, _ in self._countries]
            elif "from market.issuer where lei is not null" in text:
                self.rows = list(self._issuers.items())
            # BOTH "kind_code = 'ticker'" queries contain that phrase, so the more specific
            # (series join) must be matched before the generic one.
            elif "t.series_id, i.security_id" in text:
                self.rows = [
                    (series, _as_read(sid, uncast)) for series, sid in self._fund_by_series.items()
                ]
            elif "kind_code = 'ticker'" in text:
                self.rows = [
                    (symbol, _as_read(sid, uncast)) for symbol, sid in self._fund_by_symbol.items()
                ]
            elif "from market.tracked_fund" in text:
                self.rows = list(self._tracked)
            elif "market.security_identifier" in text:
                self.rows = []
                for key, sid in self._known.items():
                    kind, value = key.split(":", 1)
                    self.rows.append((kind, value, _as_read(sid, uncast)))
            else:
                self.rows = []
        else:
            FakeCursor.writes.append((text, tuple(params)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


def _as_read(sid: str, uncast: bool) -> Any:
    """What psycopg would return for a `uuid` column: a `uuid.UUID` unless the query cast it."""
    import uuid

    if not uncast:
        return sid
    try:
        return uuid.UUID(sid)
    except ValueError:
        return sid  # a test id that is not uuid-shaped; nothing real reads one


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
        known_issuers: dict[str, str] | None = None,
        directory_query: Sequence[tuple[Any, ...]] = (),
        exchanges: Sequence[tuple[Any, ...]] = (),
    ) -> None:
        self._directory_query = directory_query
        self._exchanges = exchanges
        self._countries = countries
        self._tracked = tracked
        self._known = known_identifiers or {}
        self._fund_by_series = fund_by_series or {}
        self._fund_by_symbol = fund_by_symbol or {}
        self._issuers = known_issuers or {}

    def cursor(self) -> FakeCursor:
        return FakeCursor(
            countries=self._countries,
            tracked=self._tracked,
            known_identifiers=self._known,
            fund_by_series=self._fund_by_series,
            fund_by_symbol=self._fund_by_symbol,
            known_issuers=self._issuers,
            directory_query=self._directory_query,
            exchanges=self._exchanges,
        )

    def commit(self) -> None:
        return None


#: `market.directory_query` as the sweep tests see it: query_key, exch_code_asked, files_under,
#: security_type2, maps_to_composite. `US.arca` is the one alias: asked of NYSE Arca, filed as US.
DIRECTORY_QUERY: list[tuple[str, str, str, str, bool]] = [
    ("AU.common", "AU", "AU", "Common Stock", False),
    ("GR.common", "GR", "GR", "Common Stock", False),
    ("LN.common", "LN", "LN", "Common Stock", False),
    ("US.arca", "UP", "US", "Common Stock", True),
    ("US.common", "US", "US", "Common Stock", False),
    ("US.reit", "US", "US", "REIT", False),
]

#: `market.exchange where enabled`: exch_code, suffix, country_iso2.
EXCHANGES: list[tuple[str, str, str]] = [
    ("AU", ".AX", "AU"),
    ("GR", ".DE", "DE"),
    ("LN", ".L", "GB"),
    ("US", "", "US"),
]


class FakePostgres(Postgres):
    directory_query: ClassVar[list[tuple[Any, ...]]] = DIRECTORY_QUERY
    exchanges: ClassVar[list[tuple[Any, ...]]] = EXCHANGES
    known: ClassVar[dict[str, str]] = {}
    fund_by_series: ClassVar[dict[str, str]] = {}
    fund_by_symbol: ClassVar[dict[str, str]] = {}
    #: lei → issuer_id already in `market.issuer`, as the edge minted them (random ids).
    known_issuers: ClassVar[dict[str, str]] = {}

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield FakeConn(
            known_identifiers=self.known,
            fund_by_series=self.fund_by_series,
            fund_by_symbol=self.fund_by_symbol,
            known_issuers=self.known_issuers,
            directory_query=self.directory_query,
            exchanges=self.exchanges,
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


#: A bond fund's filing, cut down to what the debt-term and lookup rules read. Three holdings:
#: a ZERO-COUPON note whose coupon kind is literally "None", a DEFAULTED bond in a currency and an
#: issuer category no database has seen, and an equity with no `<debtSec>` at all.
BOND_FUND_KEY = "999:000099999926000001"
BOND_FUND_BODY = b"""<edgarSubmission><formData><genInfo>
<seriesId>S000099999</seriesId><repPdDate>2026-06-30</repPdDate></genInfo><invstOrSecs>
<invstOrSec><name>T926 Zero Coupon Note</name><title>ZERO 2030</title>
<identifiers><isin value="US91282CAB12"/></identifiers><balance>1000</balance><units>PA</units>
<curCd>USD</curCd><valUSD>900</valUSD><pctVal>0.5</pctVal><assetCat>DBT</assetCat>
<issuerCat>UST</issuerCat><invCountry>US</invCountry>
<debtSec><maturityDt>2030-01-15</maturityDt><couponKind>None</couponKind>
<annualizedRt>0.0</annualizedRt><isDefault>N</isDefault></debtSec></invstOrSec>
<invstOrSec><name>T926 Defaulted Bond</name><title>DEF 2031</title>
<identifiers><isin value="US91282CCD34"/></identifiers><balance>500</balance><units>PA</units>
<curCd>XTS</curCd><valUSD>200</valUSD><pctVal>0.2</pctVal><assetCat>DBT</assetCat>
<issuerCat>T926</issuerCat><invCountry>US</invCountry>
<debtSec><maturityDt>2031-06-01</maturityDt><couponKind>Fixed</couponKind>
<annualizedRt>4.25</annualizedRt><isDefault>Y</isDefault></debtSec></invstOrSec>
<invstOrSec><name>T926 Equity</name><title>EQ</title>
<identifiers><isin value="US0378331005"/></identifiers><balance>10</balance><units>NS</units>
<curCd>USD</curCd><valUSD>2000</valUSD><pctVal>1.0</pctVal><assetCat>EC</assetCat>
<issuerCat>CORP</issuerCat><invCountry>US</invCountry></invstOrSec>
</invstOrSecs></formData></edgarSubmission>"""


def _serve_bond_fund(cik: str, accession: str, **kw: Any) -> Document:
    return _doc(BOND_FUND_BODY)


def test_discovery_writes_the_debt_terms_the_edge_used_to(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """Bond terms reach `market.set_debt_terms`, which only the edge's `fund-holdings` ever called.

    The parser always read them; the rows were thrown away, so retiring that resource would have
    stopped bond terms with no error. The zero-coupon note is the one that goes wrong quietly: a
    truthiness test on the rate turns every such note into a missing rate.
    """
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    FakeCursor.writes.clear()
    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [BOND_FUND_KEY])
    result = materialise(
        tmp_path,
        [discovery_raw.raw_nport_filing, discovery_core.discovered_security],
        BOND_FUND_KEY,
        instance=instance,
        provider=_serve_bond_fund,
    )
    assert result.success
    calls = [params for sql, params in FakeCursor.writes if "set_debt_terms" in sql]
    assert len(calls) == 1, calls
    rows = {r["maturity_date"]: r for r in json.loads(calls[0][0])}
    assert set(rows) == {"2030-01-15", "2031-06-01"}, "the equity has no debt terms to write"
    zero = rows["2030-01-15"]
    assert zero["coupon_rate"] == 0.0 and zero["coupon_rate"] is not None, zero
    assert zero["coupon_kind_code"] == "None", "a REPORTED kind, not an absence"
    assert zero["in_default"] is False
    assert rows["2031-06-01"]["in_default"] is True
    assert {r["as_of"] for r in rows.values()} == {"2026-06-30"}, "the filing's own report date"

    events = result.asset_materializations_for_node(discovery_core.discovered_security.op.name)
    meta: dict[str, Any] = {k: v.value for k, v in events[0].metadata.items()}
    assert meta["debt_terms_offered"] == 2 and meta["debt_terms_updated"] == 2, meta


def test_discovery_learns_a_new_lookup_code_before_writing_what_references_it(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """A currency or category the database has never seen is inserted BEFORE the securities that
    reference it. Written after them, or not at all, the first filing to carry one fails its whole
    transaction on a foreign key — which is what retiring the edge's `learnLookups` would do."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    FakeCursor.writes.clear()
    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [BOND_FUND_KEY])
    result = materialise(
        tmp_path,
        [discovery_raw.raw_nport_filing, discovery_core.discovered_security],
        BOND_FUND_KEY,
        instance=instance,
        provider=_serve_bond_fund,
    )
    assert result.success
    order = [sql for sql, _ in FakeCursor.writes]

    def first(prefix: str) -> int:
        at = next((i for i, sql in enumerate(order) if sql.startswith(prefix)), None)
        assert at is not None, f"nothing was written by `{prefix}…` — the code is never learned"
        return at

    security_at = first("insert into market.security ")
    for table in ("currency", "asset_category", "issuer_category", "coupon_kind"):
        assert first(f"insert into market.{table} ") < security_at, table
    currency_params = next(p for sql, p in FakeCursor.writes if "into market.currency " in sql)
    assert "XTS" in currency_params, currency_params
    category_params = next(
        p for sql, p in FakeCursor.writes if "into market.issuer_category " in sql
    )
    assert "T926" in category_params, category_params
    # DO NOTHING, never DO UPDATE: a name corrected by hand must survive the next filing.
    assert all(
        "do update" not in sql
        for sql, _ in FakeCursor.writes
        if sql.startswith(("insert into market.currency ", "insert into market.asset_category "))
    )


def test_two_filings_in_one_run_holding_one_new_security_mint_it_once(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """A run covers several filings, and a security new to the database is new to ALL of them.

    Resolution kept one snapshot of what the database knew and gave every filing a copy, so two
    funds holding the same new bond in one run minted it twice — and the second copy lost its
    identifiers to the DO NOTHING, leaving a security nothing could resolve to. Two partitions of
    the same XLK filing make the two rules disagree: 76 securities, or 152.
    """
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    second = "1064641:000141036826075255"
    FakeCursor.writes.clear()
    instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [XLK_KEY, second])
    saved = sec_nport.primary_doc
    provider: Any = _serve  # the same patch `materialise` applies, for a two-partition range
    sec_nport.primary_doc = provider
    try:
        result = dg.materialize(
            [discovery_raw.raw_nport_filing, discovery_core.discovered_security],
            instance=instance,
            tags={
                "dagster/asset_partition_range_start": XLK_KEY,
                "dagster/asset_partition_range_end": second,
            },
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "postgres_io": _Capture(),
            },
        )
    finally:
        sec_nport.primary_doc = saved
    assert result.success
    events = result.asset_materializations_for_node(discovery_core.discovered_security.op.name)
    meta: dict[str, Any] = {k: v.value for k, v in events[0].metadata.items()}
    assert meta["filings"] == 2, meta
    assert meta["securities"] == 76, f"one XLK filing twice must mint 76 securities, not {meta}"


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
        nport.parse_holdings(xml),
        known_identifiers={},
        known_issuers={},
        countries=set(),
        source="sec-nport",
    )
    assert ids[0] is None and ids[1] is None
    assert ids[2] is not None


def _no_waiting(monkeypatch_target: Any, pauses: list[float] | None = None) -> Any:
    """Replace the walk's waits with a recorder. A test must not pay 3 s a page, or 65 s a refusal,
    to a provider nobody is calling; replacing `time.sleep` instead would reach Dagster's
    executor."""
    saved = monkeypatch_target._pause

    def record(seconds: float) -> None:
        if pauses is not None:
            pauses.append(seconds)

    monkeypatch_target._pause = record
    return saved


def test_the_exchange_sweep_resumes_from_the_cursor_inside_its_own_file(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """The captured AU page (100 rows, a real `next`) followed by an exhausted page. The query's
    partition file holds both pages; the cursor passed to the second request is the first page's
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
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common"])

    def sweeper(exch_code: str, *, cursor: str | None = None, **kw: Any) -> Document:
        calls.append(cursor)
        return _doc(page1 if cursor is None else page2)

    saved = figi_provider.filter_exchange
    figi_provider.filter_exchange = sweeper
    saved_pause = _no_waiting(discovery_raw)
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep],
            partition_key="AU.common",
            instance=instance,
            resources={
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres": FakePostgres(),
            },
        )
    finally:
        figi_provider.filter_exchange = saved
        discovery_raw._pause = saved_pause

    assert result.success
    # First request starts fresh; the second carries the first page's own cursor, and the walk
    # ended (next=None) so it stopped.
    assert calls == [None, cursor1], calls

    from pyarrow import parquet as pq

    rows = pq.read_table(str(tmp_path / "raw_exchange_sweep" / "AU.common.parquet")).to_pylist()
    assert len(rows) == 2
    assert rows[-1]["cursor_at"] is None
    assert bytes(rows[0]["body"]) == page1  # the response body is stored whole


# --- the walk: resume merges, a refresh replaces -------------------------------------------------


def _page(
    *,
    figi: str,
    next_cursor: str | None,
    total: int = 2117,
    composite: str | None = None,
    security_type2: str = "Common Stock",
) -> bytes:
    line: dict[str, Any] = {"figi": figi, "ticker": figi[-4:], "securityType2": security_type2}
    if composite:
        line["compositeFIGI"] = composite
    return json.dumps({"data": [line], "next": next_cursor, "total": total}).encode()


def _sweep(
    tmp_path: Path,
    instance: dg.DagsterInstance,
    pages: dict[str | None, bytes],
    *,
    key: str = "AU.common",
    throttle_after: int | None = None,
    refuse: Sequence[int] = (),
    tags: dict[str, str] | None = None,
    asked_with: list[tuple[str, str]] | None = None,
    pauses: list[float] | None = None,
) -> tuple[Any, list[str | None]]:
    """Materialise one query once against a scripted provider; return the result and the cursors
    it asked with.

    `throttle_after` refuses EVERY request after the Nth — the provider refusing us; `refuse`
    refuses only the requests with those (1-based) numbers — the minute running out once.
    `asked_with` collects the (exchange code, securityType2) of each request, `pauses` each wait.
    """
    from muffin_ingest.providers.openfigi import OpenFigiThrottled

    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    asked: list[str | None] = []

    def sweeper(
        exch_code: str, *, cursor: str | None = None, security_type2: str = "?", **kw: Any
    ) -> Document:
        asked.append(cursor)
        if asked_with is not None:
            asked_with.append((exch_code, security_type2))
        n = len(asked)
        if (throttle_after is not None and n > throttle_after) or n in refuse:
            raise OpenFigiThrottled("429")
        return _doc(pages[cursor])

    saved = figi_provider.filter_exchange
    figi_provider.filter_exchange = sweeper
    saved_pause = _no_waiting(discovery_raw, pauses)
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep],
            partition_key=key,
            instance=instance,
            resources={
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres": FakePostgres(),
            },
            tags=tags,
            raise_on_error=False,
        )
    finally:
        figi_provider.filter_exchange = saved
        discovery_raw._pause = saved_pause
    return result, asked


def _stored(tmp_path: Path, key: str = "AU.common") -> list[dict[str, Any]]:
    from pyarrow import parquet as pq

    path = tmp_path / "raw_exchange_sweep" / f"{key}.parquet"
    if not path.exists():
        return []
    table = pq.read_table(str(path))
    if table.column_names == ["collected_nothing"]:
        return []
    rows: list[dict[str, Any]] = table.to_pylist()
    return rows


def _attempts() -> int:
    from muffin_ingest_dagster.defs.discovery.raw import REFUSAL_RETRIES

    return REFUSAL_RETRIES + 1


def test_a_resumed_sweep_keeps_the_pages_it_already_stored(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """THE DEFECT THIS EXISTS FOR: the asset returns only the pages of the CURRENT run, so without
    `merge_on` the manager replaced the file and a resumed walk kept the tail while silently
    losing everything before it.

    The fixture makes the two rules disagree: the first run is refused after one page, so a
    replacing manager would leave the walk at one row and a merging one at two.
    """
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common"])
    pages = {
        None: _page(figi="BBG0000000P1", next_cursor="c1"),
        "c1": _page(figi="BBG0000000P2", next_cursor=None),
    }

    first, asked = _sweep(tmp_path, instance, pages, throttle_after=1)
    assert first.success, "pages were fetched, so the refusal is filed rather than failed"
    assert asked == [None] + ["c1"] * _attempts(), "page two was asked, and asked again"
    stored = _stored(tmp_path)
    assert len(stored) == 1, "a refused first run files the page it did get"
    assert stored[-1]["cursor_at"] == "c1", "and says the walk has more to fetch"

    second, asked = _sweep(tmp_path, instance, pages)
    assert second.success
    assert asked == ["c1"], "the resume starts from the file's own cursor, not from the beginning"

    stored = _stored(tmp_path)
    assert len(stored) == 2, f"the resumed walk lost its earlier page: {stored}"
    assert [row["cursor_from"] for row in stored] == ["", "c1"]
    assert [row["page"] for row in stored] == [0, 1], "page numbering continues across the resume"
    assert stored[-1]["cursor_at"] is None, "and the walk is now finished"


def test_a_finished_walk_is_re_walked_and_replaces_rather_than_accumulating(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """OPENFIGI HAS NO AS-OF, so refreshing a query means walking it again — and the second walk's
    pages are the whole answer, not an extension. Keeping both would grow the partition on every
    refresh and leave nobody able to say which listings are current.

    THE TWO WALKS USE DIFFERENT CURSORS ON PURPOSE: with distinct cursors the rules disagree —
    merging keeps the orphaned first-walk page and leaves three, replacing leaves two.
    """
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common"])
    first_walk = {
        None: _page(figi="BBG0000000P1", next_cursor="c1"),
        "c1": _page(figi="BBG0000000P2", next_cursor=None),
    }
    result, asked = _sweep(tmp_path, instance, first_walk)
    assert result.success and asked == [None, "c1"]
    assert [row["page"] for row in _stored(tmp_path)] == [0, 1]

    second_walk = {
        None: _page(figi="BBG0000000Q1", next_cursor="d1"),
        "d1": _page(figi="BBG0000000Q2", next_cursor=None),
    }
    result, asked = _sweep(tmp_path, instance, second_walk)
    assert result.success
    assert asked == [None, "d1"], "a finished walk is re-walked from the beginning, not resumed"

    stored = _stored(tmp_path)
    assert len(stored) == 2, f"the refresh accumulated a second generation of pages: {stored}"
    assert all(b"BBG0000000Q" in bytes(row["body"]) for row in stored), stored
    assert [row["cursor_from"] for row in stored] == ["", "d1"]
    # A NEW WALK STARTS AT PAGE ZERO. Continuing the old walk's numbering would make `page` say
    # this query has more pages than the provider ever served it.
    assert [row["page"] for row in stored] == [0, 1], stored


# --- refusals ------------------------------------------------------------------------------------


def test_a_refusal_is_waited_out_and_the_walk_goes_on(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """THE MINUTE RUNNING OUT IS ORDINARY. Keyed pacing is 3 s a page, which is 20 a minute — the
    rate the bucket refills at, not under it — so a long walk can drain it. The refused page is
    asked again after a cool-down and the walk finishes in the same run, where until 2026-10-04 it
    stopped and waited up to an hour for the resume sensor."""
    from muffin_ingest_dagster.defs.discovery.raw import refusal_cooldown

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common"])
    pages = {
        None: _page(figi="BBG0000000P1", next_cursor="c1"),
        "c1": _page(figi="BBG0000000P2", next_cursor=None),
    }
    pauses: list[float] = []
    result, asked = _sweep(tmp_path, instance, pages, refuse=[2], pauses=pauses)

    assert result.success
    assert asked == [None, "c1", "c1"], "the refused page was asked once more"
    assert refusal_cooldown() in pauses, "after a cool-down, not at once"
    stored = _stored(tmp_path)
    assert [row["page"] for row in stored] == [0, 1] and stored[-1]["cursor_at"] is None


def test_a_query_refused_before_any_page_fails_and_claims_nothing(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """A SUCCESSFUL RUN CLAIMS ITS PARTITION, so a run that fetched nothing must not succeed: a new
    query would read as walked, and a monthly refresh as done while the file holds last month's
    walk. Failing loses nothing, because there is nothing to file.

    The control is the stored walk: it is left exactly as it was, not replaced by an empty one."""
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common"])
    pages: dict[str | None, bytes] = {None: _page(figi="BBG0000000P1", next_cursor=None)}
    result, _ = _sweep(tmp_path, instance, pages)
    assert result.success and len(_stored(tmp_path)) == 1

    result, asked = _sweep(tmp_path, instance, pages, throttle_after=0)
    assert not result.success, "a refresh the provider refused outright is a failed run"
    assert asked == [None] * _attempts()
    failure = next(e for e in result.all_events if e.event_type == dg.DagsterEventType.STEP_FAILURE)
    assert failure.step_failure_data.error is not None
    assert "nothing is claimed" in failure.step_failure_data.error.to_string()
    assert len(_stored(tmp_path)) == 1, "the stored walk survived the refused refresh"

    # AND A QUERY NEVER WALKED BEFORE GETS NO FILE AT ALL — an empty marker would make it read as
    # asked and answered with nothing.
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["LN.common"])
    result, _ = _sweep(tmp_path, instance, pages, key="LN.common", throttle_after=0)
    assert not result.success
    assert not (tmp_path / "raw_exchange_sweep" / "LN.common.parquet").exists()


def test_a_refusal_stops_the_run_before_the_next_query(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """Only an operator's range puts two queries in one run. A refusal that outlasts its cool-downs
    in the first must not spend three more refused requests on the second, and the run must say
    the second was not asked — the four outcomes sum to the queries."""
    from muffin_ingest.providers.openfigi import OpenFigiThrottled

    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.common", "US.reit"])
    asked: list[tuple[str, str | None]] = []

    def sweeper(
        exch_code: str, *, cursor: str | None = None, security_type2: str = "?", **kw: Any
    ) -> Document:
        asked.append((security_type2, cursor))
        if cursor is None:
            return _doc(_page(figi="BBG000COMMN1", next_cursor="c1"))
        raise OpenFigiThrottled("429")

    saved = figi_provider.filter_exchange
    figi_provider.filter_exchange = sweeper
    saved_pause = _no_waiting(discovery_raw)
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep],
            instance=instance,
            resources={
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres": FakePostgres(),
            },
            tags={
                "dagster/asset_partition_range_start": "US.common",
                "dagster/asset_partition_range_end": "US.reit",
            },
        )
    finally:
        figi_provider.filter_exchange = saved
        discovery_raw._pause = saved_pause

    assert result.success, "a page was fetched, so it is filed"
    assert {t for t, _ in asked} == {"Common Stock"}, "the REIT query was never asked"
    meta = result.asset_materializations_for_node("raw_exchange_sweep")[0].metadata
    outcomes = ("new_walks", "resumed_walks", "already_finished", "unasked")
    assert {k: meta[k].value for k in outcomes} == {
        "new_walks": 1,
        "resumed_walks": 0,
        "already_finished": 0,
        "unasked": 1,
    }
    assert meta["queries"].value == 2, "so the four outcomes sum to the queries"
    assert meta["refused"].value == 1


def test_the_pacing_is_per_request_across_queries(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """The second query's first page waits like any other page. Until 2026-10-04 the wait sat
    inside one query's loop, so consecutive queries went out back to back and spent the bucket's
    slack at every boundary. Two one-page queries make exactly one wait."""
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.common", "US.reit"])

    def sweeper(
        exch_code: str, *, cursor: str | None = None, security_type2: str = "?", **kw: Any
    ) -> Document:
        return _doc(
            _page(figi="BBG000PAGE01", next_cursor=None, total=1, security_type2=security_type2)
        )

    pauses: list[float] = []
    saved, saved_pause = figi_provider.filter_exchange, _no_waiting(discovery_raw, pauses)
    figi_provider.filter_exchange = sweeper
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep],
            instance=instance,
            resources={
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres": FakePostgres(),
            },
            tags={
                "dagster/asset_partition_range_start": "US.common",
                "dagster/asset_partition_range_end": "US.reit",
            },
        )
    finally:
        figi_provider.filter_exchange, discovery_raw._pause = saved, saved_pause

    assert result.success
    assert pauses == [discovery_raw.sweep_pacing()]


# --- one partition per question ------------------------------------------------------------------


def test_each_query_asks_its_own_type_and_records_the_request(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """`US.reit` must ASK for REITs — OpenFIGI types them apart from common stock, which is why
    none was in the directory on any venue until 2026-10-04 (PLD, AMT, O, SPG all absent). And the
    page must RECORD what it asked: an empty page has no rows to read the type from, and the pair
    is how a run covering several queries routes each page to its own file."""
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.reit"])
    asked_with: list[tuple[str, str]] = []
    pages: dict[str | None, bytes] = {
        None: _page(figi="BBG000REIT01", next_cursor=None, total=1, security_type2="REIT")
    }
    result, _ = _sweep(tmp_path, instance, pages, key="US.reit", asked_with=asked_with)
    assert result.success

    assert asked_with == [("US", "REIT")]
    stored = _stored(tmp_path, "US.reit")
    assert [(row["exch_code"], row["security_type2"]) for row in stored] == [("US", "REIT")]


def test_an_alias_asks_its_own_code_and_files_its_pages_under_its_own_key(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """`US.arca` asks NYSE Arca (`UP`), not the capped US composite, and its pages are its own
    partition's: which venue its LINES belong to is stage 2's reading, not the page's."""
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.arca"])
    asked_with: list[tuple[str, str]] = []
    pages: dict[str | None, bytes] = {
        None: _page(figi="BBG0154FBJJ5", next_cursor=None, total=1, composite="BBG013QNJHP8")
    }
    result, _ = _sweep(tmp_path, instance, pages, key="US.arca", asked_with=asked_with)
    assert result.success

    assert asked_with == [("UP", "Common Stock")]
    stored = _stored(tmp_path, "US.arca")
    assert [(row["exch_code"], row["security_type2"]) for row in stored] == [("UP", "Common Stock")]


def test_a_run_over_several_queries_routes_each_page_to_its_own_file(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """An operator's range hands one run several queries, and the I/O manager then needs to know
    which page belongs to which. The two queries here share a venue, so the venue alone cannot tell
    them apart — only the recorded request can."""
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.common", "US.reit"])

    def sweeper(
        exch_code: str, *, cursor: str | None = None, security_type2: str = "?", **kw: Any
    ) -> Document:
        figi = "BBG000COMMN1" if security_type2 == "Common Stock" else "BBG000REIT01"
        return _doc(_page(figi=figi, next_cursor=None, total=1, security_type2=security_type2))

    saved, saved_pause = figi_provider.filter_exchange, _no_waiting(discovery_raw)
    figi_provider.filter_exchange = sweeper
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep],
            instance=instance,
            resources={
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres": FakePostgres(),
            },
            tags={
                "dagster/asset_partition_range_start": "US.common",
                "dagster/asset_partition_range_end": "US.reit",
            },
        )
    finally:
        figi_provider.filter_exchange, discovery_raw._pause = saved, saved_pause

    assert result.success
    assert [b"BBG000COMMN1" in bytes(r["body"]) for r in _stored(tmp_path, "US.common")] == [True]
    assert [b"BBG000REIT01" in bytes(r["body"]) for r in _stored(tmp_path, "US.reit")] == [True]


def test_a_key_no_query_lists_fails_and_names_the_remedy(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """The 59 per-venue keys from before the re-key (`US`, `LN`) are the expected case. Walking
    one would ask a question nobody listed; the run fails and says to delete the key."""
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US"])
    pages: dict[str | None, bytes] = {None: _page(figi="BBG0000000U1", next_cursor=None)}
    result, asked = _sweep(tmp_path, instance, pages, key="US")

    assert not result.success
    assert asked == [], "nothing was asked of the provider"
    failure = next(e for e in result.all_events if e.event_type == dg.DagsterEventType.STEP_FAILURE)
    assert failure.step_failure_data.error is not None
    message = failure.step_failure_data.error.to_string()
    assert "not in market.directory_query" in message and "delete_dynamic_partition" in message


def test_a_resume_run_never_starts_a_new_walk(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """`unfinished_sweeps` asks for a walk the provider refused part-way. If another run finished
    it first, the resume must ask nothing — starting from page one would spend a whole query to
    learn what is already held. The control: the same run on an unfinished walk does resume."""
    from muffin_ingest_dagster.defs.discovery.partitions import SWEEP_RESUME_TAG

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common"])
    pages = {
        None: _page(figi="BBG0000000P1", next_cursor="c1"),
        "c1": _page(figi="BBG0000000P2", next_cursor=None),
    }
    resume = {SWEEP_RESUME_TAG: "true"}

    result, asked = _sweep(tmp_path, instance, pages, throttle_after=1)
    assert result.success and _stored(tmp_path)[-1]["cursor_at"] == "c1"

    result, asked = _sweep(tmp_path, instance, pages, tags=resume)
    assert result.success
    assert asked == ["c1"], "an unfinished walk is resumed"

    result, asked = _sweep(tmp_path, instance, pages, tags=resume)
    assert result.success
    assert asked == [], "a finished walk is not re-walked by a resume"
    assert len(_stored(tmp_path)) == 2, "and what it holds is untouched"


def test_stage_2_files_an_alias_line_as_the_us_line_it_names(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """NYSE Arca's BellRing line `BBG0154FBJJ5` names `BBG013QNJHP8`, the US line the capped walk
    dropped. Stage 2 must write the US line, filed under US with the bare ticker as its price
    symbol — and count, not file, a line naming no composite."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.arca"])
    page = json.dumps(
        {
            "data": [
                {
                    "figi": "BBG0154FBJJ5",
                    "compositeFIGI": "BBG013QNJHP8",
                    "shareClassFIGI": "BBG013QNJHW0",
                    "ticker": "BRBR",
                    "name": "BELLRING BRANDS INC",
                    "securityType": "Common Stock",
                    "securityType2": "Common Stock",
                },
                {"figi": "BBG000ORPHAN", "ticker": "ORPH", "securityType2": "Common Stock"},
            ],
            "total": 2,
        }
    ).encode()

    def sweeper(exch_code: str, *, cursor: str | None = None, **kw: Any) -> Document:
        return _doc(page)

    saved, saved_pause = figi_provider.filter_exchange, _no_waiting(discovery_raw)
    figi_provider.filter_exchange = sweeper
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep, discovery_core.venue_listing],
            partition_key="US.arca",
            instance=instance,
            resources={
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres": FakePostgres(),
                "postgres_io": _Capture(),
            },
        )
    finally:
        figi_provider.filter_exchange, discovery_raw._pause = saved, saved_pause

    assert result.success
    assert [(r["figi"], r["exch_code"], r["provider_symbol"]) for r in _Capture.last] == [
        ("BBG013QNJHP8", "US", "BRBR")
    ]
    meta = result.asset_materializations_for_node("venue_listing")[0].metadata
    assert meta["mapped_to_composite"].value == 1
    assert meta["unplaced_without_composite"].value == 1


# --- the checks ----------------------------------------------------------------------------------


def test_the_check_names_the_walks_to_resume(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """A half-walked query is an ordinary materialized partition and its freshness policy is
    satisfied the moment it stops, so nothing else in the system can tell it from a finished one.
    """
    from muffin_ingest_dagster.defs.discovery.checks import venue_sweep_reached_its_last_page

    instance.add_dynamic_partitions(
        discovery_partitions.SWEEP_PARTITIONS, ["AU.common", "LN.common"]
    )
    stalled: dict[str | None, bytes] = {None: _page(figi="BBG0000000P1", next_cursor="c1")}
    # CAPPED, AND STILL FINISHED: one row against a total of 2,117 and no cursor. Capping is the
    # other check's subject, and naming it here would send a resume that cannot help.
    finished: dict[str | None, bytes] = {None: _page(figi="BBG0000000R1", next_cursor=None)}
    _sweep(tmp_path, instance, stalled, key="AU.common", throttle_after=1)
    _sweep(tmp_path, instance, finished, key="LN.common")

    evaluation = venue_sweep_reached_its_last_page(
        dg.build_asset_check_context(instance=instance), RawStore(base_path=str(tmp_path))
    )
    assert isinstance(evaluation, dg.AssetCheckResult)

    assert evaluation.passed is False
    assert evaluation.metadata["queries"].value == 2
    assert evaluation.metadata["unfinished"].value == 1
    assert evaluation.metadata["never_swept"].value == 0
    # NAMED, NOT JUST COUNTED: the names are what `unfinished_sweeps` resumes.
    assert evaluation.metadata["resume_these"].value == "AU.common"


def test_the_check_answers_for_every_query_whichever_run_evaluates_it(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """A CHECK ON A PARTITIONED ASSET RECORDS ONE RESULT FOR THE ASSET, so a check that answered
    only for its run's partition reported whichever query ran last: a stalled walk passed the
    moment any other query finished. Run inside LN.common's materialisation, as production runs
    it, the check still names AU.common."""
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw
    from muffin_ingest_dagster.defs.discovery.checks import venue_sweep_reached_its_last_page

    instance.add_dynamic_partitions(
        discovery_partitions.SWEEP_PARTITIONS, ["AU.common", "LN.common"]
    )
    stalled: dict[str | None, bytes] = {None: _page(figi="BBG0000000P1", next_cursor="c1")}
    _sweep(tmp_path, instance, stalled, key="AU.common", throttle_after=1)

    def sweeper(exch_code: str, *, cursor: str | None = None, **kw: Any) -> Document:
        return _doc(_page(figi="BBG0000000R1", next_cursor=None))

    saved, saved_pause = figi_provider.filter_exchange, _no_waiting(discovery_raw)
    figi_provider.filter_exchange = sweeper
    try:
        result = dg.materialize(
            [discovery_raw.raw_exchange_sweep, venue_sweep_reached_its_last_page],
            partition_key="LN.common",
            instance=instance,
            resources={
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
                "postgres": FakePostgres(),
            },
        )
    finally:
        figi_provider.filter_exchange, discovery_raw._pause = saved, saved_pause

    assert result.success
    [evaluation] = result.get_asset_check_evaluations()
    assert evaluation.passed is False, "LN.common finished, AU.common did not"
    assert evaluation.metadata["resume_these"].value == "AU.common"


def test_a_capped_walk_passes_only_when_a_finished_alias_covers_it(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """NO CURSOR LEFT IS NOT FINISHED WHEN THE PROVIDER STOPPED ISSUING THEM — OpenFIGI's 15,000
    cap, which the US walk met on 2026-09-21 holding 15,000 of 20,096.

    THREE WALKS, ALL ENDED WITHOUT A CURSOR, AND THE RULES MUST DISAGREE ABOUT EACH:
    - US.common holds 2 of 500: capped, and `US.arca` covers it once Arca's own walk finishes;
    - GR.common holds 2 of 600: capped, and nothing covers it;
    - LN.common holds 2 of 3: drift between pages, not a cap.
    """
    from muffin_ingest_dagster.defs.discovery.checks import directory_query_within_the_cap

    keys = ["GR.common", "LN.common", "US.arca", "US.common"]
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, keys)

    def walk(prefix: str, total: int) -> dict[str | None, bytes]:
        return {
            None: _page(figi=f"BBG0000000{prefix}1", next_cursor=f"{prefix}1", total=total),
            f"{prefix}1": _page(figi=f"BBG0000000{prefix}2", next_cursor=None, total=total),
        }

    _sweep(tmp_path, instance, walk("U", 500), key="US.common")
    _sweep(tmp_path, instance, walk("G", 600), key="GR.common")
    _sweep(tmp_path, instance, walk("L", 3), key="LN.common")

    def evaluate() -> dg.AssetCheckResult:
        result = directory_query_within_the_cap(
            dg.build_asset_check_context(instance=instance),
            RawStore(base_path=str(tmp_path)),
            FakePostgres(),
        )
        assert isinstance(result, dg.AssetCheckResult)
        return result

    # ARCA NOT WALKED YET: an alias still to walk covers nothing.
    before = evaluate()
    assert before.passed is False
    assert before.metadata["capped"].value == 2
    assert before.metadata["capped_and_covered"].value == "none"
    assert before.metadata["capped_and_uncovered"].value == (
        "GR.common (2 of 600); US.common (2 of 500), US.arca not finished"
    )

    _sweep(tmp_path, instance, walk("A", 2), key="US.arca")
    after = evaluate()
    assert after.passed is False, "GR.common is capped and nothing covers it"
    assert after.metadata["capped_and_covered"].value == "US.common (2 of 500) covered by US.arca"
    assert after.metadata["capped_and_uncovered"].value == "GR.common (2 of 600)"


def test_the_finished_check_does_not_report_a_cap(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """The finished check's names are what `unfinished_sweeps` resumes, so a capped walk — which a
    re-walk cannot help — must pass it. Otherwise every resume would walk the capped query again."""
    from muffin_ingest_dagster.defs.discovery.checks import venue_sweep_reached_its_last_page

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.common"])
    capped: dict[str | None, bytes] = {
        None: _page(figi="BBG0000000U1", next_cursor=None, total=500)
    }
    _sweep(tmp_path, instance, capped, key="US.common")

    evaluation = venue_sweep_reached_its_last_page(
        dg.build_asset_check_context(instance=instance), RawStore(base_path=str(tmp_path))
    )
    assert isinstance(evaluation, dg.AssetCheckResult)
    assert evaluation.passed is True
    assert evaluation.metadata["resume_these"].value == "none"


# --- automation ----------------------------------------------------------------------------------


def _sweep_defs(tmp_path: Path) -> dg.Definitions:
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    return dg.Definitions(
        assets=[discovery_raw.raw_exchange_sweep],
        resources={
            "parquet_io": ParquetIOManager(str(tmp_path)),
            "raw_store": RawStore(base_path=str(tmp_path)),
            "postgres": FakePostgres(),
        },
    )


def _materialized(instance: dg.DagsterInstance, keys: Sequence[str]) -> None:
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    for key in keys:
        instance.report_runless_asset_event(
            dg.AssetMaterialization(asset_key=discovery_raw.raw_exchange_sweep.key, partition=key)
        )


def test_every_query_is_walked_again_on_the_monthly_tick_and_not_between(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """D3, as the daemon evaluates it. The first evaluation after a deploy requests nothing, even
    though a tick has passed — `cron_tick_passed` is empty with no previous evaluation, which is
    what keeps a roll from re-walking the directory. The tick then requests every query once."""
    keys = ["AU.common", "US.arca"]
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, keys)
    _materialized(instance, keys)
    defs = _sweep_defs(tmp_path)

    first = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=datetime(2026, 10, 30, 12, tzinfo=UTC)
    )
    assert first.total_requested == 0, "a deploy does not re-walk the directory"
    quiet = dg.evaluate_automation_conditions(
        defs=defs,
        instance=instance,
        cursor=first.cursor,
        evaluation_time=datetime(2026, 10, 31, 12, tzinfo=UTC),
    )
    assert quiet.total_requested == 0, "no tick, no walk"
    tick = dg.evaluate_automation_conditions(
        defs=defs,
        instance=instance,
        cursor=quiet.cursor,
        evaluation_time=datetime(2026, 11, 1, 4, tzinfo=UTC),
    )
    assert tick.total_requested == 2, "the first of the month walks every query"
    after = dg.evaluate_automation_conditions(
        defs=defs,
        instance=instance,
        cursor=tick.cursor,
        evaluation_time=datetime(2026, 11, 1, 4, 30, tzinfo=UTC),
    )
    assert after.total_requested == 0, "once"


def test_a_query_added_later_is_walked_and_one_present_at_the_first_evaluation_is_not(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """`on_missing()` is NEWLY missing: a key that becomes missing between two evaluations is
    walked; one already missing at the condition's first evaluation counts as handled. So at a
    rollout the keys must be added after the daemon has evaluated the new condition once."""
    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common"])
    defs = _sweep_defs(tmp_path)

    first = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, evaluation_time=datetime(2026, 10, 5, 12, tzinfo=UTC)
    )
    assert first.total_requested == 0, "present at the first evaluation: treated as handled"

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["US.reit"])
    second = dg.evaluate_automation_conditions(
        defs=defs,
        instance=instance,
        cursor=first.cursor,
        evaluation_time=datetime(2026, 10, 5, 12, 1, tzinfo=UTC),
    )
    assert second.total_requested == 1
    assert {
        key
        for result in second.results
        for key in result.true_subset.expensively_compute_partition_keys()
    } == {"US.reit"}


def test_the_sweep_condition_is_one_the_daemon_can_read() -> None:
    """A condition with a node the daemon cannot deserialise is silently never evaluated — the
    symbology rungs sat unrequested for twelve days that way (umbrella CLAUDE.md, 2026-09-24)."""
    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    for asset in (discovery_raw.raw_exchange_sweep, discovery_core.venue_listing):
        [condition] = asset.automation_conditions_by_key.values()
        assert condition is not None and condition.is_serializable, asset.key


# --- the sensors ---------------------------------------------------------------------------------


def test_new_exchange_sweeps_seeds_one_key_per_query_and_deletes_nothing(
    instance: dg.DagsterInstance,
) -> None:
    """Every row of `market.directory_query` becomes a key. A key from before the re-key is NAMED
    and kept: a sensor that read a half-empty view during a deploy must not be able to erase the
    grid."""
    from muffin_ingest_dagster.defs.discovery.automation import new_exchange_sweeps

    instance.add_dynamic_partitions(discovery_partitions.SWEEP_PARTITIONS, ["AU.common", "US"])
    context = dg.build_sensor_context(instance=instance, resources={"postgres": FakePostgres()})
    result = new_exchange_sweeps(context)
    assert isinstance(result, dg.SensorResult)

    assert result.run_requests == []
    [request] = result.dynamic_partitions_requests or []
    assert isinstance(request, dg.AddDynamicPartitionsRequest)
    assert list(request.partition_keys) == [
        "GR.common",
        "LN.common",
        "US.arca",
        "US.common",
        "US.reit",
    ]


def test_unfinished_sweeps_requests_only_walks_that_end_in_a_cursor(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """One walk refused part-way, one finished: only the first is requested, tagged as a resume, and
    its run key carries the cursor so a walk that advances is asked again."""
    from muffin_ingest_dagster.defs.discovery.automation import unfinished_sweeps
    from muffin_ingest_dagster.defs.discovery.partitions import SWEEP_RESUME_TAG

    instance.add_dynamic_partitions(
        discovery_partitions.SWEEP_PARTITIONS, ["AU.common", "LN.common"]
    )
    pages = {
        None: _page(figi="BBG0000000P1", next_cursor="c1"),
        "c1": _page(figi="BBG0000000P2", next_cursor=None),
    }
    _sweep(tmp_path, instance, pages, key="AU.common", throttle_after=1)
    _sweep(tmp_path, instance, pages, key="LN.common")

    context = dg.build_sensor_context(
        instance=instance, resources={"raw_store": RawStore(base_path=str(tmp_path))}
    )
    result = unfinished_sweeps(context)
    assert isinstance(result, dg.SensorResult)

    [request] = result.run_requests or []
    assert request.partition_key == "AU.common"
    assert request.tags[SWEEP_RESUME_TAG] == "true"
    assert request.run_key is not None and request.run_key.startswith("AU.common:")


def test_the_directory_refuses_an_ambiguous_grid() -> None:
    """Two keys asking one question would walk it twice and route its pages to whichever came last;
    one key listed twice would share a cursor. Both are control-data errors, refused loudly."""
    from muffin_ingest_dagster.defs.discovery.queries import (
        DirectoryQueryAmbiguous,
        directory_queries,
    )

    twice = [*DIRECTORY_QUERY, ("US.common", "US", "US", "Common Stock", False)]
    same_question = [*DIRECTORY_QUERY, ("US.again", "US", "US", "REIT", False)]
    for rows in (twice, same_question):
        with pytest.raises(DirectoryQueryAmbiguous):
            directory_queries(FakeConn(directory_query=rows))
    assert set(directory_queries(FakeConn(directory_query=DIRECTORY_QUERY))) == {
        row[0] for row in DIRECTORY_QUERY
    }


def _sweep_messages(instance: dg.DagsterInstance, result: Any) -> list[str]:
    return [
        record.user_message
        for record in instance.all_logs(result.run_id)
        if record.user_message and record.user_message.startswith("query ")
    ]


def test_a_finished_walk_does_not_report_a_cursor_to_resume_from(
    tmp_path: Path,
    instance: dg.DagsterInstance,
) -> None:
    """THE LOG MUST NOT CONTRADICT THE CHECK, and for one live run it did: a walk that reached its
    last page logged `resumes at 'QW9Fc1Fr…'` beside a check that correctly PASSED.

    The two cases must disagree here or the fixture proves nothing: a finished walk and a
    refused one are materialised the same way and only the message separates them.
    """
    instance.add_dynamic_partitions(
        discovery_partitions.SWEEP_PARTITIONS, ["AU.common", "LN.common"]
    )

    finished: dict[str | None, bytes] = {
        None: _page(figi="BBG0000000P1", next_cursor="c1"),
        "c1": _page(figi="BBG0000000P2", next_cursor=None),
    }
    result, _ = _sweep(tmp_path, instance, finished, key="AU.common")
    assert result.success
    assert _stored(tmp_path, "AU.common")[-1]["cursor_at"] is None, "the walk did reach its end"
    said = _sweep_messages(instance, result)
    assert said == ["query AU.common: 2 pages from page 0, reached its last page"], said

    # THE OTHER HALF, so "never says resumes at" cannot pass by saying nothing.
    result, _ = _sweep(tmp_path, instance, finished, key="LN.common", throttle_after=1)
    assert result.success
    said = _sweep_messages(instance, result)
    assert said == ["query LN.common: 1 pages from page 0, resumes at 'c1'"], said


def test_an_issuer_the_edge_minted_keeps_its_id(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """THE LANE'S FIRST PRODUCTION RUN, 2026-09-26: `issuer_lei_key` on Oaktree's LEI. The edge
    minted random ids for the 9,022 issuers it wrote, so an id derived from a held LEI is a second
    row for it, and the upsert keyed on `issuer_id` inserts it into a table where the LEI is unique.

    Here Adobe's LEI (XLK's first holding) is already held under an edge-style id. The issuer write
    must carry that id and never the derived one, and the new Adobe security must point at it."""
    import uuid

    from muffin_ingest.facets import nport

    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    adobe = "FU4LY2G4933NH2E1CP29"
    held = str(uuid.uuid4())
    FakeCursor.writes.clear()
    FakePostgres.known_issuers = {adobe: held}
    try:
        instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [XLK_KEY])
        result = materialise(
            tmp_path,
            [discovery_raw.raw_nport_filing, discovery_core.discovered_security],
            XLK_KEY,
            instance=instance,
            provider=_serve,
        )
    finally:
        FakePostgres.known_issuers = {}
    assert result.success

    issuer_params = [p for sql, ps in FakeCursor.writes if "into market.issuer " in sql for p in ps]
    assert held in issuer_params, "the held issuer was not written under its own id"
    assert nport._stable_issuer_id(adobe) not in issuer_params, (
        "a derived id was offered for an LEI already held: the row that fails issuer_lei_key"
    )
    security_params = [
        p for sql, ps in FakeCursor.writes if "into market.security " in sql for p in ps
    ]
    assert held in security_params, "the new Adobe security does not point at the held issuer"


def test_debt_terms_for_bonds_already_held_reach_the_rpc(
    tmp_path: Path, instance: dg.DagsterInstance
) -> None:
    """PRODUCTION'S SHAPE, which the test above does not reach: every bond ALREADY HELD, so every
    debt-term row carries an id READ from the database. The lane's third run (2026-09-26, AGG) died
    in `set_debt_terms` with `Object of type UUID is not JSON serializable`: psycopg returns a
    `uuid` column as `uuid.UUID`, the ids this lane mints are strings, and only the read ones broke
    `json.dumps`. The fake now returns what psycopg returns."""
    import uuid

    from muffin_ingest_dagster.defs.discovery import core as discovery_core
    from muffin_ingest_dagster.defs.discovery import raw as discovery_raw

    held = {"isin:US91282CAB12": str(uuid.uuid4()), "isin:US91282CCD34": str(uuid.uuid4())}
    FakeCursor.writes.clear()
    FakePostgres.known = held
    try:
        instance.add_dynamic_partitions(discovery_partitions.NPORT_PARTITIONS, [BOND_FUND_KEY])
        result = materialise(
            tmp_path,
            [discovery_raw.raw_nport_filing, discovery_core.discovered_security],
            BOND_FUND_KEY,
            instance=instance,
            provider=_serve_bond_fund,
        )
    finally:
        FakePostgres.known = {}
    assert result.success
    calls = [params for sql, params in FakeCursor.writes if "set_debt_terms" in sql]
    assert len(calls) == 1, calls
    assert {r["security_id"] for r in json.loads(calls[0][0])} == set(held.values())
