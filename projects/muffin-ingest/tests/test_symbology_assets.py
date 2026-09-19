"""The identity ladder, driven end to end over the captured provider bytes.

The rung assets store OpenFIGI's and Yahoo's real answers; `security_symbology` resolves one
security's materialised rung files onto the identity tables. The positional guarantee is exercised
here too: the mapping fixture is a three-entry body, and the asset's `position` column is what
keeps "which entry answers THIS security" honest.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import dagster as dg
from muffin_ingest.providers import openfigi, yahoo_search
from muffin_ingest.providers.documents import Document

from muffin_ingest_dagster.defs.prices.partitions import SECURITY_PARTITION
from muffin_ingest_dagster.defs.symbology import core as symbology_core
from muffin_ingest_dagster.defs.symbology import raw as symbology_raw
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES

FIX = FIXTURES

SID = "11111111-1111-1111-1111-111111111111"
MAPPING_BODY = (FIX / "openfigi_mapping.json").read_bytes()
YAHOO_BODY = (FIX / "yahoo_search_aapl.json").read_bytes()


class FakeCursor:
    writes: ClassVar[list[tuple[str, tuple[Any, ...]]]] = []

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        if text.startswith("select"):
            if "from market.exchange" in text:
                self.rows = [("US", "US", "")]
            elif "i.kind_code = 'isin'" in text or "kind_code = 'isin'" in text:
                self.rows = self._rows
            else:
                self.rows = []
        else:
            FakeCursor.writes.append((text, tuple(params)))
            self.rows = []

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class FakePostgres(Postgres):
    attributes: ClassVar[list[tuple[Any, ...]]] = [(SID, "US0378331005", "US")]

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield _Conn(self.attributes)


class _Conn:
    def __init__(self, attributes: list[tuple[Any, ...]]) -> None:
        self._attributes = attributes

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._attributes)

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
        instance.add_dynamic_partitions(SECURITY_PARTITION, [SID])
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
