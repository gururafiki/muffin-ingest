"""The news lane, driven end to end over Yahoo's own RSS bytes and a recording fake database.

The bodies are the captures in the library's `tests/fixtures/news` (2026-11-21).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
import pytest
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.providers import yahoo_rss
from muffin_ingest.providers.documents import Document, DocumentRefused

from muffin_ingest_dagster.defs.news import checks as news_checks
from muffin_ingest_dagster.defs.news import core as news_core
from muffin_ingest_dagster.defs.news import raw as news_raw
from muffin_ingest_dagster.defs.prices import partitions as prices_partitions
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from muffin_ingest_dagster.lib.resources import Postgres
from tests import FIXTURES, loaded_defs

AAPL = ("11111111-1111-1111-1111-111111111111", "AAPL", 5.0)
SAP = ("22222222-2222-2222-2222-222222222222", "SAP.DE", 4.0)
NOBODY = ("33333333-3333-3333-3333-333333333333", "ZZQQXX", 3.0)
TOKYO = ("44444444-4444-4444-4444-444444444444", "4290.T", 2.0)


def rss(symbol: str) -> bytes:
    return (FIXTURES / "news" / f"yahoo_rss_{symbol}.xml").read_bytes()


class Feed:
    """The feed answering from captures or refusing, recording every symbol it was asked about."""

    def __init__(self, answers: dict[str, bytes | Exception]) -> None:
        self.answers = answers
        self.calls: list[str] = []

    def fetch(self, symbol: str, **_: Any) -> Document:
        self.calls.append(symbol)
        answer = self.answers[symbol]
        if isinstance(answer, Exception):
            raise answer
        return Document(
            url=yahoo_rss.url_for(symbol),
            body=answer,
            content_type="application/rss+xml",
            fetched_at=datetime.now(UTC),
        )


def refused(symbol: str, status: int) -> DocumentRefused:
    """The refusal `documents._get` raises: the URL first, the status last."""
    return DocumentRefused(f"{yahoo_rss.url_for(symbol)}: HTTP {status}")


@contextmanager
def feed(fake: Feed) -> Iterator[Feed]:
    saved = yahoo_rss.fetch
    yahoo_rss.fetch = fake.fetch
    try:
        yield fake
    finally:
        yahoo_rss.fetch = saved


# --- the database --------------------------------------------------------------------------------

UNIVERSE: list[tuple[str, str, float]] = []
OFFERED: dict[str, list[dict[str, Any]]] = {}
CALLS: list[str] = []
COMMITS: list[int] = []
NEWEST: list[tuple[Any, int]] = []


def _columns(text: str) -> list[str]:
    return [c.strip() for c in text.split("(", 1)[1].split(")", 1)[0].split(",")]


class Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        text = " ".join(sql.split())
        CALLS.append(text)
        self.rows, self.rowcount = [], 0
        if text.startswith("insert into"):
            table = text.split()[2]
            columns = _columns(text)
            values = list(params)
            rows = [
                dict(zip(columns, values[i : i + len(columns)], strict=True))
                for i in range(0, len(values), len(columns))
            ]
            OFFERED.setdefault(table, []).extend(rows)
            self.rowcount = len(rows)
        elif text.startswith("select market.prune_news"):
            self.rows = [(7,)]
        elif text.startswith("select max(a.published_at)"):
            self.rows = list(NEWEST) or [(None, 0)]
        else:
            self.rows = list(UNIVERSE)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class Conn:
    def cursor(self) -> Cursor:
        return Cursor()

    def commit(self) -> None:
        COMMITS.append(len(CALLS))


class FakePostgres(Postgres):
    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield Conn()


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(news_raw, "SECONDS_BETWEEN_REQUESTS", 0.0)
    for held in (UNIVERSE, CALLS, COMMITS, NEWEST):
        held.clear()
    OFFERED.clear()
    yield


def visit(
    tmp_path: Path,
    subjects: list[tuple[str, str, float]],
    *,
    universe: list[tuple[str, str, float]] | None = None,
    stage_two: bool = True,
) -> dg.ExecuteInProcessResult:
    UNIVERSE[:] = subjects if universe is None else universe
    keys = [sid for sid, _, _ in subjects]
    chosen: list[Any] = [news_raw.raw_yahoo_news]
    if stage_two:
        chosen.append(news_core.company_news)
    with dg.instance_for_test() as instance:
        instance.add_dynamic_partitions(prices_partitions.SECURITY_PARTITION, keys)
        single = len(keys) == 1
        return dg.materialize(
            chosen,
            instance=instance,
            partition_key=keys[0] if single else None,
            tags=None
            if single
            else {
                ASSET_PARTITION_RANGE_START_TAG: keys[0],
                ASSET_PARTITION_RANGE_END_TAG: keys[-1],
            },
            resources={
                "postgres": FakePostgres(),
                "parquet_io": ParquetIOManager(str(tmp_path)),
                "raw_store": RawStore(base_path=str(tmp_path)),
            },
        )


def meta(result: dg.ExecuteInProcessResult, key: str) -> dict[str, Any]:
    for event in result.get_asset_materialization_events():
        materialization = event.step_materialization_data.materialization
        if materialization.asset_key == dg.AssetKey(key):
            return {k: v.value for k, v in materialization.metadata.items()}
    raise AssertionError(f"{key} did not materialize")


def outcomes_sum(m: dict[str, Any]) -> int:
    return sum(int(m[o]) for o in news_raw.OUTCOMES) + int(m["not_askable"]) + int(m["limited_out"])


def stored(tmp_path: Path, sid: str) -> list[tuple[Any, ...]]:
    path = tmp_path / "raw_yahoo_news" / f"{sid}.parquet"
    return duckdb.sql(
        f"select asked_symbol, provider, octet_length(body) from read_parquet('{path}') "
        f"where fetched_at is not null order by fetched_at"
    ).fetchall()


# --- the run -------------------------------------------------------------------------------------


def test_each_company_is_asked_once_and_every_company_is_counted(tmp_path: Path) -> None:
    fake = Feed({"AAPL": rss("AAPL"), "SAP.DE": rss("SAP.DE"), "ZZQQXX": rss("ZZQQXX")})
    with feed(fake):
        result = visit(tmp_path, [AAPL, SAP, NOBODY])
    assert result.success
    assert fake.calls == ["AAPL", "SAP.DE", "ZZQQXX"]

    m = meta(result, "raw_yahoo_news")
    assert (m["answered"], m["unknown"], m["unasked"]) == (2, 1, 0)
    assert outcomes_sum(m) == m["requested"] == 3

    # THE WHOLE BODY IS FILED, with who it was asked for.
    assert stored(tmp_path, AAPL[0]) == [("AAPL", "yahoo-rss", len(rss("AAPL")))]


def test_stage_two_writes_articles_once_and_links_per_company_then_prunes(tmp_path: Path) -> None:
    fake = Feed({"AAPL": rss("AAPL"), "SAP.DE": rss("AAPL")})
    with feed(fake):
        result = visit(tmp_path, [AAPL, SAP])
    assert result.success

    articles = OFFERED["market.news_article"]
    links = OFFERED["market.news_security"]
    assert len(articles) == 18 and len({a["url"] for a in articles}) == 18
    assert len(links) == 36
    assert {link["source_code"] for link in links} == {"yahoo-rss"}
    assert "source" not in articles[0], "the feed states no publisher, so `source` is never written"

    # ONE TRANSACTION, PRUNE LAST: the articles, their links, the prune, then the commit.
    writes = [c for c in CALLS if c.startswith(("insert into", "select market.prune_news"))]
    assert [w.split()[2] if w.startswith("insert") else "prune" for w in writes] == [
        "market.news_article",
        "market.news_security",
        "prune",
    ]
    link_insert = next(c for c in CALLS if c.startswith("insert into market.news_security"))
    assert "do nothing" in link_insert, "a link is a fact once written; it is never updated"
    # AN ARTICLE'S PUBLISHER IS NEVER OVERWRITTEN: the feed states none, and the edge's are the only
    # ones there are. The update sets exactly the three fields the feed restates.
    article_insert = next(c for c in CALLS if c.startswith("insert into market.news_article"))
    assigned = article_insert.split("do update set ", 1)[1].split(" where (", 1)[0]
    assert {a.split(" = ", 1)[0].strip('"') for a in assigned.split(", ")} == {
        "published_at",
        "title",
        "summary",
    }
    assert COMMITS and COMMITS[-1] == len(CALLS)

    m = meta(result, "company_news")
    assert (m["articles"], m["links"], m["pruned"]) == (18, 36, 7)


def test_a_refusal_is_counted_and_the_run_goes_on(tmp_path: Path) -> None:
    # 4290.T CONTAINS 429. Its 404 is a refusal about one symbol, not Yahoo throttling the run.
    fake = Feed({"AAPL": rss("AAPL"), "4290.T": refused("4290.T", 404), "SAP.DE": rss("SAP.DE")})
    with feed(fake):
        result = visit(tmp_path, [AAPL, TOKYO, SAP], stage_two=False)
    m = meta(result, "raw_yahoo_news")
    assert fake.calls == ["AAPL", "4290.T", "SAP.DE"]
    assert (m["answered"], m["refused"], m["throttled"], m["unasked"]) == (2, 1, 0, 0)
    assert outcomes_sum(m) == 3


def test_a_429_stops_the_run_and_the_rest_is_unasked(tmp_path: Path) -> None:
    fake = Feed({"AAPL": rss("AAPL"), "SAP.DE": refused("SAP.DE", 429), "ZZQQXX": rss("ZZQQXX")})
    with feed(fake):
        result = visit(tmp_path, [AAPL, SAP, NOBODY], stage_two=False)
    m = meta(result, "raw_yahoo_news")
    assert fake.calls == ["AAPL", "SAP.DE"]
    assert (m["answered"], m["throttled"], m["unasked"]) == (1, 1, 2)
    assert outcomes_sum(m) == 3


def test_three_failures_before_anything_answers_stop_the_run(tmp_path: Path) -> None:
    lost = DocumentRefused("http://x: ConnectError: refused")
    subjects = [AAPL, SAP, NOBODY, TOKYO]
    fake = Feed({s: lost for _, s, _ in subjects})
    with feed(fake):
        result = visit(tmp_path, subjects, stage_two=False)
    m = meta(result, "raw_yahoo_news")
    assert fake.calls == ["AAPL", "SAP.DE", "ZZQQXX"]
    assert (m["transport"], m["unasked"]) == (3, 1)
    assert outcomes_sum(m) == 4


def test_a_company_with_no_symbol_is_counted_not_asked(tmp_path: Path) -> None:
    fake = Feed({"AAPL": rss("AAPL")})
    with feed(fake):
        result = visit(tmp_path, [AAPL, SAP], universe=[AAPL], stage_two=False)
    m = meta(result, "raw_yahoo_news")
    assert fake.calls == ["AAPL"]
    assert (m["requested"], m["not_askable"], m["answered"]) == (2, 1, 1)
    assert outcomes_sum(m) == 2


# --- what the file keeps -------------------------------------------------------------------------


def test_a_newer_answer_replaces_the_last_and_an_unreadable_body_does_not(tmp_path: Path) -> None:
    with feed(Feed({"AAPL": rss("AAPL")})):
        assert visit(tmp_path, [AAPL], stage_two=False).success
    with feed(Feed({"AAPL": rss("PTSB.IR")})):
        assert visit(tmp_path, [AAPL], stage_two=False).success
    assert [r[2] for r in stored(tmp_path, AAPL[0])] == [len(rss("PTSB.IR"))]

    # A GATEWAY PAGE IS FILED BESIDE THE LAST ANSWER, which it must not delete.
    with feed(Feed({"AAPL": b"<html>gateway</html>"})):
        result = visit(tmp_path, [AAPL], stage_two=False)
    assert meta(result, "raw_yahoo_news")["unreadable"] == 1
    assert [r[2] for r in stored(tmp_path, AAPL[0])] == [len(rss("PTSB.IR")), 20]


# --- the check -----------------------------------------------------------------------------------


def _check(passed_rows: list[tuple[Any, int]]) -> dg.AssetCheckResult:
    NEWEST[:] = passed_rows
    result = news_checks.news_is_current(dg.build_asset_check_context(), FakePostgres())
    assert isinstance(result, dg.AssetCheckResult)
    return result


def test_the_check_warns_when_no_recent_article_came_in() -> None:
    assert _check([(datetime(2026, 11, 21, tzinfo=UTC), 40)]).passed
    stale = _check([(datetime(2026, 10, 1, tzinfo=UTC), 0)])
    assert not stale.passed and stale.severity == dg.AssetCheckSeverity.WARN
    assert not _check([]).passed


# --- the location --------------------------------------------------------------------------------


def test_the_news_assets_run_in_the_company_job() -> None:
    """In the job since 2026-11-21, after their live run on five companies (Phase 6b decision 2).
    Checked on the RESOLVED job, the shape a scheduled run executes."""
    defs = loaded_defs()
    job = defs.resolve_job_def("yahoo_companies")
    keys = {key.to_user_string() for key in job.asset_layer.selected_asset_keys}
    assert {"raw_yahoo_summary", "security_profile", "raw_yahoo_news", "company_news"} <= keys
