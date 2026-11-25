"""BIS's statistics API at its network boundary, and stage 2's reading of its CSV, over answers
captured on 2026-11-25 (`fixtures/capture.md`)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from muffin_ingest import metrics
from muffin_ingest.facets import macro
from muffin_ingest.providers import bis
from muffin_ingest.providers.documents import DocumentRefused

MACRO = Path(__file__).resolve().parents[1] / "fixtures" / "macro"
JAPAN = (MACRO / "bis_ws_long_cpi_m_jp_771.csv").read_bytes()
NO_DATA = (MACRO / "bis_no_data.xml").read_bytes()
ROUTE = "/api/v1/data/WS_LONG_CPI"


@pytest.fixture
def through_cache(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Routes every request through http-cache, records what was asked and how it was counted."""
    monkeypatch.delenv("BIS_BASE_URL", raising=False)
    monkeypatch.setenv("HTTP_CACHE_URL", "http://http-cache:8080")
    monkeypatch.setenv("MUFFIN_USER_AGENT", "muffin-test/1.0 (ops@example.com)")
    counted: list[str] = []

    @contextmanager
    def recording(provider: str) -> Iterator[dict[str, str]]:
        state = {"outcome": "answered"}
        yield state
        counted.append(f"{provider}:{state['outcome']}")

    monkeypatch.setattr(metrics, "request", recording)
    yield counted


def answering(monkeypatch: pytest.MonkeyPatch, status: int, body: bytes) -> list[str]:
    asked: list[str] = []

    def fake_get(url: str, **kwargs: Any) -> httpx.Response:
        asked.append(url)
        return httpx.Response(status, content=body, headers={"content-type": "text/csv"})

    monkeypatch.setattr(httpx, "get", fake_get)
    return asked


# --- the request ----------------------------------------------------------------------------------


def test_a_series_is_asked_through_the_cache_from_the_month_of_its_window(
    monkeypatch: pytest.MonkeyPatch, through_cache: list[str]
) -> None:
    asked = answering(monkeypatch, 200, JAPAN)
    document = bis.fetch("WS_LONG_CPI", "M.JP.771", date(2025, 10, 21))
    assert asked == [
        "http://http-cache:8080/bis/api/v1/data/WS_LONG_CPI/M.JP.771?startPeriod=2025-10&format=csv"
    ]
    assert document is not None and document.body == JAPAN
    assert through_cache == ["bis:answered"]


def test_bis_s_no_data_answer_is_an_absence_not_a_refusal(
    monkeypatch: pytest.MonkeyPatch, through_cache: list[str]
) -> None:
    """Measured: a 404 carrying SDMX error code 100, for an unknown country, an unknown unit and a
    window past the newest month alike."""
    answering(monkeypatch, 404, NO_DATA)
    assert bis.fetch("WS_LONG_CPI", "M.JP.771", date(2027, 1, 1)) is None
    assert through_cache == ["bis:absent"]


@pytest.mark.parametrize(
    ("status", "body", "counted"),
    [
        (404, b"<html>Not Found</html>", "bis:refused"),  # a 404 that does not say "no data"
        (429, b"slow down", "bis:throttled"),
        (503, b"", "bis:refused"),
    ],
)
def test_any_other_answer_is_a_refusal_and_never_an_absence(
    monkeypatch: pytest.MonkeyPatch,
    through_cache: list[str],
    status: int,
    body: bytes,
    counted: str,
) -> None:
    answering(monkeypatch, status, body)
    with pytest.raises(DocumentRefused, match=f"HTTP {status}"):
        bis.fetch("WS_LONG_CPI", "M.JP.771", date(2025, 10, 1))
    assert through_cache == [counted]


def test_a_transport_failure_raises_and_is_counted_as_one(
    monkeypatch: pytest.MonkeyPatch, through_cache: list[str]
) -> None:
    def fake_get(url: str, **kwargs: Any) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", fake_get)
    with pytest.raises(DocumentRefused, match="ConnectError"):
        bis.fetch("WS_LONG_CPI", "M.JP.771", date(2025, 10, 1))


# --- the catalogue row ----------------------------------------------------------------------------


def test_a_catalogue_row_names_its_dataflow_in_the_route_and_its_series_in_key() -> None:
    assert bis.series_key(ROUTE, {"key": "M.JP.771"}) == ("WS_LONG_CPI", "M.JP.771")


@pytest.mark.parametrize(
    ("route", "params"),
    [
        ("/api/v1/economy/cpi", {"key": "M.JP.771"}),  # not a BIS route
        ("/api/v1/data/WS_CBPOL", {"key": "M.JP"}),  # a dataflow nobody has checked
        (ROUTE, {}),  # no series
        (ROUTE, {"key": "M.JP.771", "startPeriod": "2020-01"}),  # a parameter we would not send
    ],
)
def test_a_row_this_module_cannot_read_is_refused_before_asking(
    route: str, params: dict[str, object]
) -> None:
    with pytest.raises(bis.UnknownSeries):
        bis.series_key(route, params)


# --- stage 2 --------------------------------------------------------------------------------------


def document_row(body: bytes, fetched_at: str = "2026-11-25T22:00:00+00:00") -> dict[str, Any]:
    """The row stage 1 stores for a BIS answer: the document whole, plus the context columns."""
    return {
        "url": "http://http-cache:8080/bis/api/v1/data/WS_LONG_CPI/M.JP.771?startPeriod=2025-10",
        "body": body,
        "content_type": "text/csv;charset=UTF-8",
        "sha256": "x",
        "fetched_at": fetched_at,
        "run_id": "r1",
        "indicator_code": "jp-cpi",
        "route": ROUTE,
        "provider": "bis",
        "asked_from": "2025-10-21",
    }


def test_the_answer_s_months_are_read_as_their_first_day_in_percent() -> None:
    found = macro.points([document_row(JAPAN)])
    assert len(found) == 12
    assert found[0].as_of == "2025-10-01" and found[0].value == pytest.approx(2.860061)
    assert found[-1].as_of == "2026-09-01" and found[-1].value == pytest.approx(2.097902)
    assert {p.dimension for p in found} == {""}


def test_a_newest_document_is_the_series_answer_and_its_window_is_what_was_asked() -> None:
    older = document_row(JAPAN.replace(b"2.097902", b"9.9"), "2026-11-24T22:00:00+00:00")
    parsed = macro.normalise([older, document_row(JAPAN)], ["jp-cpi"])
    [answered] = parsed.series
    assert answered.window == ("2025-10-21", "2026-11-25")
    assert answered.points[-1].value == pytest.approx(2.097902)


def test_a_missing_observation_is_dropped_and_a_multiplier_is_applied() -> None:
    body = (
        b"FREQ,REF_AREA,UNIT_MEASURE,UNIT_MULT,TIME_PERIOD,OBS_VALUE\n"
        b"M,JP,771,0,2026-08,\n"  # BIS leaves OBS_VALUE empty for a missing observation
        b"Q,JP,628,3,2026-Q3,1.5\n"  # a quarter, in thousands
        b"A,JP,771,0,2025,2.5\n"
    )
    assert macro.sdmx_csv_rows(body) == [
        {"date": "2026-07-01", "value": 1500.0},
        {"date": "2025-01-01", "value": 2.5},
    ]


def test_a_document_with_a_header_and_no_observation_has_no_point() -> None:
    header = JAPAN.split(b"\n", 1)[0] + b"\n"
    parsed = macro.normalise([document_row(header)], ["jp-cpi"])
    assert parsed.no_points == ["jp-cpi"] and parsed.series == []
