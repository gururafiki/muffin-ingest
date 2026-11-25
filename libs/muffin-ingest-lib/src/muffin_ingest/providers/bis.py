"""The BIS statistics API (stats.bis.org), keyless SDMX, for macro series no other provider serves.

WHY BIS. `jp-cpi` asked OECD's `economy/cpi` for Japan, and OECD's series ends at 2021-06
(measured 2026-11-25: Germany, Korea and the UK answer to 2026-09/10 in the same call). FRED's
monthly Japanese CPI series are empty after 2023, and its only current one is the World Bank's
annual figure. BIS's `WS_LONG_CPI` carries Japan monthly to 2026-09, and for Germany, Korea and the
US it equals OECD's year-on-year change to six decimals, in PERCENT where OECD states a FRACTION.

ONE REQUEST PER SERIES, `GET /api/v1/data/<flow>/<key>?startPeriod=YYYY-MM&format=csv`, ~0.12 s.

AN ABSENCE IS A 404 WITH SDMX ERROR CODE 100, "No data for data query". Measured for an unknown
country (`M.XX.771`), an unknown unit (`M.JP.999`) and a window past the newest month alike. It is
an answer, so `fetch` returns it as one; any other non-200 is a refusal and raises.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Final

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document, DocumentRefused

#: BIS's own host. `settings.provider_base` points it at http-cache when one is configured.
REAL_ORIGIN: Final = "https://stats.bis.org"

#: The name these requests are counted under, and the catalogue's `provider` for a BIS series.
PROVIDER: Final = "bis"

#: The catalogue's route for a BIS series is BIS's own path, with the dataflow in it. The flows this
#: module has a reading for; anything else is a catalogue row nobody has checked.
ROUTE_PREFIX: Final = "/api/v1/data/"
FLOWS: Final = frozenset({"WS_LONG_CPI"})

#: The SDMX error code BIS answers a query that matches no data with.
NO_DATA_CODE: Final = b'code="100"'


class UnknownSeries(ValueError):
    """The catalogue names a BIS route or parameter this module has no reading for."""


def series_key(route: str, params: dict[str, object]) -> tuple[str, str]:
    """The (dataflow, series key) a catalogue row names: `route` is BIS's path with the flow in it,
    and `params` holds exactly `key` (`M.JP.771`: monthly, Japan, year-on-year change in percent).
    """
    if not route.startswith(ROUTE_PREFIX):
        raise UnknownSeries(f"a BIS route starts with {ROUTE_PREFIX!r}, not {route!r}")
    flow = route.removeprefix(ROUTE_PREFIX)
    if flow not in FLOWS:
        raise UnknownSeries(f"no reading for BIS dataflow {flow!r} (known: {sorted(FLOWS)})")
    extra = sorted(set(params) - {"key"})
    key = str(params.get("key") or "")
    if extra or not key:
        raise UnknownSeries(
            f"a BIS series takes exactly a `key`; the catalogue names {sorted(params)}"
        )
    return flow, key


def url_for(flow: str, key: str, start: date) -> str:
    # The location spelled out, so `test_every_provider_names_the_location_that_serves_its_origin`
    # can read this call site.
    base = settings.provider_base("bis", REAL_ORIGIN)
    return f"{base}{ROUTE_PREFIX}{flow}/{key}?startPeriod={start:%Y-%m}&format=csv"


def fetch(flow: str, key: str, start: date, *, timeout_s: float = 30.0) -> Document | None:
    """One series from `start`'s month, the CSV body whole; `None` when BIS states it has no data.

    A transport failure or any other non-200 raises `DocumentRefused`: never an absence.
    """
    import httpx

    url = url_for(flow, key, start)
    headers = {"User-Agent": settings.user_agent(), "Accept": "text/csv"}
    refused: DocumentRefused | None = None
    absent = False
    with metrics.request(PROVIDER) as outcome:
        try:
            response = httpx.get(url, headers=headers, timeout=timeout_s, follow_redirects=True)
        except httpx.HTTPError as exc:
            raise DocumentRefused(f"{url}: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 404 and NO_DATA_CODE in response.content:
            outcome["outcome"] = "absent"
            absent = True
        elif response.status_code != 200:
            outcome["outcome"] = "throttled" if response.status_code == 429 else "refused"
            refused = DocumentRefused(f"{url}: HTTP {response.status_code}")
    # RAISED OUTSIDE THE METRICS BLOCK, which records any exception escaping it as `transport`.
    if refused is not None:
        raise refused
    if absent:
        return None
    return Document(
        url=url,
        body=response.content,
        content_type=response.headers.get("content-type", "application/octet-stream"),
        fetched_at=datetime.now(UTC),
    )
