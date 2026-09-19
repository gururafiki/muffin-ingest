"""Parsing the two whole-file registries. No network here — stage 2 reads bytes off disk.

That separation is the point of the split: NSE changed its header once and the parse below THROWS
on it deliberately, so the fix must cost a re-parse of a file already on disk and never a refetch
of a provider that rate-limits by User-Agent.
"""

from __future__ import annotations

import csv
import io
import json
import re
from typing import Any

#: An ISIN is twelve characters. Anything else is a truncated or column-shifted row, and a bad key
#: here maps one company's filings onto another — so it is dropped rather than stored.
ISIN = re.compile(r"^[A-Z0-9]{12}$")


class RegistryUnreadable(RuntimeError):
    """The document is not the shape this parser knows.

    DELIBERATELY FATAL. A silently-empty parse looks exactly like a provider that has stopped
    publishing, and the two demand opposite responses.
    """


def cik_map(body: bytes) -> list[dict[str, Any]]:
    """SEC's `company_tickers.json` → `[{ticker, cik}]`.

    THE SHAPE IS AN OBJECT KEYED BY ROW INDEX, not an array: `{"0": {"cik_str": 320193, "ticker":
    "AAPL", "title": "Apple Inc."}, "1": {...}}`. Reading it as a list yields nothing, with no
    error.

    `cik_str` is a NUMBER despite its name, and is not zero-padded — SEC's own submissions API
    wants it padded to ten digits, which is a formatting concern for whoever asks, not a fact
    about the filer. Stored as the integer `market.security.cik` already is.
    """
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RegistryUnreadable(f"company_tickers.json is not JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RegistryUnreadable(
            f"company_tickers.json is a {type(parsed).__name__}, expected an object keyed by row "
            f"index — read as a list it yields nothing and reports no error"
        )

    out: list[dict[str, Any]] = []
    for row in parsed.values():
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        raw_cik = row.get("cik_str")
        try:
            cik = int(raw_cik)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if ticker:
            out.append({"ticker": ticker, "cik": cik})

    if not out:
        raise RegistryUnreadable(
            "company_tickers.json parsed to zero filers — the file is ~10,400 rows, so an empty "
            "result is a shape change rather than SEC delisting every registrant"
        )
    return out


def nse_equities(body: bytes) -> list[dict[str, Any]]:
    """NSE's `EQUITY_L.csv` → `[{symbol, isin}]`.

    MATCHED ON A TRIMMED HEADER NAME, NEVER BY POSITION. NSE's header is

        SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, ... , ISIN NUMBER, ...

    — note the LEADING SPACE on every column after the first. A column inserted upstream shifts
    every positional read, and the failure is silent: each company gets another company's symbol,
    which then maps its filings onto the wrong security.

    THE ISIN IS THE JOIN KEY AND THAT WAS MEASURED. India was first shipped joining on
    `market.listing.symbol` because RELIANCE, HDFCBANK and INFY all work — and only **239 of 645**
    Indian equities carry a symbol NSE recognises; the rest hold a vendor abbreviation (`SUEL` for
    SUZLON, `HUVR` for HINDUNILVR). The published list holds 2,570 rows with 2,570 DISTINCT ISINs,
    no blanks and none claimed twice, which is what makes the ISIN safe here rather than assumed.
    Coverage 37% -> 97%.
    """
    text = body.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    rows = [r for r in rows if any(cell.strip() for cell in r)]
    # A file with no header at all is unreadable; a header with no rows beneath it is an empty
    # LIST, which falls through to the check at the bottom so it is reported as the provider
    # event it is rather than as a parse failure.
    if not rows:
        raise RegistryUnreadable("nse equity list is empty; the published file is ~2,570 rows")

    header = [cell.strip().upper() for cell in rows[0]]
    try:
        i_symbol = header.index("SYMBOL")
        i_isin = header.index("ISIN NUMBER")
    except ValueError as exc:
        raise RegistryUnreadable(
            f"nse equity list header changed: {'|'.join(header)} — refusing rather than reading "
            f"by position, which would map every company onto another company's symbol"
        ) from exc

    out: list[dict[str, Any]] = []
    for row in rows[1:]:
        if len(row) <= max(i_symbol, i_isin):
            continue
        symbol = row[i_symbol].strip()
        isin = row[i_isin].strip().upper()
        if symbol and ISIN.match(isin):
            out.append({"symbol": symbol, "isin": isin})

    if not out:
        raise RegistryUnreadable(
            "nse equity list parsed to zero equities — an empty list is a provider event, not a "
            "statement that India has no listed companies"
        )
    return out
