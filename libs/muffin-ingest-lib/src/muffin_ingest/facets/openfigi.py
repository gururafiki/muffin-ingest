"""Parsing OpenFIGI responses. No network here — stage 2 reads bytes off disk.

TWO SHAPES, BOTH POSITIONAL AND BOTH EASY TO GET WRONG:

* `/v3/filter` returns ONE object per page: `{"data": [...], "next": "...", "total": N}`. `data`
  holds 100 listing rows with `figi`/`compositeFIGI`/`ticker`/`name`/`securityType`/
  `securityType2` — and NO ISIN, which is why the directory joins on FIGI.
* `/v3/mapping` returns ONE ENTRY PER JOB in the request order — entry j answers job j. Reordering
  there would attach one company's listing to another's, which is worse than resolving nothing.
  (The mapping parser lands with the symbology rungs.)

THE TWO TYPE FIELDS ARE NOT INTERCHANGEABLE. `securityType2` is the coarse bucket
(`Common Stock`, `Depositary Receipt`, `Mutual Fund`) and `securityType` the fine one. An ETF is
`securityType: 'ETP'` inside `securityType2: 'Mutual Fund'`; a receipt is `securityType: 'ADR'`
beside `securityType2: 'Depositary Receipt'`. Storing the fine one beside the coarse is what keeps
"an instrument is an ETF" answerable without re-labelling every existing row.
"""

from __future__ import annotations

import json
from typing import Any


class OpenFigiUnreadable(RuntimeError):
    """The body is not the OpenFIGI shape this parser knows. Deliberately fatal — a silently-empty
    parse looks exactly like a venue with nothing listed."""


def parse_filter(
    body: bytes, *, exch_code: str
) -> tuple[list[dict[str, Any]], str | None, int | None]:
    """One `/v3/filter` page → `(listing_rows, next_cursor, total)`.

    A row without a FIGI or a ticker is skipped: without the FIGI it cannot be keyed, and without
    the ticker it has nothing to address a security with. `next` is the cursor for the NEXT page,
    and `None` means the venue is exhausted — the caller must treat those two facts differently,
    which is why the cursor is a separate return rather than a field.
    """
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise OpenFigiUnreadable(f"openfigi /v3/filter body is not JSON: {exc}") from exc
    if isinstance(parsed, dict) and "error" in parsed and "data" not in parsed:
        # A REFUSAL WEARING A SUCCESS: OpenFIGI answers `{"error": "Invalid key '…'."}` with HTTP
        # 200. That is a shape problem in OUR request, never a venue that has nothing.
        raise OpenFigiUnreadable(f"openfigi /v3/filter replied with an error: {parsed['error']}")
    if not isinstance(parsed, dict) or "data" not in parsed:
        shape = sorted(parsed.keys()) if isinstance(parsed, dict) else type(parsed).__name__
        raise OpenFigiUnreadable(f"openfigi /v3/filter body has no 'data' key — shape is {shape}")

    rows: list[dict[str, Any]] = []
    for r in parsed.get("data") or []:
        if not isinstance(r, dict):
            continue
        figi = r.get("figi")
        ticker = r.get("ticker")
        if not figi or not ticker:
            continue
        rows.append(
            {
                "figi": str(figi),
                "composite_figi": str(r["compositeFIGI"]) if r.get("compositeFIGI") else None,
                "exch_code": exch_code,
                "ticker": str(ticker),
                "name": str(r["name"]) if r.get("name") else None,
                # THE COARSE BUCKET IS WHAT A STOCK SWEEP FILTERS ON; THE FINE ONE IS WHAT NAMES AN
                # ETF OR AN ADR. Kept side by side so the directory never has to choose.
                #
                # `figi_security_type` IS THE COLUMN'S NAME IN THE DATABASE AND THIS ONCE SAID
                # `security_type_detail`. Nothing could catch it: the writer takes its columns from
                # the row's keys, so the disagreement only exists at the moment of the INSERT, and
                # this lane had never run — the first real write died with `column
                # "security_type_detail" of relation "venue_listing" does not exist`. The database's
                # spelling wins: it is the live `exchange_listing`'s, it carries the comment
                # explaining why this is the only column naming a fund, and it has an index.
                "security_type": str(r["securityType2"]) if r.get("securityType2") else None,
                "figi_security_type": str(r["securityType"]) if r.get("securityType") else None,
            }
        )
    return rows, parsed.get("next"), parsed.get("total")
