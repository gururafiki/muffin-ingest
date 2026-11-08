"""Analysts' rating and price-target actions, from the Yahoo quoteSummary documents already stored.

WHAT THIS REPLACES. finviz's analyst actions, asked per US ticker through openbb, until finviz moved
its quote page on 2026-11-04 (muffin-deployment#494). Every quoteSummary the company lane stores
carries `upgradeDowngradeHistory.history`, so this is a stage 2 over files on disk and costs no
provider request. Approved by the user on 2026-11-08: the umbrella's
docs/specs/2026-11-07-analyst-actions-from-yahoo.md, choices 1a, 2a and 3a.

MEASURED ON AAPL'S STORED DOCUMENT (the captured fixture): 971 actions from 2012-09-12 to
2026-10-01, each with the same eight fields: `epochGradeDate`, `firm`, `fromGrade`, `toGrade`,
`action`, `priceTargetAction`, `priorPriceTarget`, `currentPriceTarget`.

  * `action` is `main` 709, `reit` 171, `down` 38, `up` 35, `init` 18. The app shows a word, so
    each maps to one (`STATUS`); a code outside the five is kept off the row and counted.
  * `0.0` IS AN ABSENCE WEARING A VALUE: `currentPriceTarget` is 0.0 on 22 of 971 and
    `priorPriceTarget` on 130. Read literally, it would serve "lowers its target to 0".
  * The same firm on the same security twice in a day happens (2 of 971, at different seconds), and
    at the same second never, so a row is keyed on the action's timestamp.

Non-US listings mostly lack the module (`yahoo_summary`'s docstring), and a document without it says
nothing about analyst actions. Only a document that carries the module is that company's whole
answer, which is what lets the asset retract what the newest one no longer states.

TARGETS ARE IN THE QUOTE CURRENCY AS QUOTED, as for `security_estimate`: a London line's targets
are in pence, so the label is the quote's own code (`GBX`), and it is withheld where
`market.currency` does not hold it.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from muffin_ingest.facets import price_chart
from muffin_ingest.providers import yahoo_chart, yahoo_summary

#: The source these rows are written under, the provider that stated them.
SOURCE = "yfinance"

#: Yahoo's action codes, as the app reads them. `main` is Yahoo's "maintains", a word finviz never
#: used, which is why the vocabulary is written out rather than passed through.
STATUS = {
    "up": "Upgrade",
    "down": "Downgrade",
    "init": "Initiated",
    "reit": "Reiterated",
    "main": "Maintains",
}

MODULE = "upgradeDowngradeHistory"


@dataclass
class Normalised:
    #: Rows for `market.security_price_target`.
    rows: list[dict[str, Any]] = field(default_factory=list)
    #: Companies whose newest document carries the module: their whole answer, retractable.
    answered: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)


def _fetch_order(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("fetched_at") or ""), str(row.get("url") or ""))


def _target(value: Any) -> float | None:
    """A positive finite target, or None. Yahoo writes 0.0 where an action carries no target."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if number > 0 and number == number and number != float("inf") else None


def _text(value: Any) -> str | None:
    return value.strip() or None if isinstance(value, str) else None


def rating_change(from_grade: str | None, to_grade: str | None) -> str | None:
    """`Hold → Buy` where the grade moved, the bare grade where it did not, as finviz served it."""
    if from_grade and to_grade and from_grade != to_grade:
        return f"{from_grade} → {to_grade}"
    return to_grade or from_grade


def normalise(
    rows: Sequence[Mapping[str, Any]], *, known_currencies: Collection[str]
) -> Normalised:
    """Stored summary documents to analyst actions, from the newest document per company."""
    stats = {
        "documents": 0,
        "legacy_rows": 0,
        "unreadable": 0,
        "absent": 0,
        "without_module": 0,
        "companies": 0,
        "actions": 0,
        "no_firm_or_date": 0,
        "unknown_action": 0,
        "target_nulled": 0,
        "currency_withheld": 0,
        "duplicates": 0,
    }
    newest: dict[str, Mapping[str, Any]] = {}
    for row in sorted(rows, key=_fetch_order):
        sid = row.get("security_id")
        if sid is None:
            continue
        if row.get("body") is None:
            stats["legacy_rows"] += 1
            continue
        newest[str(sid)] = row

    out = Normalised(stats=stats)
    for sid, row in sorted(newest.items()):
        stats["documents"] += 1
        try:
            summary = yahoo_summary.parse(bytes(row["body"]))
        except yahoo_chart.YahooRefused:
            stats["unreadable"] += 1
            continue
        if summary.error is not None:
            stats["absent"] += 1
            continue
        if MODULE not in summary.modules:
            stats["without_module"] += 1
            continue
        history = summary.module(MODULE).get("history")
        if not isinstance(history, list):
            stats["without_module"] += 1
            continue

        price = summary.module("price")
        detail = summary.module("summaryDetail")
        spelling = price.get("currency") or detail.get("currency")
        currency = price_chart.quote_currency(spelling, known_currencies)

        out.answered.append(sid)
        stats["companies"] += 1
        seen: set[tuple[str, str]] = set()
        for entry in history:
            if not isinstance(entry, dict):
                continue
            firm = _text(entry.get("firm"))
            epoch = entry.get("epochGradeDate")
            if firm is None or isinstance(epoch, bool) or not isinstance(epoch, int | float):
                stats["no_firm_or_date"] += 1
                continue
            at = datetime.fromtimestamp(int(epoch), UTC)
            key = (at.isoformat(), firm)
            if key in seen:
                stats["duplicates"] += 1
                continue
            seen.add(key)

            code = _text(entry.get("action"))
            status = STATUS.get(code.lower()) if code else None
            if code and status is None:
                stats["unknown_action"] += 1
            prior, current = entry.get("priorPriceTarget"), entry.get("currentPriceTarget")
            target_from, target_to = _target(prior), _target(current)
            stats["target_nulled"] += (prior is not None and target_from is None) + (
                current is not None and target_to is None
            )
            if currency is None and spelling and (target_from or target_to):
                stats["currency_withheld"] += 1

            out.rows.append(
                {
                    "security_id": sid,
                    "source_code": SOURCE,
                    "published_at": at.isoformat(),
                    "published_date": at.date().isoformat(),
                    "analyst_company": firm,
                    "target_from": target_from,
                    "target_to": target_to,
                    "currency_code": currency,
                    "status": status,
                    "rating_change": rating_change(
                        _text(entry.get("fromGrade")), _text(entry.get("toGrade"))
                    ),
                }
            )
            stats["actions"] += 1
    return out
