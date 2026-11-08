"""Analyst actions from the stored quoteSummary documents, driven over Yahoo's own bytes.

AAPL's captured document (2026-10-10) carries 971 actions from 2012-09-12 to 2026-10-01: 22 with a
current target of 0.0 and 130 with a prior target of 0.0, 73 where the grade moved, and two days on
which one firm acted twice (Melius Research 2025-08-08, Morgan Stanley 2019-05-30). Toyota's
document has no `upgradeDowngradeHistory` at all, which is the non-US shape and says nothing.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from muffin_ingest.facets import analyst_actions

FIXTURES = Path(__file__).parents[1] / "fixtures"
KNOWN = frozenset({"USD", "GBX", "JPY"})
FETCHED = "2026-10-10T12:00:00+00:00"


def summary_body(symbol: str) -> bytes:
    return gzip.decompress((FIXTURES / f"yahoo_summary_{symbol}.json.gz").read_bytes())


def edited(symbol: str, edit: Any) -> bytes:
    parsed = json.loads(summary_body(symbol))
    edit(parsed["quoteSummary"]["result"][0])
    return json.dumps(parsed).encode()


def stored(symbol: str, *, body: bytes | None = None, fetched: str = FETCHED) -> dict[str, Any]:
    return {
        "security_id": f"sec-{symbol}",
        "url": f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
        "body": summary_body(symbol) if body is None else body,
        "fetched_at": fetched,
    }


def test_every_action_in_the_document_becomes_a_row() -> None:
    out = analyst_actions.normalise([stored("AAPL")], known_currencies=KNOWN)
    assert out.stats["actions"] == 971
    assert out.stats["duplicates"] == 0
    assert out.answered == ["sec-AAPL"]
    dates = sorted(r["published_date"] for r in out.rows)
    assert (dates[0], dates[-1]) == ("2012-09-12", "2026-10-01")


def test_one_firm_twice_in_a_day_is_two_actions() -> None:
    out = analyst_actions.normalise([stored("AAPL")], known_currencies=KNOWN)
    melius = [
        r
        for r in out.rows
        if r["analyst_company"] == "Melius Research" and r["published_date"] == "2025-08-08"
    ]
    assert len(melius) == 2
    assert melius[0]["published_at"] != melius[1]["published_at"]


def test_a_zero_target_is_no_target() -> None:
    out = analyst_actions.normalise([stored("AAPL")], known_currencies=KNOWN)
    assert out.stats["target_nulled"] == 22 + 130
    assert sum(r["target_to"] is None for r in out.rows) == 22
    assert sum(r["target_from"] is None for r in out.rows) == 130
    assert all(r["target_to"] is None or r["target_to"] > 0 for r in out.rows)


def test_the_newest_action_reads_as_the_app_expects() -> None:
    out = analyst_actions.normalise([stored("AAPL")], known_currencies=KNOWN)
    newest = max(out.rows, key=lambda r: r["published_at"])
    assert newest == {
        "security_id": "sec-AAPL",
        "source_code": "yfinance",
        "published_at": "2026-10-01T15:55:51+00:00",
        "published_date": "2026-10-01",
        "analyst_company": "Morgan Stanley",
        "target_from": 360.0,
        "target_to": 355.0,
        "currency_code": "USD",
        "status": "Maintains",
        "rating_change": "Overweight",
    }


def test_every_action_code_has_a_word() -> None:
    out = analyst_actions.normalise([stored("AAPL")], known_currencies=KNOWN)
    assert out.stats["unknown_action"] == 0
    assert {r["status"] for r in out.rows} == set(analyst_actions.STATUS.values())


def test_a_grade_that_moved_reads_as_a_transition() -> None:
    out = analyst_actions.normalise([stored("AAPL")], known_currencies=KNOWN)
    transitions = [r for r in out.rows if r["rating_change"] and " → " in r["rating_change"]]
    assert len(transitions) == 73


def test_a_document_without_the_module_says_nothing_and_is_not_an_answer() -> None:
    out = analyst_actions.normalise([stored("7203.T")], known_currencies=KNOWN)
    assert out.rows == []
    assert out.answered == []
    assert out.stats["without_module"] == 1


def test_an_empty_history_is_an_answer_with_nothing_in_it() -> None:
    body = edited("AAPL", lambda r: r["upgradeDowngradeHistory"].update(history=[]))
    out = analyst_actions.normalise([stored("AAPL", body=body)], known_currencies=KNOWN)
    assert out.rows == []
    assert out.answered == ["sec-AAPL"]


def test_an_absent_symbol_is_not_an_answer() -> None:
    out = analyst_actions.normalise([stored("BDMS-F.BK")], known_currencies=KNOWN)
    assert out.answered == []
    assert out.stats["absent"] == 1


def test_only_the_newest_document_per_company_is_read() -> None:
    older = edited(
        "AAPL",
        lambda r: r["upgradeDowngradeHistory"]["history"].append(
            {
                "epochGradeDate": 1300000000,
                "firm": "Gone Capital",
                "toGrade": "Buy",
                "fromGrade": "",
                "action": "init",
                "priceTargetAction": "",
                "currentPriceTarget": 0.0,
                "priorPriceTarget": 0.0,
            }
        ),
    )
    out = analyst_actions.normalise(
        [stored("AAPL", body=older, fetched="2026-10-01T00:00:00+00:00"), stored("AAPL")],
        known_currencies=KNOWN,
    )
    assert out.stats["documents"] == 1
    assert not any(r["analyst_company"] == "Gone Capital" for r in out.rows)


def test_a_pence_target_is_labelled_in_pence_and_an_unknown_code_is_withheld() -> None:
    def to_pence(r: dict[str, Any]) -> None:
        r["price"]["currency"] = "GBp"

    pence = analyst_actions.normalise(
        [stored("AAPL", body=edited("AAPL", to_pence))], known_currencies=KNOWN
    )
    assert {r["currency_code"] for r in pence.rows} == {"GBX"}

    unknown = analyst_actions.normalise(
        [stored("AAPL", body=edited("AAPL", to_pence))], known_currencies=frozenset({"USD"})
    )
    assert {r["currency_code"] for r in unknown.rows} == {None}
    assert unknown.stats["currency_withheld"] > 0


def test_an_unknown_action_code_is_counted_not_guessed() -> None:
    def odd(r: dict[str, Any]) -> None:
        r["upgradeDowngradeHistory"]["history"][0]["action"] = "xfer"

    out = analyst_actions.normalise(
        [stored("AAPL", body=edited("AAPL", odd))], known_currencies=KNOWN
    )
    assert out.stats["unknown_action"] == 1
    assert sum(r["status"] is None for r in out.rows) == 1
