"""SEC's insider data sets, over 2026q1 trimmed to ten filings (captured 2026-10-27).

The nine tracked filings' own Form 4 XML (fetched the same day) is the ground truth for document
order: the data set's rows are in neither that order nor any other.
"""

from __future__ import annotations

import io
import re
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from muffin_ingest.facets import insider

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sec"
DATASET = (FIXTURES / "insider_2026q1.trimmed.zip").read_bytes()
AS_OF = datetime(2026, 10, 27, 21, tzinfo=UTC)

ALPHABET = 1652044
#: The nine filings' issuers. Alphabet is attributed to both of its share classes.
ISSUERS: dict[int, list[str]] = {
    ALPHABET: ["goog", "googl"],
    1687187: ["s-1687187"],
    1478320: ["s-1478320"],
    1527541: ["s-1527541"],
    1069899: ["s-1069899"],
    1971213: ["s-1971213"],
    1616707: ["s-1616707"],
    24090: ["s-24090"],
    1760965: ["s-1760965"],
}
#: Each fixture filing's first security.
SECURITY_OF = {
    "0001193125-26-135334": "goog",
    "0001140361-26-012433": "s-1687187",
    "0001780664-26-000018": "s-1478320",
    "0001016817-26-000004": "s-1971213",
    "0001877855-26-000002": "s-24090",
}
#: `market.insider_transaction_code`, spelled differently from SEC's form on purpose: the name
#: written must be the table's, not one the reader carries.
CODES = {c: f"code {c}" for c in "PSVADFIMCEHOXGLWZJKU"}


def read(
    body: bytes = DATASET,
    issuers: dict[int, list[str]] = ISSUERS,
    codes: dict[str, str] = CODES,
    quarter: tuple[int, int] | None = (2026, 1),
) -> insider.Trades:
    return insider.dataset_rows(body, issuers, codes, as_of=AS_OF, quarter=quarter)


def rows_of(trades: insider.Trades, accession: str, security: str) -> list[dict[str, object]]:
    found = [r for r in trades.rows if r["accession_number"] == accession]
    return sorted(
        (r for r in found if r["security_id"] == security),
        key=lambda r: (str(r["line_key"])[0], int(str(r["line_key"]).split(":")[1])),
    )


def xml_lines(accession: str, table: str) -> list[tuple[float, float | None]]:
    """(shares, shares owned after) per line, in the filing's own order, rounded as SEC rounds."""
    text = (FIXTURES / f"form4_{accession}.txt").read_text()
    match = re.search(r"<XML>(.*?)</XML>", text, re.S)
    assert match
    root = ET.fromstring(match.group(1).strip())
    tag = {"nd": "nonDerivativeTable/nonDerivativeTransaction"}.get(
        table, "derivativeTable/derivativeTransaction"
    )

    def value(el: ET.Element, path: str) -> float | None:
        found = el.find(path)
        return None if found is None or found.text is None else round(float(found.text), 2)

    out = []
    for line in root.findall(tag):
        shares = value(line, "transactionAmounts/transactionShares/value")
        assert shares is not None
        out.append(
            (shares, value(line, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"))
        )
    return out


def rebuilt(table: str, edit: Callable[[list[str]], list[str]]) -> bytes:
    """The fixture with one table's lines rewritten."""
    source = zipfile.ZipFile(io.BytesIO(DATASET))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for info in source.infolist():
            text = source.read(info).decode()
            if info.filename == table:
                text = "\n".join(edit(text.rstrip("\n").split("\n"))) + "\n"
            archive.writestr(info.filename, text)
    return out.getvalue()


@pytest.mark.parametrize(
    ("accession", "table"),
    [
        ("0001193125-26-135334", "nd"),
        ("0001140361-26-012433", "nd"),
        ("0001780664-26-000018", "nd"),
        ("0001780664-26-000018", "d"),
        ("0001016817-26-000004", "d"),
        ("0001877855-26-000002", "nd"),
    ],
)
def test_lines_are_numbered_in_the_filing_s_own_order(accession: str, table: str) -> None:
    """Every one of these lists its lines out of document order in the data set's file."""
    lines = [
        r
        for r in rows_of(read(), accession, SECURITY_OF[accession])
        if str(r["line_key"]).startswith(f"{table}:")
    ]
    assert [r["line_key"] for r in lines] == [f"{table}:{n}" for n in range(1, len(lines) + 1)]
    assert [(r["shares"], r["shares_owned_after"]) for r in lines] == xml_lines(accession, table)


def test_a_filing_is_attributed_to_every_share_class_of_its_issuer() -> None:
    """Alphabet's Form 4s name only GOOG; the trade is a fact about both share classes."""
    trades = read()
    goog = rows_of(trades, "0001193125-26-135334", "goog")
    googl = rows_of(trades, "0001193125-26-135334", "googl")
    assert len(goog) == len(googl) == 7
    assert [r["line_key"] for r in goog] == [r["line_key"] for r in googl]
    assert {r["trade_key"] for r in goog}.isdisjoint(r["trade_key"] for r in googl)
    assert trades.stats["issuer_fanned_out"] == 1


def test_the_owner_is_the_lowest_cik_not_the_first_in_the_file() -> None:
    """The data set lists this filing's four owners as 1368026, 1953511, 1353085, 1352851."""
    [first, *_] = rows_of(read(), "0001104659-26-038038", "s-1527541")
    assert first["owner_cik"] == 1352851
    assert read().stats["several_owners"] == 2


def test_a_line_reads_the_filing_s_values() -> None:
    [first, *_] = rows_of(read(), "0001193125-26-135334", "goog")
    assert first == {
        "trade_key": insider.trade_key("goog", "0001193125-26-135334", "nd:1"),
        "security_id": "goog",
        "accession_number": "0001193125-26-135334",
        "line_key": "nd:1",
        "filing_date": date(2026, 3, 31),
        "transaction_date": date(2026, 3, 27),
        "owner_name": first["owner_name"],
        "owner_title": first["owner_title"],
        "owner_cik": 1238734,
        "issuer_cik": ALPHABET,
        "form_type": "4",
        "transaction_code": "S",
        "transaction_type": "code S",
        "direction": "Disposition",
        "security_type": "Class C Capital Stock",
        "shares": 2677.0,
        "price": 273.91,
        "shares_owned_after": 58124.0,
        "is_derivative": False,
        "source_code": insider.SOURCE_DATASET,
        "as_of": AS_OF,
    }
    assert first["owner_name"]


def test_the_data_set_rounds_to_two_decimals_where_the_filing_does_not() -> None:
    """3,647.903 shares in the XML; the data set says 3,647.9."""
    [line] = [r for r in rows_of(read(), "0000897069-26-000475", "s-1760965")]
    assert line["shares"] == 3647.9


def test_a_line_with_no_code_is_withheld_and_counted_and_its_neighbour_written() -> None:
    trades = read()
    assert [r["line_key"] for r in rows_of(trades, "0000897069-26-000475", "s-1760965")] == ["nd:1"]
    assert trades.stats["code_unknown:<none>"] == 1


def test_a_code_the_table_lacks_is_withheld_and_counted() -> None:
    trades = read(codes={c: n for c, n in CODES.items() if c != "S"})
    assert not any(r["transaction_code"] == "S" for r in trades.rows)
    assert trades.stats["code_unknown:S"] > 0
    assert "0001193125-26-135334" in trades.accessions


def test_amendments_and_annual_forms_keep_their_form() -> None:
    forms = {r["accession_number"]: r["form_type"] for r in read().rows}
    assert forms["0001616707-26-000081"] == "4/A"
    assert forms["0001877855-26-000002"] == "5"
    assert read().stats["amendments"] == 1


def test_derivative_lines_are_marked() -> None:
    lines = rows_of(read(), "0001016817-26-000004", "s-1971213")
    assert {r["line_key"] for r in lines} == {f"d:{n}" for n in range(1, 9)}
    assert all(r["is_derivative"] for r in lines)


def test_an_untracked_issuer_s_filing_is_skipped_and_counted() -> None:
    trades = read()
    assert trades.stats["filings"] == 10
    assert trades.stats["filings_untracked"] == 1
    assert "0001193125-26-134840" not in trades.accessions
    assert len(trades.accessions) == 9
    assert trades.stats["issuers_tracked"] == 9


def test_the_retraction_covers_every_tracked_filing_even_one_with_nothing_written() -> None:
    """A filing whose every line was withheld still holds the Form 4 lane's copy, to retract."""
    trades = read(codes={})
    assert trades.rows == []
    assert len(trades.accessions) == 9


def test_a_filing_dated_outside_the_quarter_is_counted() -> None:
    assert read().stats["filed_outside_quarter"] == 0
    assert read(quarter=(2026, 2)).stats["filed_outside_quarter"] == 9


def test_a_2016_shaped_submission_table_reads_the_same() -> None:
    """SEC added AFF10B5ONE after 2016; the 2006 and 2016 data sets have thirteen columns."""

    def drop_last_column(lines: list[str]) -> list[str]:
        assert lines[0].endswith("\tAFF10B5ONE")
        return [line.rsplit("\t", 1)[0] for line in lines]

    old = read(rebuilt("SUBMISSION.tsv", drop_last_column))
    assert old.rows == read().rows


def test_a_filing_with_a_malformed_row_is_withheld_whole_and_kept_out_of_the_retraction() -> None:
    """Skipping only the bad line would renumber the filing's other lines."""
    first = 0

    def add_a_field(lines: list[str]) -> list[str]:
        nonlocal first
        first = next(i for i, line in enumerate(lines) if line.startswith("0001193125-26-135334"))
        return [*lines[:first], lines[first] + "\textra", *lines[first + 1 :]]

    trades = read(rebuilt("NONDERIV_TRANS.tsv", add_a_field))
    assert first > 0
    assert trades.stats["filings_malformed"] == 1
    assert "0001193125-26-135334" not in trades.accessions
    assert not rows_of(trades, "0001193125-26-135334", "goog")
    assert len(trades.rows) == len(read().rows) - 14


def test_an_unreadable_number_is_counted_not_written() -> None:
    def spoil(lines: list[str]) -> list[str]:
        header = lines[0].split("\t")
        price = header.index("TRANS_PRICEPERSHARE")
        cells = lines[1].split("\t")
        cells[price] = "nan"
        return [lines[0], "\t".join(cells), *lines[2:]]

    trades = read(rebuilt("NONDERIV_TRANS.tsv", spoil))
    assert trades.stats["number_unreadable"] == 1
    assert all(r["price"] is None or r["price"] == r["price"] for r in trades.rows)


@pytest.mark.parametrize(
    "body",
    [b'<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>', b""],
    ids=["s3-error", "empty"],
)
def test_a_body_that_is_not_a_zip_raises(body: bytes) -> None:
    with pytest.raises(insider.DatasetUnreadable):
        read(body)


def test_a_missing_table_or_column_raises() -> None:
    source = zipfile.ZipFile(io.BytesIO(DATASET))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for info in source.infolist():
            if info.filename != "DERIV_TRANS.tsv":
                archive.writestr(info.filename, source.read(info))
    with pytest.raises(insider.DatasetUnreadable, match="DERIV_TRANS"):
        read(out.getvalue())

    def rename(lines: list[str]) -> list[str]:
        return [lines[0].replace("TRANS_SHARES", "SHARES"), *lines[1:]]

    with pytest.raises(insider.DatasetUnreadable, match="TRANS_SHARES"):
        read(rebuilt("NONDERIV_TRANS.tsv", rename))


@pytest.mark.parametrize(
    ("quarter", "bounds"),
    [
        ((2026, 1), (date(2026, 1, 1), date(2026, 3, 31))),
        ((2024, 1), (date(2024, 1, 1), date(2024, 3, 31))),
        ((2026, 4), (date(2026, 10, 1), date(2026, 12, 31))),
    ],
)
def test_quarter_bounds(quarter: tuple[int, int], bounds: tuple[date, date]) -> None:
    assert insider.quarter_bounds(*quarter) == bounds


def test_a_trade_key_names_the_security_the_filing_and_the_line() -> None:
    key = insider.trade_key("goog", "0001193125-26-135334", "nd:1")
    assert key == insider.trade_key("goog", "0001193125-26-135334", "nd:1")
    assert len({key, insider.trade_key("googl", "0001193125-26-135334", "nd:1"),
                insider.trade_key("goog", "0001193125-26-135334", "nd:2")}) == 3  # fmt: skip
