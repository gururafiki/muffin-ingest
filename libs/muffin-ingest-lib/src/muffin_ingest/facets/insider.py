"""Stage 2 of SEC's insider trades: `market.insider_trade` rows from SEC's quarterly data sets.

DECIDED 2026-10-24 (Phase 5 decisions 4 and 10). History comes from SEC's Insider Transactions Data
Sets, one zip a quarter since 2006: SEC's own flattening of every Form 3, 4 and 5 into tab-separated
tables. This module reads SUBMISSION, REPORTINGOWNER, NONDERIV_TRANS and DERIV_TRANS; the holdings,
footnote and signature tables stay in raw.

A LINE IS NAMED BY ITS FILING. The edge keyed a trade on a hash of its own values, so two identical
lines of one filing collapsed into one. Here a line is `(accession, line_key)`, where `line_key` is
`nd:<n>` or `d:<n>`, the n-th non-derivative or derivative transaction in DOCUMENT order. Measured
2026-10-27: the tables' row order is not document order (about half of 2026q1's filings list their
lines out of order), but each line's surrogate key (`NONDERIV_TRANS_SK`, `DERIV_TRANS_SK`) is.
Sorted by it, the lines matched the order of the filing's own XML in all ten tables of nine 2026q1
filings, and the shares owned after each line step consistently only in that order.

THE DATA SET ROUNDS SHARES AND PRICES TO TWO DECIMALS (3,647.903 shares in the XML, 3,647.9 here;
122.9621 and 122.96). A quarter's rows replace the Form 4 lane's for the same filings, so a
fractional share count loses its third decimal then.

ONE OWNER PER LINE, THE LOWEST CIK. A filing can name several reporting owners (1.3% of 2026q1's;
a fund's general partner, manager and fund report one trade together). The data set lists them in
no stable order (a four-owner filing's rows came back in neither the XML's order nor any other), so
"the first owner in the document" cannot be read from it. The lowest CIK is a rule the Form 4 lane
can apply too, so a quarter replacing that lane's rows names the same person.

A CODE `market.insider_transaction_code` LACKS IS WITHHELD AND COUNTED, never written: the table has
no foreign key from `insider_trade` on purpose (the TRL stall's shape), so this is the guard. A line
with no code at all (3 in 2026q1) is withheld the same way.

NO NETWORK. Stage 1 stores the zip whole.
"""

from __future__ import annotations

import csv
import hashlib
import io
import math
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

#: What stage 1 adds to a stored data set: nothing. The quarter is the partition key.
CONTEXT_COLUMNS: frozenset[str] = frozenset()

#: `market.data_source` codes. The edge wrote `sec-form4`, and the Form 4 lane keeps it.
SOURCE_DATASET = "sec-form345"
SOURCE_FORM4 = "sec-form4"

#: SEC's acquired/disposed code, in the words `security_insider_summary` counts.
DIRECTION: dict[str, str] = {"A": "Acquisition", "D": "Disposition"}

Table = Literal["nd", "d"]

#: The columns read, per table. Anything else stays in raw. `AFF10B5ONE` is not among them: SEC
#: added it to SUBMISSION after 2016, and the 2006 and 2016 data sets have thirteen columns.
_SUBMISSION = ("ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERCIK")
_OWNER = ("ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME", "RPTOWNER_TITLE")
_LINE = (
    "ACCESSION_NUMBER",
    "SECURITY_TITLE",
    "TRANS_DATE",
    "TRANS_CODE",
    "TRANS_SHARES",
    "TRANS_PRICEPERSHARE",
    "TRANS_ACQUIRED_DISP_CD",
    "SHRS_OWND_FOLWNG_TRANS",
)
_LINE_TABLES: dict[Table, tuple[str, str]] = {
    "nd": ("NONDERIV_TRANS.tsv", "NONDERIV_TRANS_SK"),
    "d": ("DERIV_TRANS.tsv", "DERIV_TRANS_SK"),
}

#: SEC's date format in every table: `31-MAR-2026`.
_DATE = "%d-%b-%Y"


class DatasetUnreadable(RuntimeError):
    """The body is not an insider data set. Never read as a quarter with no trades."""


@dataclass
class Trades:
    """`market.insider_trade` rows, the filings they came from, and what was decided on the way."""

    rows: list[dict[str, Any]]
    #: Every tracked issuer's filing in the data set, whether or not a line of it was written. The
    #: writer retracts the rows these filings hold before inserting, so a line withheld now does not
    #: leave the Form 4 lane's copy behind.
    accessions: set[str] = field(default_factory=set)
    stats: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True)
class _Filing:
    accession: str
    filing_date: date | None
    form_type: str
    issuer_cik: int


@dataclass(frozen=True)
class _Owner:
    cik: int
    name: str | None
    title: str | None


def quarter_bounds(year: int, quarter: int) -> tuple[date, date]:
    """The first and last day of a calendar quarter: the data set holds the filings filed in it."""
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"no quarter {quarter}")
    first = date(year, 3 * quarter - 2, 1)
    last = date(year + 1, 1, 1) if quarter == 4 else date(year, 3 * quarter + 1, 1)
    return first, date.fromordinal(last.toordinal() - 1)


def line_key(table: Table, ordinal: int) -> str:
    """Which line of a filing: `nd:3` is its third non-derivative transaction."""
    return f"{table}:{ordinal}"


def trade_key(security_id: str, accession: str, key: str) -> str:
    """`insider_trade.trade_key` (its primary key) for a line of a filing, per security."""
    return hashlib.sha256(f"{security_id}|{accession}|{key}".encode()).hexdigest()


def _rows(
    archive: zipfile.ZipFile, name: str, wanted: Sequence[str], malformed: set[str]
) -> Iterator[list[str]]:
    """A table's rows as lists of the WANTED columns, in that order. Streamed: 2006q1's
    NONDERIV_TRANS alone is 171,549 rows, and only tracked issuers' lines are kept. A row of the
    wrong width is not yielded; its accession goes into `malformed`."""
    try:
        handle = archive.open(name)
    except KeyError as exc:
        raise DatasetUnreadable(f"no {name} in the data set") from exc
    with handle, io.TextIOWrapper(handle, "utf-8", newline="") as text:
        reader = csv.reader(text, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(reader, None)
        if header is None:
            raise DatasetUnreadable(f"{name} is empty")
        missing = [c for c in wanted if c not in header]
        if missing:
            raise DatasetUnreadable(f"{name} has no {', '.join(missing)}")
        width = len(header)
        index = [header.index(c) for c in wanted]
        for row in reader:
            if len(row) != width:
                # A tab inside a value would shift every column after it, writing a price into the
                # shares column. Measured: none in 2006q1, 2016q1 or 2026q1.
                malformed.add(row[0] if row else "")
                continue
            yield [row[i] for i in index]


def _date(text: str, stats: Counter[str], what: str) -> date | None:
    if not text:
        return None
    try:
        return datetime.strptime(text, _DATE).date()
    except ValueError:
        stats[f"{what}_unreadable"] += 1
        return None


def _number(text: str, stats: Counter[str]) -> float | None:
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        stats["number_unreadable"] += 1
        return None
    if not math.isfinite(value):
        stats["number_unreadable"] += 1
        return None
    return value


def dataset_rows(
    body: bytes,
    issuers: Mapping[int, Sequence[str]],
    codes: Mapping[str, str],
    *,
    as_of: datetime,
    quarter: tuple[int, int] | None = None,
) -> Trades:
    """Read one quarter's data set into `insider_trade` rows for the tracked issuers.

    `issuers` maps an issuer CIK to every equity it is attributed to (the edge's rule: a filing
    names the issuer, not the share class, and Alphabet's name only `GOOG`). `codes` is
    `market.insider_transaction_code` (`code -> name`). With `quarter`, a filing dated outside it is
    counted: the retraction assumes the data set holds exactly the quarter's filings, which it did
    for 2006q1, 2016q1, 2025q4 and 2026q1.
    """
    try:
        archive = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise DatasetUnreadable(f"not a zip ({body[:60]!r})") from exc

    stats: Counter[str] = Counter()
    malformed: set[str] = set()
    bounds = quarter_bounds(*quarter) if quarter else None
    filings: dict[str, _Filing] = {}
    with archive:
        for accession, filed, form, cik in _rows(archive, "SUBMISSION.tsv", _SUBMISSION, malformed):
            stats["filings"] += 1
            if not cik.isdigit() or int(cik) not in issuers:
                stats["filings_untracked"] += 1
                continue
            filing = _Filing(accession, _date(filed, stats, "filing_date"), form, int(cik))
            filings[accession] = filing
            if bounds and filing.filing_date and not bounds[0] <= filing.filing_date <= bounds[1]:
                stats["filed_outside_quarter"] += 1

        owners: dict[str, list[_Owner]] = defaultdict(list)
        for accession, cik, name, title in _rows(archive, "REPORTINGOWNER.tsv", _OWNER, malformed):
            if accession in filings and cik.isdigit():
                owners[accession].append(_Owner(int(cik), name or None, title or None))

        lines: dict[tuple[str, Table], list[tuple[int, list[str]]]] = defaultdict(list)
        for table, (name, sk) in _LINE_TABLES.items():
            for row in _rows(archive, name, (*_LINE, sk), malformed):
                if row[0] in filings:
                    lines[(row[0], table)].append((int(row[-1]), row[:-1]))

    # A FILING WITH A MALFORMED ROW IS WITHHELD WHOLE, not the row alone: dropping one line would
    # renumber the filing's others (`nd:3` read as `nd:2`). It also leaves the retraction, so the
    # Form 4 lane's copy of it, if any, survives rather than being replaced by nothing.
    for accession in malformed & filings.keys():
        del filings[accession]
        stats["filings_malformed"] += 1
    rows: list[dict[str, Any]] = []
    for (accession, table), found in lines.items():
        if accession not in filings:
            continue
        filing = filings[accession]
        securities = issuers[filing.issuer_cik]
        named = sorted(owners.get(accession, ()), key=lambda o: o.cik)
        owner = named[0] if named else None
        if owner is None:
            stats["owner_missing"] += 1
        found.sort(key=lambda pair: pair[0])
        for ordinal, (_, line) in enumerate(found, start=1):
            _, title, when, code, shares, price, acquired, after = line
            stats[f"lines_{table}"] += 1
            if code not in codes:
                stats[f"code_unknown:{code or '<none>'}"] += 1
                continue
            direction = DIRECTION.get(acquired)
            if direction is None:
                stats["direction_unknown"] += 1
            key = line_key(table, ordinal)
            values = {
                "accession_number": accession,
                "line_key": key,
                "filing_date": filing.filing_date,
                "transaction_date": _date(when, stats, "transaction_date"),
                "owner_name": owner.name if owner else None,
                "owner_title": owner.title if owner else None,
                "owner_cik": owner.cik if owner else None,
                "issuer_cik": filing.issuer_cik,
                "form_type": filing.form_type,
                "transaction_code": code,
                "transaction_type": codes[code],
                "direction": direction,
                "security_type": title or None,
                "shares": _number(shares, stats),
                "price": _number(price, stats),
                "shares_owned_after": _number(after, stats),
                "is_derivative": table == "d",
                "source_code": SOURCE_DATASET,
                "as_of": as_of,
            }
            for security_id in securities:
                rows.append(
                    {
                        "trade_key": trade_key(security_id, accession, key),
                        "security_id": security_id,
                        **values,
                    }
                )

    for accession, filing in filings.items():
        if len(owners.get(accession, ())) > 1:
            stats["several_owners"] += 1
        if filing.form_type.endswith("/A"):
            stats["amendments"] += 1
        if len(issuers[filing.issuer_cik]) > 1:
            stats["issuer_fanned_out"] += 1
    stats["malformed"] = len(malformed)
    stats["issuers_tracked"] = len({f.issuer_cik for f in filings.values()})
    stats["rows"] = len(rows)
    return Trades(rows=rows, accessions=set(filings), stats=stats)
