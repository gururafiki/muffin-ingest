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

EACH DAY'S FORM 4s are read here too (`form4_rows`), by OUR reader of the filing's XML, not
edgartools' `Form4.parse_xml` (decided 2026-10-27, amending decision 9). Measured that day: that
parser fetches `data.sec.gov/submissions/CIK<owner>.json` for every reporting owner, to decide
whether to reverse the name, and raises after five retries if SEC does not answer, so stage 2 would
have made one request per owner outside the pool's pacing and the request counters. It also
rewrote what the data set states: "Ternus John" became "John Ternus", an empty expiration date came
back as the footnote marker "[F2]", and "See Remarks" titles were replaced by the remarks. Both
lanes go through `_filing_rows`, so a line means the same thing whichever lane wrote it, and the
nine captured 2026q1 filings reproduce the data set's rows (to its two decimals).

NO NETWORK. Stage 1 stores the zip and each filing's complete submission text whole.
"""

from __future__ import annotations

import csv
import hashlib
import io
import math
import re
import xml.etree.ElementTree as ET
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


@dataclass(frozen=True)
class _Line:
    """One transaction line, as either lane reads it, before it is checked against the tables."""

    title: str | None
    when: date | None
    code: str
    shares: float | None
    price: float | None
    acquired: str
    after: float | None


class Form4Unreadable(RuntimeError):
    """The body is not a Form 3, 4 or 5 submission. Never read as a filing with no lines."""


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


def _filing_rows(
    stats: Counter[str],
    *,
    filing: _Filing,
    owners: Sequence[_Owner],
    lines: Mapping[Table, Sequence[_Line]],
    securities: Sequence[str],
    codes: Mapping[str, str],
    source: str,
    as_of: datetime,
) -> list[dict[str, Any]]:
    """One filing's rows: each line, in document order, once per equity of the issuer."""
    owner = min(owners, key=lambda o: o.cik) if owners else None
    if owner is None:
        stats["owner_missing"] += 1
    if len(owners) > 1:
        stats["several_owners"] += 1
    if filing.form_type.endswith("/A"):
        stats["amendments"] += 1
    if len(securities) > 1:
        stats["issuer_fanned_out"] += 1
    rows: list[dict[str, Any]] = []
    for table, found in lines.items():
        for ordinal, line in enumerate(found, start=1):
            stats[f"lines_{table}"] += 1
            if line.code not in codes:
                stats[f"code_unknown:{line.code or '<none>'}"] += 1
                continue
            direction = DIRECTION.get(line.acquired)
            if direction is None:
                stats["direction_unknown"] += 1
            key = line_key(table, ordinal)
            values = {
                "accession_number": filing.accession,
                "line_key": key,
                "filing_date": filing.filing_date,
                "transaction_date": line.when,
                "owner_name": owner.name if owner else None,
                "owner_title": owner.title if owner else None,
                "owner_cik": owner.cik if owner else None,
                "issuer_cik": filing.issuer_cik,
                "form_type": filing.form_type,
                "transaction_code": line.code,
                "transaction_type": codes[line.code],
                "direction": direction,
                "security_type": line.title,
                "shares": line.shares,
                "price": line.price,
                "shares_owned_after": line.after,
                "is_derivative": table == "d",
                "source_code": source,
                "as_of": as_of,
            }
            for security_id in securities:
                rows.append(
                    {
                        "trade_key": trade_key(security_id, filing.accession, key),
                        "security_id": security_id,
                        **values,
                    }
                )
    return rows


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

        found: dict[str, dict[Table, list[tuple[int, _Line]]]] = defaultdict(
            lambda: {"nd": [], "d": []}
        )
        for table, (name, sk) in _LINE_TABLES.items():
            for row in _rows(archive, name, (*_LINE, sk), malformed):
                if row[0] not in filings:
                    continue
                _, title, when, code, shares, price, acquired, after, order = row
                line = _Line(
                    title=title or None,
                    when=_date(when, stats, "transaction_date"),
                    code=code,
                    shares=_number(shares, stats),
                    price=_number(price, stats),
                    acquired=acquired,
                    after=_number(after, stats),
                )
                found[row[0]][table].append((int(order), line))

    # A FILING WITH A MALFORMED ROW IS WITHHELD WHOLE, not the row alone: dropping one line would
    # renumber the filing's others (`nd:3` read as `nd:2`). It also leaves the retraction, so the
    # Form 4 lane's copy of it, if any, survives rather than being replaced by nothing.
    for accession in malformed & filings.keys():
        del filings[accession]
        stats["filings_malformed"] += 1
    rows: list[dict[str, Any]] = []
    for accession, filing in filings.items():
        tables = found.get(accession, {"nd": [], "d": []})
        rows += _filing_rows(
            stats,
            filing=filing,
            owners=owners.get(accession, ()),
            # Document order is the surrogate key's; the file's own order is not.
            lines={
                t: [line for _, line in sorted(ls, key=lambda p: p[0])] for t, ls in tables.items()
            },
            securities=issuers[filing.issuer_cik],
            codes=codes,
            source=SOURCE_DATASET,
            as_of=as_of,
        )
    stats["malformed"] = len(malformed)
    stats["issuers_tracked"] = len({f.issuer_cik for f in filings.values()})
    stats["rows"] = len(rows)
    return Trades(rows=rows, accessions=set(filings), stats=stats)


#: The complete submission's header: which filing, and the day SEC dates it.
_ACCESSION = re.compile(rb"^ACCESSION NUMBER:\s*(\S+)", re.M)
_FILED = re.compile(rb"^FILED AS OF DATE:\s*(\d{8})", re.M)
_XML = re.compile(rb"<XML>\s*(.*?)\s*</XML>", re.S)

#: Each lane's table of lines in an ownership document.
_XML_TABLES: dict[Table, str] = {
    "nd": "nonDerivativeTable/nonDerivativeTransaction",
    "d": "derivativeTable/derivativeTransaction",
}


def _text(element: ET.Element | None, path: str) -> str | None:
    """The stripped text at `path`, or None. A footnote reference is not a value: an element
    holding only `<footnoteId>` reads as None, never as the marker."""
    if element is None:
        return None
    found = element.find(path)
    if found is None or found.text is None:
        return None
    text = found.text.strip()
    return text or None


def _iso_date(text: str | None, stats: Counter[str], what: str) -> date | None:
    """An XML date, which may carry a zone offset after the day (`2026-03-27-05:00`)."""
    if text is None:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        stats[f"{what}_unreadable"] += 1
        return None


def form4_rows(
    body: bytes,
    issuers: Mapping[int, Sequence[str]],
    codes: Mapping[str, str],
    *,
    as_of: datetime,
) -> Trades:
    """Read one filing's complete submission text (`<accession>.txt`) into `insider_trade` rows.

    The same rules as `dataset_rows`, through the same builder; only the reading differs. The
    filing names its issuer, and an issuer we do not track (an owner that is itself a tracked
    company files under its own CIK as well) yields nothing and is counted.

    Values keep the filing's precision, where the data set rounds to two decimals; a quarter's data
    set replaces these rows once it is published.
    """
    stats: Counter[str] = Counter()
    accession_match = _ACCESSION.search(body)
    xml_match = _XML.search(body)
    if accession_match is None or xml_match is None:
        raise Form4Unreadable(
            f"not a complete submission with an ownership document ({body[:60]!r})"
        )
    try:
        root = ET.fromstring(xml_match.group(1))
    except ET.ParseError as exc:
        raise Form4Unreadable(f"its ownership document is not XML: {exc}") from exc
    if root.tag != "ownershipDocument":
        raise Form4Unreadable(f"its XML is a {root.tag}, not an ownership document")

    accession = accession_match.group(1).decode()
    filed_match = _FILED.search(body)
    filed = (
        _iso_date(f"{m[:4]}-{m[4:6]}-{m[6:]}", stats, "filing_date")
        if filed_match and (m := filed_match.group(1).decode())
        else None
    )
    cik = _text(root, "issuer/issuerCik")
    stats["filings"] = 1
    if cik is None or not cik.isdigit() or int(cik) not in issuers:
        stats["filings_untracked"] = 1
        return Trades(rows=[], accessions=set(), stats=stats)
    filing = _Filing(accession, filed, _text(root, "documentType") or "", int(cik))

    owners = []
    for node in root.findall("reportingOwner"):
        owner_cik = _text(node, "reportingOwnerId/rptOwnerCik")
        if owner_cik and owner_cik.isdigit():
            owners.append(
                _Owner(
                    int(owner_cik),
                    _text(node, "reportingOwnerId/rptOwnerName"),
                    _text(node, "reportingOwnerRelationship/officerTitle"),
                )
            )
    lines: dict[Table, list[_Line]] = {}
    for table, path in _XML_TABLES.items():
        lines[table] = [
            _Line(
                title=_text(node, "securityTitle/value"),
                when=_iso_date(_text(node, "transactionDate/value"), stats, "transaction_date"),
                code=_text(node, "transactionCoding/transactionCode") or "",
                shares=_number(
                    _text(node, "transactionAmounts/transactionShares/value") or "", stats
                ),
                price=_number(
                    _text(node, "transactionAmounts/transactionPricePerShare/value") or "", stats
                ),
                acquired=_text(node, "transactionAmounts/transactionAcquiredDisposedCode/value")
                or "",
                after=_number(
                    _text(node, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")
                    or "",
                    stats,
                ),
            )
            for node in root.findall(path)
        ]
    rows = _filing_rows(
        stats,
        filing=filing,
        owners=owners,
        lines=lines,
        securities=issuers[filing.issuer_cik],
        codes=codes,
        source=SOURCE_FORM4,
        as_of=as_of,
    )
    stats["issuers_tracked"] = 1
    stats["rows"] = len(rows)
    return Trades(rows=rows, accessions={accession}, stats=stats)
