"""Stage 2 of SEC's daily index: which filings SEC disseminated on one day. Bytes in, lines out.

THE INDEX IS THE SEC FAMILY'S TRIGGER. One ~650 KB file a day names every filing SEC disseminated,
with its form, filer CIK and path, so a filer is re-read when it files rather than on a timer
(Phase 5, decided 2026-10-24). Polling each filer's submissions instead would cost ~3,400 requests
a day to learn what one file says.

THREE FACTS MEASURED 2026-10-26 SHAPE THIS MODULE:

  * A DAY WITH NO INDEX ANSWERS 403, NOT 404. Columbus Day, a Saturday, and today before
    publication all return S3's 263-byte `AccessDenied` XML, which reads as SEC refusing us. So the
    lane asks the quarter's LISTING first (one request) and never requests a day it does not name:
    `classify_day` decides weekend, holiday, published or not yet published from the listing alone.
  * THE FORMAT IS FIXED-WIDTH UNTIL IT IS NOT. The form column is 12 characters, and a longer form
    (`SCHEDULE 13G/A`, 47-127 lines a day) pushes the company name right. The separators that hold
    are runs of two or more spaces, and the last three fields (CIK, date, path) are anchored right.
  * A DAY'S INDEX NAMES FILINGS DATED WEEKS EARLIER (late dissemination: 20251223 in the
    2026-10-19 file). `filed` is SEC's date; which index named it is the caller's partition key.
"""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

from muffin_ingest.providers.documents import Document

#: What stage 1 adds to a stored body: which document it is (`form_index` or `quarter_listing`) and
#: the status SEC answered with. Both are needed to read the file back and neither is in the body.
CONTEXT_COLUMNS = frozenset({"document", "http_status"})

Kind = Literal["form_index", "quarter_listing"]
DayState = Literal["weekend", "published", "holiday", "not_published"]

_LINE = re.compile(r"^(\S.*?)\s{2,}(.*\S)\s+(\d+)\s+(\d{8})\s+(\S+)\s*$")
_LISTED = re.compile(r"^form\.(\d{8})\.idx$")


class IndexUnreadable(RuntimeError):
    """The body is not a daily form index. Never read as a day with no filings."""


@dataclass(frozen=True)
class IndexLine:
    form: str
    company: str
    cik: int
    filed: date
    path: str

    @property
    def accession(self) -> str:
        """`edgar/data/1745931/0001493152-26-048210.txt` -> `0001493152-26-048210`."""
        return self.path.rsplit("/", 1)[-1].removesuffix(".txt")


def raw_rows(
    document: Document, *, kind: Kind, http_status: int, run_id: str
) -> list[dict[str, Any]]:
    """The stored row: the body whole, plus which document it is and how SEC answered."""
    return [{**document.as_row(run_id), "document": kind, "http_status": http_status}]


def parse(body: bytes) -> list[IndexLine]:
    """Every filing line of one `form.<yyyymmdd>.idx`, in file order.

    A body with no dashed separator, or a line that does not parse, raises: an index read as empty
    would look exactly like a day on which nobody filed.
    """
    text = body.decode("latin-1")
    lines = text.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.startswith("-----"))
    except StopIteration:
        raise IndexUnreadable("no separator line: not a daily form index") from None
    out: list[IndexLine] = []
    for number, line in enumerate(lines[start + 1 :], start=start + 2):
        if not line.strip():
            continue
        match = _LINE.match(line)
        if match is None:
            raise IndexUnreadable(f"line {number} does not parse: {line[:120]!r}")
        form, company, cik, filed, path = match.groups()
        out.append(
            IndexLine(
                form=form,
                company=company,
                cik=int(cik),
                filed=datetime.strptime(filed, "%Y%m%d").date(),
                path=path,
            )
        )
    if not out:
        raise IndexUnreadable("no filing lines: SEC publishes an index only for a business day")
    return out


def listing_days(body: bytes) -> set[date]:
    """The days a quarter's directory listing names a `form.<yyyymmdd>.idx` for."""
    try:
        items = json.loads(body)["directory"]["item"]
    except (ValueError, KeyError, TypeError) as exc:
        raise IndexUnreadable(f"not a directory listing: {exc}") from exc
    days: set[date] = set()
    for item in items:
        match = _LISTED.match(str(item.get("name", "")))
        if match:
            days.add(datetime.strptime(match.group(1), "%Y%m%d").date())
    return days


def classify_day(day: date, listed: Collection[date]) -> DayState:
    """What SEC published for `day`, decided without asking for the day itself.

    `listed` is every day the relevant listings name: the day's quarter, and the next quarter's too
    when the day is the quarter's last business day (an observed New Year's Day falls on 31
    December). A weekday absent from the listing is a holiday once a LATER day is listed, and not
    yet published until then.
    """
    if day.weekday() >= 5:
        return "weekend"
    if day in listed:
        return "published"
    if any(later > day for later in listed):
        return "holiday"
    return "not_published"


def named(
    lines: Iterable[IndexLine], ciks: Collection[int], forms: Collection[str]
) -> list[IndexLine]:
    """The lines whose filer is one of `ciks` and whose form is one of `forms`."""
    return [line for line in lines if line.cik in ciks and line.form in forms]
