"""SEC's daily form index and its quarter listing, over captured bytes (2026-10-23, 2026 QTR4)."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from muffin_ingest.facets import sec_index
from muffin_ingest.providers.documents import Document

FIXTURES = Path(__file__).parents[1] / "fixtures" / "sec"
INDEX = (FIXTURES / "form.20261023.trimmed.idx").read_bytes()
LISTING = (FIXTURES / "daily_index_2026_QTR4_listing.json").read_bytes()
_LINES = INDEX.splitlines(keepends=True)
#: The header and its separator, and no filing line.
HEADER_ONLY = b"".join(_LINES[: next(i for i, x in enumerate(_LINES) if x.startswith(b"---")) + 1])


def test_every_line_parses_including_a_form_wider_than_its_column() -> None:
    lines = sec_index.parse(INDEX)
    assert len(lines) == 21
    wide = [line for line in lines if line.form == "SCHEDULE 13G/A"]
    assert wide, "the fixture keeps the overflow case"
    # A fixed-width read would put the form's tail in the company name.
    assert all(not line.company.startswith(("A ", "/A")) for line in wide)
    first = lines[0]
    assert (first.form, first.company, first.cik) == ("10-K", "ACUITY INC. (DE)", 1144215)
    assert first.accession == "0001144215-26-000052"


def test_a_day_s_index_can_name_a_filing_dated_earlier() -> None:
    """Late dissemination: `filed` is SEC's date, and the index day is the caller's key."""
    assert any(line.filed < date(2026, 10, 23) for line in sec_index.parse(INDEX))


@pytest.mark.parametrize(
    "body",
    [
        b"<html>Access Denied</html>",
        INDEX.split(b"-----")[0],
        HEADER_ONLY,
        INDEX + b"this line is not an index line\n",
    ],
    ids=["not-an-index", "no-separator", "no-filing-line", "a-garbled-line"],
)
def test_an_index_that_does_not_parse_raises_rather_than_reading_empty(body: bytes) -> None:
    with pytest.raises(sec_index.IndexUnreadable):
        sec_index.parse(body)


def test_the_listing_names_the_days_sec_published() -> None:
    days = sec_index.listing_days(LISTING)
    assert len(days) == 16
    assert date(2026, 10, 23) in days
    assert date(2026, 10, 12) not in days  # Columbus Day


@pytest.mark.parametrize(
    ("day", "listed", "want"),
    [
        (date(2026, 10, 23), None, "published"),
        (date(2026, 10, 12), None, "holiday"),
        (date(2026, 10, 24), None, "weekend"),
        (date(2026, 10, 26), None, "not_published"),
        # The same weekday is a holiday only once a later day is listed.
        (date(2026, 10, 12), {date(2026, 10, 9)}, "not_published"),
    ],
    ids=["published", "columbus-day", "saturday", "today", "nothing-later-yet"],
)
def test_a_day_is_classified_from_the_listing_without_asking_for_it(
    day: date, listed: set[date] | None, want: str
) -> None:
    days = sec_index.listing_days(LISTING) if listed is None else listed
    assert sec_index.classify_day(day, days) == want


def test_named_keeps_the_tracked_filers_forms() -> None:
    lines = sec_index.parse(INDEX)
    tracked = {line.cik for line in lines if line.form == "4"}
    kept = sec_index.named(lines, tracked, {"4"})
    assert kept and all(line.form == "4" and line.cik in tracked for line in kept)
    assert sec_index.named(lines, tracked, {"10-K"}) == [
        line for line in lines if line.form == "10-K" and line.cik in tracked
    ]


def test_stage_1_adds_only_its_context_columns_and_keeps_the_bytes() -> None:
    document = Document(
        url="https://www.sec.gov/Archives/edgar/daily-index/2026/QTR4/form.20261023.idx",
        body=INDEX,
        content_type="text/plain",
        fetched_at=datetime(2026, 10, 24, 11, tzinfo=UTC),
    )
    [row] = sec_index.raw_rows(document, kind="form_index", http_status=200, run_id="r")
    assert set(row) - set(document.as_row("r")) == sec_index.CONTEXT_COLUMNS
    assert hashlib.sha256(row["body"]).hexdigest() == hashlib.sha256(INDEX).hexdigest()
