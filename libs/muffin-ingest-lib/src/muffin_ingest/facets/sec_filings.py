"""Stage 2 of SEC's submissions: a filer's filings, its profile and its former names. Bytes in.

ONE CIK, TWO SHAPES. The main document (`submissions/CIK##########.json`) carries the filer's
identity and its most recent ~1,000 filings under `filings.recent`, and names its older pages in
`filings.files`. An older page is the SAME parallel arrays at the TOP level, with no wrapper.
Reading only the first shape reads nothing from every page, which looks exactly like a filer with
no history. Measured 2026-10-26 on 19 filers and six pages: both shapes carry the same 16 arrays.

PORTED FROM THE EDGE (`market-refresh/xbrl.ts::submissionsFrom`), with three changes:
  * the forms kept come from `market.filing_form.kept`, passed in, instead of a literal list;
  * the filings table is read by edgartools (decision 9), and a table it cannot read raises
    instead of skipping rows: measured over 50,672 filings, none lacks an accession or a date;
  * each filing carries the two links the app opens (`report_url`, `filing_detail_url`), which
    only the retiring openbb resource wrote. Both are built from SEC's real origin, never the
    http-cache base, because a reader follows them.

NO NETWORK. `providers.sec_edgar.fetch` is stage 1's. Reading the filings table configures edgar,
which needs `MUFFIN_USER_AGENT` set, as it always is in the code location.
"""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from muffin_ingest.facets import fiscal
from muffin_ingest.providers import sec_edgar
from muffin_ingest.providers.documents import Document

#: What stage 1 adds to a stored body: which document it is, the older page's name (null for the
#: main document), and the status SEC answered with. None of the three is in the body.
CONTEXT_COLUMNS = frozenset({"document", "page", "http_status"})

Kind = Literal["submissions", "submissions_page"]

#: `000000000` IS SEC'S PLACEHOLDER, NOT AN EIN: every foreign issuer measured carries it (ASML,
#: BHP, BUD, NVO, SAP, TSM, VOD). The same shape as N-PORT's `<cusip>000000000</cusip>`, which once
#: collapsed four companies into one security.
_PLACEHOLDER = re.compile(r"^0+$")


#: The source the filings writer writes under: the edge's filing-history resource used it too, so a
#: row it wrote and one this lane writes are the same provenance.
SOURCE = "sec-submissions"

#: The regulator a filing is made with (`security_filing.disclosure_source_code`).
REGULATOR = "sec"

#: SEC's forms, from the control table (`market.filing_form`): which are kept, which refresh the
#: company facts, and which period each reports.
FORMS_SQL = """
select form_code, kept, refreshes_facts, is_amendment, fiscal_role
  from market.filing_form
 where source_code = %s
"""


@dataclass(frozen=True)
class Form:
    code: str
    kept: bool
    refreshes_facts: bool
    is_amendment: bool
    #: `annual`, `transition` or `interim`; null for an amendment or a form reporting no period.
    fiscal_role: fiscal.Role | None


def forms(conn: Any) -> dict[str, Form]:
    """Every SEC form in `market.filing_form`. A form with no row is not stored."""
    with conn.cursor() as cur:
        cur.execute(FORMS_SQL, (REGULATOR,))
        return {
            str(code): Form(str(code), bool(kept), bool(facts), bool(amend), role)
            for code, kept, facts, amend, role in cur.fetchall()
        }


class SubmissionsUnreadable(RuntimeError):
    """The body is not a submissions document. Never read as a filer with no filings."""


@dataclass(frozen=True)
class Filing:
    accession: str
    form: str
    filing_date: date
    report_date: date | None
    #: SEC sends 1 or 0. Read as a number: `bool("0")` is True, which would send the segments
    #: parser after every pre-2009 filing.
    is_xbrl: bool
    primary_document: str | None

    def report_url(self, cik: int) -> str | None:
        """The filing's main document, as a reader opens it. Null for a filing SEC names none for
        (441 of 50,672 measured, all from before 2001)."""
        if self.primary_document is None:
            return None
        folder = self.accession.replace("-", "")
        return f"{sec_edgar.WWW_ORIGIN}/Archives/edgar/data/{cik}/{folder}/{self.primary_document}"

    def filing_detail_url(self, cik: int) -> str:
        """SEC's index page for the filing: the app's link when there is no main document."""
        return f"{sec_edgar.WWW_ORIGIN}/Archives/edgar/data/{cik}/{self.accession}-index.htm"


@dataclass(frozen=True)
class FormerName:
    name: str
    from_date: date | None
    to_date: date | None


@dataclass(frozen=True)
class Profile:
    """What the main document says about the registrant. Field names are `filer_profile`'s
    columns, plus the SIC (written to `security` too) and the registrant's name."""

    name: str | None
    sic: str | None
    sic_description: str | None
    entity_type: str | None
    owner_org: str | None
    ein: str | None
    lei: str | None
    category: str | None
    fiscal_year_end: str | None
    state_of_incorporation: str | None
    website: str | None
    investor_website: str | None
    phone: str | None
    us_ticker: str | None
    us_exchange: str | None
    hq_street: str | None
    hq_city: str | None
    hq_state_or_country: str | None
    hq_zip: str | None

    def filer_profile_columns(self) -> dict[str, str | None]:
        """The `filer_profile` row, without the security, source and time the writer adds."""
        out = dict(self.__dict__)
        for key in ("name", "sic", "sic_description"):
            del out[key]
        return out


def raw_rows(
    document: Document, *, kind: Kind, page: str | None, http_status: int, run_id: str
) -> list[dict[str, Any]]:
    """The stored row: the body whole, plus which document it is and how SEC answered."""
    return [{**document.as_row(run_id), "document": kind, "page": page, "http_status": http_status}]


def _load(body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise SubmissionsUnreadable(f"not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SubmissionsUnreadable(f"not an object: {type(payload).__name__}")
    return payload


def _table(payload: dict[str, Any]) -> dict[str, Any]:
    """The filings arrays, from either shape."""
    if "accessionNumber" in payload:
        return payload
    recent = (payload.get("filings") or {}).get("recent")
    if not isinstance(recent, dict) or "accessionNumber" not in recent:
        raise SubmissionsUnreadable("neither a main document nor an older page")
    return recent


def filings(body: bytes, forms: Collection[str]) -> list[Filing]:
    """The filings of one document whose form is in `forms`, in SEC's order (newest first)."""
    table = _table(_load(body))
    # PARSE ERRORS ONLY (pyarrow's `ArrowInvalid` is a `ValueError`). Anything else, such as a
    # process with no `MUFFIN_USER_AGENT` to configure edgar with, is a fault of the run and must
    # not be counted against the filer.
    try:
        rows = sec_edgar.filings_table(table)
    except (ValueError, KeyError, TypeError) as exc:
        raise SubmissionsUnreadable(f"edgartools could not read the filings: {exc}") from exc
    wanted = set(forms)
    out: list[Filing] = []
    for row in rows:
        if row["form"] not in wanted:
            continue
        report = _text(row["reportDate"])
        out.append(
            Filing(
                accession=str(row["accession_number"]),
                form=str(row["form"]),
                filing_date=row["filing_date"],
                report_date=date.fromisoformat(report) if report else None,
                is_xbrl=_flag(row["isXBRL"]),
                primary_document=_text(row["primaryDocument"]),
            )
        )
    return out


def older_pages(body: bytes) -> list[str]:
    """The names of the filer's older pages, from the main document. A page names none."""
    payload = _load(body)
    files = (payload.get("filings") or {}).get("files") or []
    return [str(f["name"]) for f in files if isinstance(f, dict) and f.get("name")]


def profile(body: bytes) -> Profile:
    """The registrant's identity, from the main document."""
    d = _load(body)
    if "filings" not in d:
        raise SubmissionsUnreadable("an older page carries no profile")
    # `addresses.business` is where the company operates; `mailing` is often a registered agent.
    address = (d.get("addresses") or {}).get("business") or {}
    ein = _text(d.get("ein"))
    return Profile(
        name=_text(d.get("name")),
        sic=_text(d.get("sic")),
        sic_description=_text(d.get("sicDescription")),
        entity_type=_text(d.get("entityType")),
        owner_org=_text(d.get("ownerOrg")),
        ein=None if ein is None or _PLACEHOLDER.match(ein) else ein,
        lei=_text(d.get("lei")),
        category=_text(d.get("category")),
        fiscal_year_end=_text(d.get("fiscalYearEnd")),
        state_of_incorporation=_text(d.get("stateOfIncorporation")),
        website=_text(d.get("website")),
        investor_website=_text(d.get("investorWebsite")),
        phone=_text(d.get("phone")),
        # The FIRST of each array, which SEC pairs by position: a ticker and the venue it trades on.
        us_ticker=_first(d.get("tickers")),
        us_exchange=_first(d.get("exchanges")),
        hq_street=_text(address.get("street1")),
        hq_city=_text(address.get("city")),
        hq_state_or_country=_text(address.get("stateOrCountry")),
        hq_zip=_text(address.get("zipCode")),
    )


def former_names(body: bytes) -> list[FormerName]:
    """The registrant's earlier names, one per name.

    ONE PER NAME because `security_former_name` is keyed on it, and an upsert carrying a key twice
    fails the whole statement. A name SEC lists twice (a company renamed back) keeps its latest
    span.
    """
    d = _load(body)
    by_name: dict[str, FormerName] = {}
    for item in d.get("formerNames") or []:
        if not isinstance(item, dict):
            continue
        name = _text(item.get("name"))
        if name is None:
            continue
        found = FormerName(name, _day(item.get("from")), _day(item.get("to")))
        held = by_name.get(name)
        if held is None or (found.to_date or date.min) > (held.to_date or date.min):
            by_name[name] = found
    return list(by_name.values())


def _text(value: Any) -> str | None:
    """SEC sends `""` and `null` for a field it does not hold. Both are absence, never a value."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first(value: Any) -> str | None:
    return _text(value[0]) if isinstance(value, list) and value else None


def _flag(value: Any) -> bool:
    try:
        return int(value) == 1
    except (TypeError, ValueError):
        return False


def _day(value: Any) -> date | None:
    """SEC sends a full timestamp (`2007-01-10T00:00:00.000Z`); only the date means anything."""
    text = _text(value)
    return date.fromisoformat(text[:10]) if text else None


def reports(found: Collection[Filing], forms_by_code: Mapping[str, Form]) -> list[fiscal.Report]:
    """The registrant's own accounts reports, as the calendar reads them.

    A form reports a period only if `filing_form.fiscal_role` says so, which leaves out every
    amendment (a 10-K/A restates a year the 10-K already gave) and every 6-K (monthly revenue as
    often as results). A report SEC names no period for cannot place one, and is left out too.
    """
    out: list[fiscal.Report] = []
    for filing in found:
        form = forms_by_code.get(filing.form)
        if form is None or form.fiscal_role is None or filing.report_date is None:
            continue
        out.append(
            fiscal.Report(filing.accession, filing.form, filing.report_date, form.fiscal_role)
        )
    return out


def filing_rows(
    found: Collection[Filing], *, cik: int, security_ids: Collection[str], as_of: str
) -> list[dict[str, Any]]:
    """`market.security_filing` rows, one per filing and security the CIK maps to.

    NEVER `segments_parsed_at` OR `segments_parser_version`: the edge's segments handler owns both
    until Phase 6, and a writer naming them would re-queue or hide every filing it touches.
    """
    rows: list[dict[str, Any]] = []
    for filing in found:
        common = {
            "accession_number": filing.accession,
            "filing_date": filing.filing_date.isoformat(),
            "report_date": filing.report_date.isoformat() if filing.report_date else None,
            "report_type": filing.form,
            "report_url": filing.report_url(cik),
            "filing_detail_url": filing.filing_detail_url(cik),
            "is_xbrl": filing.is_xbrl,
            "source_code": SOURCE,
            "disclosure_source_code": REGULATOR,
            "as_of": as_of,
        }
        rows += [{"security_id": sid, **common} for sid in security_ids]
    return rows
