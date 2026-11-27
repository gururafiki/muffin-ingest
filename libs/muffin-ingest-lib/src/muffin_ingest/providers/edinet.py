"""EDINET (Japan's Financial Services Agency): its code list, one day's documents, and one
document's archive.

Three requests, three grains, measured from the node on 2026-11-26/27:

  * the CODE LIST (`searchdocument/codelist/Edinetcode.zip` on `disclosure2dl`): one ZIP of
    575,634 bytes holding `EdinetcodeDlInfo.csv` (Shift_JIS), 11,475 filers with their EDINET code
    and, for listed ones, the five-digit 証券コード (the local code plus a check digit, `72030`).
    It needs no key;
  * ONE DAY'S DOCUMENTS (`api/v2/documents.json?date=&type=2`), the only way to discover a filing:
    EDINET has no per-company listing. 0.7 MB in ~1-2.5 s (2026-06-18: 720 documents, 154 annual
    reports). THE LIST KEEPS A ROLLING TEN YEARS: a day older than that (bisected 2026-11-27 to
    between 2016-11-24 and 2016-11-28) answers `metadata.status` 404, and so does a future day;
  * one document's ARCHIVE (`api/v2/documents/{docID}?type=1`), a ZIP holding the XBRL instance
    (0.6-2.6 MB, 3.6-8 s; Toyota's 2017 report still downloads while its day is listed).

EDINET ANSWERS EVERY REQUEST WITH HTTP 200, so the status is in the body, measured 2026-11-27:
  - a bad or missing key: `{"StatusCode": 401, "message": "Access denied …"}`;
  - a list or document that does not exist: `{"metadata": {"status": "404", …}}`;
  - a malformed date: `{"metadata": {"status": "400", "message": "Bad Request"}}`;
  - a document: the ZIP itself (`PK`).
`classify` reads which. A refusal (the key, a throttle, a server fault, our own bad request) is
raised and never returned, because it says nothing about the document.

THE KEY TRAVELS IN THE QUERY STRING (`Subscription-Key`), so it is redacted from every URL a
document keeps (`redacted`), as DART's is: raw holds what EDINET sent, never our credential.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

from muffin_ingest import metrics, settings
from muffin_ingest.providers.documents import Document

ORIGIN = "https://api.edinet-fsa.go.jp"
DOWNLOAD_ORIGIN = "https://disclosure2dl.edinet-fsa.go.jp"

#: An annual report (有価証券報告書): document type 120, ordinance 010 (企業内容等の開示に関する
#: 内閣府令), form 030000. Amendments (130) and the other forms are listed and not fetched.
ANNUAL_REPORT = {"docTypeCode": "120", "ordinanceCode": "010", "formCode": "030000"}

#: An archive larger than this is refused unread. The largest measured is 2.6 MB.
MAX_ARCHIVE_BYTES = 32 * 1024 * 1024

_KEY = re.compile(r"(Subscription-Key=)[^&]*")

Kind = Literal["zip", "ok", "absent", "throttled", "refused"]


class EdinetRefused(RuntimeError):
    """EDINET refused the request (the key, a throttle, a fault, a malformed request). Never an
    absence: it says nothing about the document."""

    def __init__(self, status: int, url: str, message: str = "") -> None:
        super().__init__(f"EDINET refused {url}: status {status} {message}".rstrip())
        self.status = status


class EdinetTransport(RuntimeError):
    """The request never got an answer. Never an absence."""


@dataclass(frozen=True)
class Answer:
    """What EDINET served, unchanged, with the status its body states. `too_large` is an archive
    over the gate, not read."""

    document: Document
    status: int
    too_large: bool = False


def redacted(url: str) -> str:
    """A URL without our credential."""
    return _KEY.sub(r"\1REDACTED", url)


def classify(body: bytes) -> tuple[Kind, int]:
    """What a body says, and the status it states. A ZIP is a document; JSON carries either the
    gateway's `StatusCode` (the key, a throttle) or the API's `metadata.status`."""
    if body[:2] == b"PK":
        return "zip", 200
    try:
        parsed: Any = json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError):
        return "refused", 0
    if not isinstance(parsed, dict):
        return "refused", 0
    gateway = parsed.get("StatusCode")
    if gateway is not None:
        status = _int(gateway)
        return ("throttled" if status == 429 else "refused"), status
    meta = parsed.get("metadata")
    status = _int(meta.get("status")) if isinstance(meta, dict) else 0
    if status == 200:
        return "ok", 200
    if status == 404:
        return "absent", 404
    return ("throttled" if status == 429 else "refused"), status


def _int(value: object) -> int:
    try:
        return int(str(value))
    except ValueError:
        return 0


def _key() -> str:
    key = os.environ.get("EDINET_API_KEY", "")
    if not key:
        raise EdinetTransport("EDINET_API_KEY is not set")
    return key


def _get(
    base: str,
    path: str,
    params: dict[str, str],
    *,
    keyed: bool = True,
    timeout_s: float = 120.0,
    max_bytes: int | None = None,
) -> Answer:
    import httpx

    url = f"{base}/{path}"
    sent = {"User-Agent": settings.user_agent()}
    query = {**params, **({"Subscription-Key": _key()} if keyed else {})}
    refused: EdinetRefused | None = None
    too_large = False
    with metrics.request("edinet") as outcome:
        try:
            with (
                httpx.Client(timeout=timeout_s, follow_redirects=False) as client,
                client.stream("GET", url, params=query, headers=sent) as response,
            ):
                full_url = redacted(str(response.url))
                body, too_large = _read_bounded(response, max_bytes)
        except httpx.HTTPError as exc:
            raise EdinetTransport(f"{path}: {type(exc).__name__}: {redacted(str(exc))}") from exc
        if response.status_code != 200:
            outcome["outcome"] = "throttled" if response.status_code == 429 else "refused"
            refused = EdinetRefused(response.status_code, full_url)
            status = response.status_code
        elif too_large:
            outcome["outcome"] = "too_large"
            status = 200
        else:
            kind, status = classify(body)
            if kind == "absent":
                outcome["outcome"] = "absent"
            elif kind in ("throttled", "refused"):
                outcome["outcome"] = kind
                refused = EdinetRefused(status, full_url, body[:200].decode("utf-8", "replace"))
    # RAISED OUTSIDE THE METRICS BLOCK, which records any exception escaping it as `transport`.
    if refused is not None:
        raise refused
    return Answer(
        document=Document(
            url=full_url,
            body=body,
            content_type=response.headers.get("content-type", "application/octet-stream"),
            fetched_at=datetime.now(UTC),
        ),
        status=status,
        too_large=too_large,
    )


def _read_bounded(response: Any, max_bytes: int | None) -> tuple[bytes, bool]:
    """A streamed body, or (b"", True) once it is known to be over `max_bytes`."""
    if max_bytes is None or response.status_code != 200:
        return bytes(response.read()), False
    declared = response.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > max_bytes:
        return b"", True
    chunks: list[bytes] = []
    read = 0
    for chunk in response.iter_bytes():
        read += len(chunk)
        if read > max_bytes:
            return b"", True
        chunks.append(chunk)
    return b"".join(chunks), False


def _api() -> str:
    return settings.provider_base("edinet", ORIGIN)


def daily_index(day: date | str, timeout_s: float = 120.0) -> Answer:
    """Every document filed on `day` (Tokyo), with its metadata (`type=2`)."""
    return _get(_api(), "api/v2/documents.json", {"date": str(day), "type": "2"},
                timeout_s=timeout_s)  # fmt: skip


def document(doc_id: str, timeout_s: float = 240.0) -> Answer:
    """A document's archive (`type=1`: the XBRL and its attachments)."""
    return _get(_api(), f"api/v2/documents/{doc_id}", {"type": "1"}, timeout_s=timeout_s,
                max_bytes=MAX_ARCHIVE_BYTES)  # fmt: skip


def code_list(timeout_s: float = 240.0) -> Answer:
    """The code list: every filer's EDINET code and, for a listed one, its securities code."""
    base = settings.provider_base("edinet-dl", DOWNLOAD_ORIGIN)
    return _get(base, "searchdocument/codelist/Edinetcode.zip", {}, keyed=False,
                timeout_s=timeout_s)  # fmt: skip
