"""EDINET: the provider's answers, the code list, a day's list, an archive, and a report read.

The instance fixtures are three real FY2026 annual reports trimmed by `edinet_trim.py` to what
`read_report` reads (the root, units, the contexts kept facts use, the segment-note block, and
the numeric facts of anchorable contexts), and remain valid XBRL:
  - Yellow Hat (S100YC2P, JGAAP): a 報告セグメント group header with its 計, and goodwill tables
    under a heading naming the segments;
  - Makita (S100YH4M, IFRS): regions as its reportable segments, a footnote mentioning 地域, and
    a regional table whose cells carry an inline 「(うち:ドイツ)」;
  - Kanesou (S100YGWS): no subsidiaries, so a non-consolidated note and figures.
`edinet_day_2026-06-25.json` is that day's list trimmed to one document of each shape.
"""

from __future__ import annotations

import gzip
import io
import json
import zipfile
from pathlib import Path

import httpx
import pytest
import respx

from muffin_ingest.facets import edinet as fe
from muffin_ingest.facets import segments
from muffin_ingest.providers import edinet
from tests.unit import edinet_rules

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "edinet"


def instance(name: str) -> str:
    with gzip.open(FIXTURES / f"edinet_{name}.xbrl.gz", "rt", encoding="utf-8") as fh:
        return fh.read()


def split(r: fe.ReportRead, axis: str, end: str, metric: str = "revenue") -> dict[str, float]:
    return {
        f.member_code.removeprefix("edinet:"): f.value
        for f in r.note.facts
        if f.axis == f"edinet:{axis}" and f.metric_code == metric and f.period_ending == end
    }


# ── The provider ───────────────────────────────────────────────────────────────────────────────

GATEWAY_401 = (
    b'{"StatusCode": 401,"message": "Access denied due to invalid subscription key.Make sure to '
    b'provide a valid key for an active subscription."}'
)
NOT_FOUND = '{\r\n  "metadata": {\r\n    "title": "提出された書類を取得するためのAPI",\r\n    "status": "404",\r\n    "message": "Not Found"\r\n  }\r\n}'.encode()  # noqa: E501
BAD_REQUEST = b'{"metadata": {"status": "400", "message": "Bad Request"}}'


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (b"PK\x03\x04rest", ("zip", 200)),
        (GATEWAY_401, ("refused", 401)),
        (b'{"StatusCode": 429, "message": "Rate limit"}', ("throttled", 429)),
        (NOT_FOUND, ("absent", 404)),
        (BAD_REQUEST, ("refused", 400)),
        ((FIXTURES / "edinet_day_2026-06-25.json").read_bytes(), ("ok", 200)),
        (b"<html>gateway</html>", ("refused", 0)),
    ],
)
def test_every_answer_is_http_200_so_the_body_says_what_it_is(
    body: bytes, expected: tuple[str, int]
) -> None:
    """Measured 2026-11-27: a bad key, a missing document and a malformed date all answer HTTP 200,
    and only the body says which. A refusal is never an absence."""
    assert edinet.classify(body) == expected


def test_the_key_is_redacted_from_every_url_kept() -> None:
    url = "https://api.edinet-fsa.go.jp/api/v2/documents/S100Y9NX?type=1&Subscription-Key=abc123"
    assert edinet.redacted(url).endswith("Subscription-Key=REDACTED")


@respx.mock
def test_a_refusal_in_the_body_is_raised_and_an_absence_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDINET_API_KEY", "secret-key")
    monkeypatch.setenv("MUFFIN_USER_AGENT", "muffin-test/1.0 (ops@example.com)")
    monkeypatch.delenv("HTTP_CACHE_URL", raising=False)
    monkeypatch.delenv("EDINET_BASE_URL", raising=False)
    respx.get(url__regex=r".*/api/v2/documents/S100ZZZZ.*").mock(
        return_value=httpx.Response(200, content=NOT_FOUND)
    )
    respx.get(url__regex=r".*/api/v2/documents/S100Y9NX.*").mock(
        return_value=httpx.Response(200, content=GATEWAY_401)
    )
    absent = edinet.document("S100ZZZZ")
    assert absent.status == 404
    assert "secret-key" not in absent.document.url
    assert absent.document.url.endswith("Subscription-Key=REDACTED")
    with pytest.raises(edinet.EdinetRefused) as refused:
        edinet.document("S100Y9NX")
    assert refused.value.status == 401
    assert "secret-key" not in str(refused.value)


# ── The code list ──────────────────────────────────────────────────────────────────────────────

CODE_LIST_HEADER = (
    "ＥＤＩＮＥＴコード,提出者種別,上場区分,連結の有無,資本金,決算日,提出者名,提出者名（英字）,"
    "提出者名（ヨミ）,所在地,提出者業種,証券コード,提出者法人番号"
)


def code_list_zip(*rows: str) -> bytes:
    text = "\r\n".join(["ダウンロード実行日,2026年11月27日現在,件数,4件", CODE_LIST_HEADER, *rows])
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("EdinetcodeDlInfo.csv", text.encode("cp932"))
    return buf.getvalue()


def test_the_code_list_maps_a_local_code_to_its_filer() -> None:
    """The securities code is the local code plus a check digit, and the local code can hold a
    letter (`409A0`). A filer with no securities code is unlisted and skipped."""
    archive = code_list_zip(
        "E02144,内国法人・組合,上場,有,635401,3月31日,トヨタ自動車株式会社,TOYOTA,ト,愛知,輸送用機器,72030,1180301018771",
        "E39999,内国法人・組合,上場,有,1,3月31日,新規上場株式会社,NEW,シ,東京,情報・通信業,409A0,1",
        "E00001,内国法人・組合,非上場,無,1,3月31日,非上場株式会社,PRIVATE,ヒ,東京,サービス業,,2",
    )
    found = fe.code_list(archive)
    assert set(found) == {"7203", "409A"}
    assert found["7203"] == fe.Filer("E02144", "7203", "トヨタ自動車株式会社")


def test_a_code_list_with_another_header_is_refused() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("EdinetcodeDlInfo.csv", "a,b\r\nc,d\r\n1,2".encode("cp932"))
    with pytest.raises(ValueError, match="unknown header"):
        fe.code_list(buf.getvalue())


# ── A day's list ───────────────────────────────────────────────────────────────────────────────


def test_a_day_s_list_names_only_the_annual_report_as_annual() -> None:
    day = fe.parse_day((FIXTURES / "edinet_day_2026-06-25.json").read_bytes())
    assert (day.status, day.count, len(day.listed)) == (200, 8, 8)
    annual = [d for d in day.listed if d.is_annual and not d.withdrawn]
    assert [(d.doc_id, d.edinet_code) for d in annual] == [("S100Y9NX", "E02367")]
    nintendo = annual[0]
    assert (nintendo.period_start, nintendo.period_end, nintendo.xbrl) == (
        "2025-04-01",
        "2026-03-31",
        True,
    )
    assert str(nintendo.filed) == "2026-06-25"
    # An amendment (130), a fund's report (ordinance 030) and a withdrawal are listed, not annual.
    by_id = {d.doc_id: d for d in day.listed}
    assert not by_id["S100YK3Q"].is_annual
    assert not by_id["S100YA3V"].is_annual
    assert by_id["S100YK9S"].withdrawn


def test_a_day_outside_the_window_states_404() -> None:
    old = b'{"metadata": {"title": "x", "status": "404", "message": "Not Found"}}'
    assert fe.parse_day(old) == fe.DayList(404, 0, ())


# ── An archive ─────────────────────────────────────────────────────────────────────────────────


def archive(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, body in entries.items():
            z.writestr(name, body)
    return buf.getvalue()


def test_the_report_s_instance_is_the_jpcrp_one_and_never_the_auditor_s() -> None:
    zipped = archive(
        {
            "XBRL/PublicDoc/jpaud-aar-cn-001_E02367-000_2026-03-31_01.xbrl": b"auditor",
            "XBRL/PublicDoc/jpcrp030000-asr-001_E02367-000_2026-03-31_01.xbrl": b"report",
        }
    )
    assert fe.instance_of(zipped) == b"report"
    assert fe.instance_of(archive({"XBRL/PublicDoc/readme.txt": b"x"})) is None


def test_an_instance_over_the_gate_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("XBRL/PublicDoc/jpcrp030000-asr.xbrl", b"x" * 100)
    monkeypatch.setattr(fe, "MAX_INSTANCE_BYTES", 10)
    with pytest.raises(fe.TooLarge):
        fe.instance_of(buf.getvalue())


# ── A report read ──────────────────────────────────────────────────────────────────────────────


def test_yellow_hat_s_segments_and_none_of_its_goodwill_tables() -> None:
    r = fe.read_report(instance("yellowhat-S100YC2P"), edinet_rules.vocabulary())
    assert r.block == "jpcrp_cor:NotesSegmentInformationEtcConsolidatedFinancialStatementsTextBlock"
    assert not r.nonconsolidated
    assert split(r, "報告セグメント", "2026-03-31") == {
        "カー用品・二輪用品等販売事業": 165_486e6,
        "賃貸不動産事業": 5_793e6,
        "その他": 0.0,
    }
    # Four goodwill and impairment tables under headings naming the segments are skipped.
    assert (r.note.tables, r.note.read, r.note.skipped_kind) == (6, 2, 4)
    assert not [f for f in r.note.facts if "償却" in f.member_code or "残高" in f.member_code]


def test_makita_s_regions_as_segments_beside_its_regional_table() -> None:
    """Makita's reportable segments ARE regions; its regional table states revenue by customer
    location, a different split of the same 777,600 百万円. Both are read, each on its own axis."""
    r = fe.read_report(instance("makita-S100YH4M"), edinet_rules.vocabulary())
    assert split(r, "報告セグメント", "2026-03-31") == {
        "日本": 151_458e6,
        "欧州": 393_338e6,
        "北米": 81_633e6,
        "アジア": 34_333e6,
        "その他": 116_838e6,
    }
    assert split(r, "地域", "2026-03-31") == {
        "日本": 132_136e6,
        "欧州": 390_110e6,
        "北米": 78_662e6,
        "アジア": 47_730e6,
        "その他": 128_963e6,
    }
    assert sum(split(r, "製品及びサービス", "2026-03-31").values()) == 777_600e6
    # Profit does not reconcile (adjustments), so it takes revenue's split unreconciled.
    profit = [f for f in r.note.facts if f.metric_code == "operating_income"]
    assert profit and all(f.partition_id == 1 and f.reconciled_to is None for f in profit)


def test_a_company_without_subsidiaries_anchors_on_its_own_figures() -> None:
    r = fe.read_report(instance("kanesou-S100YGWS"), edinet_rules.vocabulary())
    assert r.nonconsolidated
    assert split(r, "製品及びサービス", "2026-03-31") == {
        "鋳鉄器材": 2_323_377e3,
        "スチール機材": 1_173_437e3,
        "製作金物": 4_292_511e3,
        "その他": 583_827e3,
    }


def test_a_consolidated_note_never_anchors_on_a_non_consolidated_figure() -> None:
    """Kanesou's block relabelled consolidated: its figures are all non-consolidated, so nothing
    anchors and nothing is read."""
    x = instance("kanesou-S100YGWS").replace(
        "NotesSegmentInformationEtcFinancialStatementsTextBlock",
        "NotesSegmentInformationEtcConsolidatedFinancialStatementsTextBlock",
    )
    r = fe.read_report(x, edinet_rules.vocabulary())
    assert not r.nonconsolidated and r.note.facts == []


def test_a_report_with_no_note_reads_nothing() -> None:
    r = fe.read_report('<xbrli:xbrl xmlns:xbrli="x"></xbrli:xbrl>', edinet_rules.vocabulary())
    assert (r.block, r.note.facts) == (None, [])


# ── The vocabulary ─────────────────────────────────────────────────────────────────────────────


def test_an_empty_vocabulary_is_refused() -> None:
    with pytest.raises(segments.VocabularyEmpty):
        fe.build_vocabulary([("heading", "x", "business")], edinet_rules.AXES, 1)


def test_two_axes_for_one_kind_are_refused() -> None:
    rules = [(r, p, role) for r, p, role, _ in edinet_rules.RULES]
    with pytest.raises(ValueError, match="two edinet axes"):
        fe.build_vocabulary(rules, [*edinet_rules.AXES, ("business", "edinet:other")], 1)


def test_the_vocabulary_reads_its_rules_in_priority_order() -> None:
    """`RULES` orders a rule's rows by priority, highest first, and a label takes the first pattern
    it matches: capital expenditure must be tried before the bare 有形固定資産."""
    labels = [label.role for label in edinet_rules.vocabulary().note.labels]
    assert labels.index("capital_expenditure") < labels.index("long_lived_assets")
    assert json.dumps(fe.RULES).count("priority desc") == 1


# ── The catalogue, the queries and raw ─────────────────────────────────────────────────────────


def _answer(body: bytes, status: int = 200, too_large: bool = False) -> edinet.Answer:
    from datetime import UTC, datetime

    from muffin_ingest.providers.documents import Document

    return edinet.Answer(
        Document("http://http-cache:8080/edinet/x?Subscription-Key=REDACTED", body, "x",
                 datetime(2026, 11, 27, tzinfo=UTC)),
        status,
        too_large,
    )  # fmt: skip


def _listed(**given: object) -> fe.Listed:
    base: dict[str, object] = {
        "doc_id": "S1", "edinet_code": "E1", "doc_type": "120", "ordinance": "010",
        "form": "030000", "period_start": "2025-04-01", "period_end": "2026-03-31",
        "submitted": "2026-06-25 15:00", "withdrawn": False, "xbrl": True,
    }  # fmt: skip
    return fe.Listed(**{**base, **given})  # type: ignore[arg-type]


def test_a_day_s_catalogue_keeps_reports_and_amendments_and_names_withdrawals() -> None:
    """Annual reports of every ordinance and their amendments are kept, as EDINET lists them (the
    filing function picks the companies' reports); a withdrawal names its document by id alone."""
    day = fe.parse_day((FIXTURES / "edinet_day_2026-06-25.json").read_bytes())
    found = fe.document_rows(day.listed, listed_on="2026-06-25")
    kinds = sorted((r["doc_type_code"], r["ordinance_code"], r["form_code"]) for r in found.rows)
    assert kinds == [
        ("120", "010", "030000"), ("120", "030", "07A000"), ("120", "030", "07C000"),
        ("130", "010", "030001"),
    ]  # fmt: skip
    assert (found.withdrawn, found.incomplete) == (["S100YK9S"], 0)
    amendment = next(r for r in found.rows if r["doc_type_code"] == "130")
    assert (amendment["doc_id"], amendment["parent_doc_id"], amendment["sec_code"]) == (
        "S100YK3Q", "S100YGKQ", "21140",
    )  # fmt: skip
    assert amendment["filed_on"] == "2026-06-25"
    assert all(r["listed_on"] == "2026-06-25" and r["source_code"] == "edinet" for r in found.rows)


def test_an_entry_without_a_filer_or_a_day_is_counted_and_not_kept() -> None:
    found = fe.document_rows(
        [_listed(edinet_code=""), _listed(doc_id="S2", submitted=None), _listed(doc_type="350")],
        listed_on="2026-06-25",
    )
    assert (found.rows, found.withdrawn, found.incomplete) == ([], [], 2)


def test_a_withdrawn_entry_that_keeps_its_filer_is_a_row_marked_withdrawn() -> None:
    found = fe.document_rows([_listed(withdrawn=True)], listed_on="2026-06-25")
    assert [r["withdrawn"] for r in found.rows] == [True] and found.withdrawn == []


def test_an_archive_s_outcome_is_read_from_its_body() -> None:
    """EDINET answers HTTP 200 for everything, so the outcome is the body's: a ZIP, its 404, or
    something else, which is asked again rather than taken for an absence."""
    assert fe.archive_outcome(_answer(b"PK\x03\x04")) == "answered"
    assert fe.archive_outcome(_answer(b'{"metadata": {"status": "404"}}', 404)) == "absent"
    assert fe.archive_outcome(_answer(b"", too_large=True)) == "too_large"
    assert fe.archive_outcome(_answer(b'{"metadata": {"status": "200"}}')) == "not_zip"
    assert fe.day_outcome(_answer(b"{}", 404)) == "absent"
    assert fe.day_outcome(_answer(b"{}")) == "answered"


def test_the_newest_finished_fetch_is_held_and_only_an_answered_one_has_a_body() -> None:
    rows = [
        {"document": "archive", "accession": "S1", "outcome": "not_zip",
         "fetched_at": "2026-11-27T02:00", "run_id": "r2", "body": b"junk"},
        {"document": "archive", "accession": "S1", "outcome": "answered",
         "fetched_at": "2026-11-27T01:00", "run_id": "r1", "body": b"PK1"},
        {"document": "archive", "accession": "S2", "outcome": "absent",
         "fetched_at": "2026-11-27T03:00", "run_id": "r3", "body": b"{}"},
    ]  # fmt: skip
    held = fe.held_archive(rows, "S1")
    assert held == fe.Held("not_zip", "2026-11-27T02:00", "r2")
    assert fe.archive_body(rows, held) is None
    first = fe.Held("answered", "2026-11-27T01:00", "r1")
    assert fe.archive_body(rows, first) == b"PK1"
    assert fe.held_archive(rows, "S3") is None


def test_a_held_day_is_its_newest_answered_list_and_never_an_absence() -> None:
    rows = [
        {"document": "day", "outcome": "answered", "fetched_at": "2026-11-20", "body": b"old"},
        {"document": "day", "outcome": "answered", "fetched_at": "2026-11-21", "body": b"new"},
        {"document": "day", "outcome": "absent", "fetched_at": "2026-11-22", "body": b"{}"},
    ]
    held = fe.held_day(rows)
    assert held is not None and held["body"] == b"new"
    assert fe.held_day(rows[2:]) is None


class _Cursor:
    def __init__(self, sent: list[tuple[str, tuple[object, ...]]]) -> None:
        self.sent = sent

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self.sent.append((sql, params))

    def fetchone(self) -> tuple[int, int, int]:
        return (3, 1, 2)


class _Conn:
    def __init__(self) -> None:
        self.sent: list[tuple[str, tuple[object, ...]]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.sent)


def test_filing_none_asks_for_every_filer_and_an_empty_list_asks_nothing() -> None:
    """NULL files every mapped filer's reports; an empty list must not become NULL, which would
    turn "nothing changed today" into a pass over the whole catalogue."""
    conn = _Conn()
    assert fe.file_filings(conn, []) == fe.Filed(0, 0, 0) and conn.sent == []
    assert fe.file_filings(conn, None) == fe.Filed(3, 1, 2)
    assert fe.file_filings(conn, ["E1"]) == fe.Filed(3, 1, 2)
    assert [p for _, p in conn.sent] == [(None,), (["E1"],)]


def test_the_queries_read_the_regulator_s_own_parser_version_and_form() -> None:
    for sql in (fe.FILINGS, fe.DUE_BY_FILER, fe.UNREAD):
        assert "p.regulator = 'edinet'" in sql and "'有価証券報告書'" in sql
    assert "source_code = 'edinet'" in fe.FILERS
