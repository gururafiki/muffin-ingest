"""EDINET's `market.segment_note_rule` rows as muffin-deployment seeds them, for the tests.

KEEP IN STEP WITH THE MIGRATION that seeds them (muffin-deployment,
`…_japan_s_segment_notes_are_read_by_rule.sql`): the lane reads the table, and these tests read
this copy. Each row is (rule, pattern, role, priority); within a rule the highest priority is tried
first, and a label takes the first pattern it matches.
"""

from muffin_ingest.facets import edinet

RULES: tuple[tuple[str, str, str, int], ...] = (
    # The note's block: consolidated where the company has subsidiaries, its own otherwise.
    (
        "block",
        r"^NotesSegmentInformation(Etc)?ConsolidatedFinancialStatements(IFRS|USGAAP)?TextBlock$",
        "consolidated",
        20,
    ),
    (
        "block",
        r"^NotesSegmentInformation(Etc)?FinancialStatementsTextBlock$",
        "nonconsolidated",
        10,
    ),
    # Headings: the kind of the tables beneath. Within one heading line a `skip` wins.
    ("heading", r"主要な顧客", "skip", 60),
    ("heading", r"差異|差額", "skip", 60),
    ("heading", r"減損損失|のれん", "skip", 60),
    ("heading", r"報告セグメント", "business", 30),
    ("heading", r"製品及びサービス", "product", 20),
    ("heading", r"地域", "geography", 10),
    # Labels: structure first, then the metrics, capital expenditure before long-lived assets.
    ("label", r"^うち", "subset", 130),
    (
        "label",
        r"顧客との契約から生じる収益|^その他の収益|一時点で移転|一定の期間にわたり移転",
        "skip",
        125,
    ),
    ("label", r"セグメント間", "skip", 120),
    ("label", r"小計", "subtotal", 115),
    ("label", r"^\(?(連結|合計|総計|計)\)?$|計上額$|^報告セグメント計$", "total", 110),
    ("label", r"消去|調整|全社", "adjustment", 105),
    (
        "label",
        r"外部顧客(への|に対する)(売上高|営業収益|売上収益)|^(売上高|営業収益|売上収益|外部収益)$",
        "revenue",
        100,
    ),
    ("label", r"粗利益", "skip", 95),
    (
        "label",
        r"^(セグメント利益|営業利益|セグメント利益又は損失|営業損益)",
        "operating_income",
        90,
    ),
    ("label", r"^(セグメント資産|資産合計)$", "total_assets", 85),
    ("label", r"減価償却費", "depreciation", 80),
    ("label", r"有形固定資産及び無形固定資産の増加額|^資本的支出$", "capital_expenditure", 75),
    ("label", r"有形固定資産|非流動資産", "long_lived_assets", 70),
    # A line with no label of its own earns a metric from the concept its members sum to.
    (
        "concept",
        r"^(?!.*(Gain|Loss|Proceeds|Cost|Receivable|Payable|Expense|Allowance|Tax|Deferred|Unearned))"
        r".*(NetSales|Revenue)",
        "revenue",
        10,
    ),
)

AXES = (
    ("business", "edinet:報告セグメント"),
    ("geography", "edinet:地域"),
    ("product", "edinet:製品及びサービス"),
)


def vocabulary(parser_version: int = 1) -> edinet.Vocabulary:
    """The vocabulary built as `facets.edinet.vocabulary` builds it, from these rows."""
    ordered = sorted(RULES, key=lambda r: (r[0], -r[3], r[1]))
    return edinet.build_vocabulary(
        [(r, p, role) for r, p, role, _ in ordered], AXES, parser_version
    )
