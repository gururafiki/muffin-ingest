"""CNINFO's `market.segment_note_rule` rows (the segment note) as muffin-deployment seeds them.

KEEP IN STEP WITH THE MIGRATION that seeds them (muffin-deployment,
`…_china_s_segment_note_is_read_by_rule.sql`): the lane reads the table, and these tests read this
copy. Each row is (rule, pattern, role, priority); within a rule the highest priority is tried
first, and a label takes the first pattern it matches. Labels are matched NFKC-normalised, so a
full-width colon is `:` and a bracket `(`.
"""

from muffin_ingest.facets import segment_note as sn

RULES: tuple[tuple[str, str, str, int], ...] = (
    # Headings: the kind of the tables beneath. Only business is read (one axis, `cninfo:经营分部`).
    ("heading", r"主要客户", "skip", 60),
    ("heading", r"地区信息|地理信息|地区分部|区域信息|按地区", "geography", 40),
    ("heading", r"经营分部|业务分部|分部报告|分部信息|报告分部|分部分析", "business", 30),
    # Labels. External revenue under 其中 is still revenue: it is the row a split reconciles to.
    (
        "label",
        r"^其中:?(对外交易收入|来自外部客户的收入|外部客户收入|对外营业收入|外部营业收入)$",
        "revenue",
        140,
    ),
    ("label", r"^其中", "subset", 130),
    ("label", r"分部间|内部", "skip", 120),
    ("label", r"小计$", "subtotal", 115),
    ("label", r"^(合计|总计|合并|集团合计|本集团合计|合并金额)$", "total", 110),
    # An elimination column. `未分配` is a member: SF Holding's carries ¥1,458 m of external
    # revenue, without which its split does not add up.
    (
        "label",
        r"^(分部间|地区间|业务间)?(抵销|抵消)$|^合并抵(销|消)$|^抵(销|消)及调整$|^调整$",
        "adjustment",
        105,
    ),
    (
        "label",
        r"^(对外交易收入|来自外部客户的收入|外部客户收入|对外营业收入|外部营业收入"
        r"|营业收入|营业总收入|营业收入合计|营业总收入合计)$",
        "revenue",
        100,
    ),
    ("label", r"^(分部利润|营业利润|分部经营利润|分部营业利润)", "operating_income", 90),
    # Members naming places: a table of mostly these is a geography table, whatever its heading
    # (Bank of China's `八 分部报告` opens with 境内 / 中银香港集团 / 其他国家和地区).
    (
        "member",
        r"地区$|大湾区$|^(境内|境外|国内|海外|中国内地|中国大陆|中国境内|中国境外)",
        "geography",
        100,
    ),
    ("member", r"香港|澳门|台湾|^总行", "geography", 100),
    (
        "member",
        r"^(华东|华南|华北|华中|西南|西北|东北|长三角|珠三角|京津冀|环渤海|粤港澳|中东部|亚太|欧洲|美洲|北美|南美|非洲|中东)",
        "geography",
        100,
    ),
)


def vocabulary() -> sn.NoteVocabulary:
    """The note vocabulary built as `facets.cninfo.note_vocabulary` builds it, from these rows."""
    ordered = sorted(RULES, key=lambda r: (r[0], -r[3], r[1]))
    return sn.NoteVocabulary(
        headings=tuple(sn.Heading(p, role) for rule, p, role, _ in ordered if rule == "heading"),
        labels=tuple(sn.Label(p, role) for rule, p, role, _ in ordered if rule == "label"),
        members=tuple(sn.Heading(p, role) for rule, p, role, _ in ordered if rule == "member"),
    )
