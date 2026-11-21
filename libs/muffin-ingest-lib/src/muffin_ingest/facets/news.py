"""Company news: Yahoo's RSS documents read into `market.news_article` and `market.news_security`.

Stage 2 of the news lane (Phase 6b, umbrella docs/specs/2026-11-21-macro-and-events.md). No
provider call: it reads the documents `raw_yahoo_news` stored.

WHAT A DOCUMENT CAN SAY, measured on the captures of 2026-11-21 (`tests/fixtures/news`):

  * ANSWERED: a channel naming the asked symbol, with items (AAPL 18, SAP.DE 18, 7203.T 16,
    PTSB.IR 1);
  * EMPTY: a channel naming the symbol, with no items;
  * UNKNOWN: Yahoo did not take the symbol. The channel is present, its description reads
    "Latest Financial News for " with the symbol blanked, and it has no items (ZZQQXX). Counted, and
    never recorded as a miss: whether a symbol is dead is the price lane's evidence, not a feed's;
  * UNREADABLE: not an RSS document at all.

AN ARTICLE IS STORED ONCE, keyed on its URL, because one story is returned for several companies;
each company it came back for gets a link. The link is Yahoo's attribution, not ours
(`news_security`'s comment).

THE URL DROPS ONE PARAMETER. The feed appends `.tsrc=rss` to every link, and none of the 21,673
URLs the edge stored carries it, so keeping it would store a story the edge already holds a
second time. Nothing else in the URL is touched.

NOTHING OLDER THAN THE RETENTION WINDOW IS WRITTEN, so re-parsing a stored document can never
bring back an article `market.prune_news` has deleted. The window is the function's default.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

#: The source the links are written under: the feed, not the publisher.
SOURCE = "yahoo-rss"

#: `market.prune_news`'s default window. An item older than this is not written.
RETENTION_DAYS = 90

#: What one stored document can be, in the order stage 2 counts them.
OUTCOMES = ("answered", "empty", "unknown", "unreadable")

#: The parameter the feed adds to every link (`.tsrc=rss`), which the stored URLs never carry.
TRACKING_PARAMETER = ".tsrc"

_PREFIX = "Latest Financial News for"


@dataclass(frozen=True)
class Item:
    """One headline, as the feed states it."""

    url: str
    published_at: datetime
    title: str
    summary: str | None


@dataclass(frozen=True)
class Read:
    """What one document was, and its items when it answered."""

    outcome: str
    items: tuple[Item, ...] = ()
    #: Items left out, by reason: no link, no title, or an unreadable date.
    skipped: int = 0


def article_url(link: str) -> str:
    """The link with the feed's tracking parameter removed and every other byte as the feed sent it.

    TEXTUAL, NOT DECODED AND RE-ENCODED: `urlencode` would rewrite an encoded value (`%2F`, a space
    as `+`), and a URL that differs by one escape stores the same story twice.
    """
    link = link.strip()
    base, sep, query = link.partition("?")
    if not sep:
        return link
    query, hash_sep, fragment = query.partition("#")
    kept = [p for p in query.split("&") if p.split("=", 1)[0] != TRACKING_PARAMETER]
    rebuilt = base + ("?" + "&".join(kept) if any(kept) else "")
    return rebuilt + (hash_sep + fragment if hash_sep else "")


def _text(element: ET.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    return element.text.strip() or None


def read(body: bytes) -> Read:
    """One stored document, classified, with its items when the feed answered."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return Read("unreadable")
    channel = root.find("channel")
    if root.tag != "rss" or channel is None:
        return Read("unreadable")
    description = _text(channel.find("description")) or ""
    named = description.removeprefix(_PREFIX).strip() if description.startswith(_PREFIX) else None
    raw_items = channel.findall("item")
    if not raw_items:
        # A BLANKED SYMBOL IS YAHOO DECLINING THE NAME. Measured on ZZQQXX: the description ends at
        # "for ", and the image title reads "Yahoo! Finance:  News" with two spaces.
        return Read("unknown" if named == "" else "empty")

    items: list[Item] = []
    skipped = 0
    for raw in raw_items:
        link, title, stamp = (
            _text(raw.find("link")),
            _text(raw.find("title")),
            _text(raw.find("pubDate")),
        )
        if link is None or title is None or stamp is None:
            skipped += 1
            continue
        try:
            published = parsedate_to_datetime(stamp)
        except (TypeError, ValueError):
            skipped += 1
            continue
        if published.tzinfo is None:
            # RFC 822 says `-0000` means "no zone stated": read as UTC, never as a local zone.
            published = published.replace(tzinfo=UTC)
        items.append(
            Item(
                url=article_url(link),
                published_at=published.astimezone(UTC),
                title=title,
                summary=_text(raw.find("description")),
            )
        )
    return Read("answered", tuple(items), skipped)


@dataclass
class Normalised:
    #: Rows for `market.news_article`, one per URL.
    articles: list[dict[str, Any]] = field(default_factory=list)
    #: Rows for `market.news_security`.
    links: list[dict[str, Any]] = field(default_factory=list)
    #: Each company's newest document's outcome.
    outcomes: dict[str, str] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=dict)


def _fetch_order(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("fetched_at") or ""), str(row.get("url") or ""))


def normalise(rows: Sequence[Mapping[str, Any]], *, now: datetime) -> Normalised:
    """Stored documents to articles and links, from each company's newest document.

    An article returned for several companies is written once, from the newest document that
    carries it, and linked to each of them.
    """
    stats = {f"documents_{o}": 0 for o in OUTCOMES}
    stats.update(
        {
            "documents": 0,
            "legacy_rows": 0,
            "items": 0,
            "items_skipped": 0,
            "items_too_old": 0,
            "articles": 0,
            "links": 0,
        }
    )
    newest: dict[str, Mapping[str, Any]] = {}
    for row in sorted(rows, key=_fetch_order):
        sid = row.get("security_id")
        if sid is None:
            continue
        if row.get("body") is None:
            stats["legacy_rows"] += 1
            continue
        newest[str(sid)] = row

    cutoff = now - timedelta(days=RETENTION_DAYS)
    out = Normalised(stats=stats)
    articles: dict[str, tuple[str, dict[str, Any]]] = {}
    links: set[tuple[str, str]] = set()
    for sid, row in sorted(newest.items()):
        stats["documents"] += 1
        result = read(bytes(row["body"]))
        out.outcomes[sid] = result.outcome
        stats[f"documents_{result.outcome}"] += 1
        stats["items_skipped"] += result.skipped
        fetched = str(row.get("fetched_at") or "")
        for item in result.items:
            stats["items"] += 1
            if item.published_at < cutoff:
                stats["items_too_old"] += 1
                continue
            links.add((item.url, sid))
            held = articles.get(item.url)
            if held is None or fetched >= held[0]:
                articles[item.url] = (
                    fetched,
                    {
                        "url": item.url,
                        "published_at": item.published_at,
                        "title": item.title,
                        "summary": item.summary,
                    },
                )
    out.articles = [article for _, article in sorted(articles.values(), key=lambda a: a[1]["url"])]
    out.links = [
        {"url": url, "security_id": sid, "source_code": SOURCE} for url, sid in sorted(links)
    ]
    stats["articles"] = len(out.articles)
    stats["links"] = len(out.links)
    return out
