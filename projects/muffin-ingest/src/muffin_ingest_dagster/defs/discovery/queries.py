"""The questions the venue directory asks OpenFIGI — one `exchange_sweep` partition each.

A `/v3/filter` query is `(exchCode, securityType2)`: it has its own `total`, its own 15,000-result
cap, its own cursor and its own completeness, so it is the unit the provider is asked about and
therefore the partition (the umbrella's docs/specs/2026-10-04-the-venue-directory-asks-by-query.md).
The list lives in `market.directory_query`, a view over two control tables: every enabled venue x
every enabled type (`US.common`, `US.reit`, `US.dr`, `US.partnership`), plus aliases asked of one
code and filed under another venue (`US.arca` asks NYSE Arca, files under US). Adding a type or a
split is a row there, never a release.

REFUSED WHEN AMBIGUOUS. The raw files route each page to its partition by the request it records
(`exch_code`, `security_type2`), and the sensor seeds one key per row. Two rows with one key would
share a cursor, and two keys asking one question would walk it twice and route its pages to
whichever came last. Both are errors in the control data, so reading them fails loudly instead of
seeding a grid that cannot be trusted.
"""

from dataclasses import dataclass
from typing import Any


class DirectoryQueryAmbiguous(RuntimeError):
    """`market.directory_query` lists a key twice, or one question under two keys."""


class DirectoryQueryUnknown(KeyError):
    """A partition key that `market.directory_query` no longer lists."""


@dataclass(frozen=True)
class DirectoryQuery:
    """One question: what to ask OpenFIGI, and which venue its lines are filed under."""

    key: str
    exch_code_asked: str
    files_under: str
    security_type2: str
    #: The lines are a LOCAL exchange's, each filed as the composite line it names.
    maps_to_composite: bool

    @property
    def request(self) -> tuple[str, str]:
        """What the provider is asked — and what each raw page records about itself."""
        return (self.exch_code_asked, self.security_type2)


def directory_queries(conn: Any) -> dict[str, DirectoryQuery]:
    """Every enabled question, by partition key."""
    with conn.cursor() as cur:
        cur.execute(
            "select query_key, exch_code_asked, files_under, security_type2, maps_to_composite "
            "from market.directory_query"
        )
        rows = cur.fetchall()
    out: dict[str, DirectoryQuery] = {}
    asked: dict[tuple[str, str], str] = {}
    for key, exch_code_asked, files_under, security_type2, maps_to_composite in rows:
        query = DirectoryQuery(
            key=str(key),
            exch_code_asked=str(exch_code_asked),
            files_under=str(files_under),
            security_type2=str(security_type2),
            maps_to_composite=bool(maps_to_composite),
        )
        if query.key in out:
            raise DirectoryQueryAmbiguous(f"market.directory_query lists {query.key} twice")
        if query.request in asked:
            raise DirectoryQueryAmbiguous(
                f"{query.key} and {asked[query.request]} ask OpenFIGI the same question "
                f"{query.request}; one of them must go"
            )
        out[query.key] = query
        asked[query.request] = query.key
    return out


def query_for(queries: dict[str, DirectoryQuery], key: str) -> DirectoryQuery:
    """The question a partition key stands for, or an error naming the remedy.

    A key the view no longer lists is a partition from before the 2026-10-04 re-key (`US`, `LN`),
    or a type or alias since disabled. Walking it would ask a question nobody wants answered, and
    guessing what it meant is how a grid stops meaning anything.
    """
    query = queries.get(key)
    if query is None:
        raise DirectoryQueryUnknown(
            f"exchange_sweep key {key!r} is not in market.directory_query: a partition from "
            "before the 2026-10-04 re-key, or a type or alias since disabled. Delete the key "
            "(instance.delete_dynamic_partition('exchange_sweep', key)); its raw file stays."
        )
    return query
