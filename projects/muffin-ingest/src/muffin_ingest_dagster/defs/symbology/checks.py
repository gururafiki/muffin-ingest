"""Asset checks of the symbology family."""

import dagster as dg
from muffin_ingest.facets import symbology as sym

from muffin_ingest_dagster.defs.symbology.core import security_symbology
from muffin_ingest_dagster.lib.resources import Postgres

#: A class the provider named for one security while another holds it. Read from the PROBES,
#: because the refused adoption leaves no other trace: `security_identifier` holds the class once,
#: under its first holder, and the second security simply has none.
SHARE_CLASS_HELD_BY_ANOTHER = """
select p.security_id::text, p.value, i.security_id::text
  from market.identifier_probe p
  join market.security_identifier i on i.kind_code = %s and i.value = p.value
 where p.scheme = %s and p.outcome = 'hit' and i.security_id <> p.security_id
 order by p.value, p.security_id
"""


@dg.asset_check(asset=security_symbology, blocking=False)
def one_security_per_share_class(postgres: Postgres) -> dg.AssetCheckResult:
    """Does any share class belong to two of our securities?

    THE KEY MAKES IT IMPOSSIBLE TO STORE AND EASY TO HIDE. `security_identifier` is keyed
    `(kind_code, value)`, so a second security naming a held class is refused — correctly, and
    without a trace in any table but the probe. That refusal is an identity fact: the universe
    holds one company twice (Worldline's two ISINs, FR0011981968 and FR00140182K6, are the known
    case), and every downstream lane then collects it twice, prices it twice, and counts it twice.

    WARN AND NAMED, because resolving it is identity consolidation — choosing the survivor and
    re-pointing its holdings — which is a decision, not a retry. The names are the worklist.
    """
    with postgres.connect() as conn, conn.cursor() as cur:
        cur.execute(SHARE_CLASS_HELD_BY_ANOTHER, (sym.SHARE_CLASS_KIND, sym.SHARE_CLASS_KIND))
        pairs = cur.fetchall()
    return dg.AssetCheckResult(
        passed=not pairs,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "duplicates": len(pairs),
            "first": "; ".join(f"{v}: {sid} (held by {holder})" for sid, v, holder in pairs[:10])
            or "none",
            "note": "two securities naming one share class hold one company twice; the holder "
            "kept the class, and choosing the survivor is identity consolidation",
        },
    )
