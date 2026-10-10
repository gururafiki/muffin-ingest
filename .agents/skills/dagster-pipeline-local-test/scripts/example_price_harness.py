"""A template harness for `branch_on_node.sh`: the price lane's two stages over real securities.

Copy it, change `SYMBOLS` and the assets, keep the shape. It asks the provider for real (one chart
request per security, through http-cache), reads production's subjects and control tables, and
writes nothing: the core rows land in `readonly.CAPTURED` and are compared with the table.
"""

import json
import tempfile

import dagster as dg
from dagster._core.storage.tags import (
    ASSET_PARTITION_RANGE_END_TAG,
    ASSET_PARTITION_RANGE_START_TAG,
)
from muffin_ingest.facets import prices
from muffin_ingest_dagster.defs.prices import core, raw
from muffin_ingest_dagster.lib.io_managers import ParquetIOManager, RawStore
from readonly import CAPTURED, SKIPPED, Capture, SafePostgres

SYMBOLS = ["ALG.KW"]

with SafePostgres().connect() as conn:
    keys = [s.security_id for s in prices.askable_subjects(conn) if s.symbol in SYMBOLS]

tmp = tempfile.mkdtemp()
resources = {
    "postgres": SafePostgres(),
    "postgres_io": Capture(),
    "parquet_io": ParquetIOManager(tmp),
    "raw_store": RawStore(base_path=tmp),
}
with dg.instance_for_test() as instance:
    instance.add_dynamic_partitions("security", keys)
    run = dg.materialize(
        [raw.raw_price_chart, core.price_bar_history],
        instance=instance,
        partition_key=keys[0] if len(keys) == 1 else None,
        tags=None
        if len(keys) == 1
        else {ASSET_PARTITION_RANGE_START_TAG: keys[0], ASSET_PARTITION_RANGE_END_TAG: keys[-1]},
        resources=resources,
    )
    for node in ("raw_price_chart", "price_bar_history"):
        event = run.asset_materializations_for_node(node)[0]
        print(node, json.dumps({k: v.value for k, v in event.metadata.items() if k != "path"}))

print("captured rows", len(CAPTURED))
print("intercepted writes", sorted(set(SKIPPED)))
