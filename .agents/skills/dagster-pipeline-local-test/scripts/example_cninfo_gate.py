"""A reader gate's harness: read every CNINFO PDF under /docs with one library, one line each.

Run by gate_reader.sh, twice: `python /gate.py base` on the running image's library and
`python /gate.py new` with the branch's first on PYTHONPATH. Copy it for another reader: change the
glob and the call, keep the output shape (doc, status, facts sorted, period).
"""

import glob
import json
import sys
import time

from muffin_ingest.facets import cn_pdf

label = sys.argv[1]
paths = sorted(p for p in glob.glob("/docs/*/*") if not p.endswith(".url"))
with open(f"/work/gate_{label}.jsonl", "w", encoding="utf-8") as out:
    for path in paths:
        with open(path, "rb") as handle:
            body = handle.read()
        row: dict[str, object] = {"doc": path}
        try:
            started = time.time()
            read = cn_pdf.segment_facts_from_pdf(body)
            row.update(
                status="read",
                ms=int(1000 * (time.time() - started)),
                page=read.page,
                period=read.period_ending,
                facts=sorted(
                    [f.axis, f.member_code, f.metric_code, f.value, f.partition_id,
                     f.reconciled_to, f.parent_member]
                    for f in read.facts
                ),
            )
        except Exception as error:  # a reader that raises is a result, recorded per document
            row.update(status="error", error=f"{type(error).__name__}: {error}"[:300])
        out.write(json.dumps(row, ensure_ascii=False) + "\n")
print(label, "done", len(paths), flush=True)
