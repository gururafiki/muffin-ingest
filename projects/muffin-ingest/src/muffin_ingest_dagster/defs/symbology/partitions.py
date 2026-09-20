"""Partitions definitions and constants the symbology family shares."""

import dagster as dg

#: THE LADDER'S OWN GRID, AND IT IS DELIBERATELY NOT THE PRICE LANE'S.
#:
#: These rungs used to import `prices.partitions.security_partitions`, so one dynamic partition set
#: served two families. Dagster's partition grid is not an index — it is the STATE that says which
#: securities are collected, the state `nightly_prices` walks and
#: `no_security_is_far_behind_the_sweep` reads, and the state the 2026-09-19 design stopped pruning
#: the event log in order to keep. Sharing it did two things, neither of which had happened only
#: because this lane's sensor has never been started:
#:
#: * `new_symbols_needed` re-asked a stale miss by DELETING the security's partition so it would
#:   look new. That delete lands on the price lane's record too: the bars in Postgres are untouched
#:   and the fact that we collected them is gone, so the sweep re-does the security and the
#:   far-behind check goes blind for it.
#: * the ladder's population is not the price lane's. Seeding it would have added thousands of
#:   subjects the price lane has no reason to ask about, and `nightly_prices` slices the grid by
#:   count — so the nightly sweep would have spent its slots on them and a full pass would have
#:   taken twice as many nights.
#:
#: A security resolved here therefore no longer joins the price grid as a side effect. That path is
#: not lost: `new_securities_need_history` seeds it from `prices.askable_subjects`, which is the
#: price lane deciding its own population, which is the point.
SYMBOLOGY_PARTITIONS = "symbology_subject"


symbology_subjects = dg.DynamicPartitionsDefinition(name=SYMBOLOGY_PARTITIONS)


#: How many securities one run may cover. The mapping rungs batch TEN JOBS PER REQUEST, so 200
#: partitions cost 20 provider requests — the shape the skill names as the model for a bulk API,
#: and the reason this is not coarsened into one partition per batch.
SYMBOLOGY_PER_RUN = 200


#: When a security whose probe said `miss` is asked again. Not never — a security can gain a US
#: listing, and Yahoo's index is inconsistent enough that a name it lacks today may appear next
#: quarter. Not soon either: re-asking a known absence every run is how a rate-limited provider
#: gets spent on answers already written down.
REASK_AFTER_DAYS = 30


#: When the re-ask is even CONSIDERED. The automation daemon ticks every 30 seconds; without this
#: the condition would query `identifier_probe` ~2,880 times a day per rung to answer a question
#: whose input moves once a day. Gating on a cron tick means the condition is handed an empty
#: candidate subset on every other tick and returns without touching the database.
REASK_CRON = "0 3 * * *"
