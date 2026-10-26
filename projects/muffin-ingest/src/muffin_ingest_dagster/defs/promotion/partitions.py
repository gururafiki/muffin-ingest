"""Partitions of the promotion family: one per weekly wave."""

import dagster as dg

#: THE WAVE'S SLOT, Monday 06:17 UTC: after the company lane (03:30 to ~05:50), the absence mark
#: (05:41) and the listing and symbol-map floors (05:43, 05:47), and before the SEC lanes (11:00).
#: Decided by the user 2026-10-25: a wave a week, checked each week against the price night and
#: the company lane.
WAVE_CRON = "17 6 * * 1"

#: ONE PARTITION PER WAVE, keyed by the Monday it runs. `end_offset=1` makes a week its own
#: partition from the minute it starts, so the schedule runs the wave at the start of its week;
#: without it a week's partition exists only once the week has ended.
wave_weeks = dg.WeeklyPartitionsDefinition(
    start_date="2026-10-26",
    day_offset=1,
    hour_offset=6,
    minute_offset=17,
    end_offset=1,
)

#: HOW MANY CANDIDATE LINES A WAVE ASKS YAHOO ABOUT FOR EACH PLACE IN IT. About a quarter of the
#: lines measured on 2026-10-26 were not companies, and a line Yahoo cannot price is not asked again
#: for 90 days, so twice the places fills a wave with room to spare. A wave of 500 asks ~1,000, at
#: the chart lane's 1.4 s: ~24 minutes, against the ~2,500 requests of a price night.
ASK_PER_PLACE = 2
