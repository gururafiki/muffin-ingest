"""The nasdaq lane's daily grid, and what bounds a run."""

from datetime import timedelta

import dagster as dg

#: ONE PARTITION PER DATE, NASDAQ'S OWN GRAIN, from 2012 (the start of the stored history) to 21
#: days ahead. `end_offset=1` is today, so 22 reaches today + 21: the forward window the calendar
#: serves. About 5,450 keys, far under Dagster's 100,000 per asset.
nasdaq_days = dg.DailyPartitionsDefinition(start_date="2012-01-01", timezone="UTC", end_offset=22)

#: The window the daily schedule re-asks: a past week, in which actuals are filled in, and the
#: days ahead, in which dates are added and moved.
WINDOW_PAST_DAYS = 7
WINDOW_AHEAD_DAYS = 21

#: Days a backfill run covers. A day costs one request at ~2.3 s plus the pacing, so 60 days are
#: about four minutes, short of the run limit with room for a slow provider.
DAYS_PER_RUN = 60

#: Spacing between two requests. The validation paced at one second over 50 days with no refusal.
SECONDS_BETWEEN_REQUESTS = 1.0

#: THE SCHEDULE RE-ASKS THE WINDOW DAILY, and the company job visits a seventh of the grid daily,
#: so a day and a half without a materialisation is a lane that stopped, as for the company lane.
FRESH = dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=36))
