"""The sensor that makes every `AutomationCondition` in this location do anything at all."""

import dagster as dg

# THE AUTOMATION SENSOR SHIPS STOPPED, AND WITHOUT IT `AutomationCondition` DOES NOTHING.
#
# `security_return` declares `AutomationCondition.eager()` and had never once fired: measured
# 2026-09-11, **`AUTO-MATERIALIZE runs ever: 0`** against 48 daemon ticks, all of them from the two
# standard sensors, while the history load wrote 20 M rows and the returns table sat at the 96
# securities a hand-run had given it. Dagster creates `default_automation_condition_sensor`
# automatically and leaves it STOPPED, so the mechanism this design uses to replace the old
# system's :24/:54/:14 cron choreography was inert.
#
# Declared here rather than started in the UI, for the same reason every other control in this
# repo is: a thing switched on by hand is a thing the next rebuild forgets, and nothing would
# report it — the runs simply would not happen.
automation = dg.AutomationConditionSensorDefinition(
    name="default_automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
