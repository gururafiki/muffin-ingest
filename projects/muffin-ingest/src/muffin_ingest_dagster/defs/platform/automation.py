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
#: WHAT THE CODE LOCATION EVALUATES INSTEAD OF THE DAEMON — assets whose condition contains a
#: Python subclass. Dagster hands a condition to the AssetDaemon only if EVERY node in it is
#: whitelisted for serialisation (`is_serializable` is `all(children)`); otherwise the location
#: ships a display SNAPSHOT with `automation_condition = None`
#: (`external_data.resolve_automation_condition_args`), and a daemon-evaluated sensor has nothing
#: to evaluate. The asset is silently left out — built-in branches included, not just ours.
#:
#: Measured 2026-09-24 on the symbology rungs, whose `SYMBOLOGY_AUTOMATION` carries `ReAskAfter`:
#: 6,984 partitions seeded, fifteen hours, 1,807 ticks of the sensor below, nothing requested. The
#: UI still displayed the condition from the snapshot, so it looked configured, and every test
#: passed — `evaluate_automation_conditions` runs in-process, where the Python object exists. They
#: had been unable to fire since they shipped on 2026-09-12.
#:
#: KEYED BY NAME so this module imports no family, and guarded rather than trusted: a key that stops
#: matching leaves the rungs uncovered, and `test_automation_is_evaluable` fails on exactly that.
EVALUATED_IN_THE_CODE_LOCATION = dg.AssetSelection.assets(
    "raw_figi_ticker", "raw_figi_local_symbol"
)

automation = dg.AutomationConditionSensorDefinition(
    name="default_automation_condition_sensor",
    # EVERYTHING ELSE. Two automation sensors may not target one asset — Dagster refuses the
    # definitions outright — so the split has to be a subtraction, not an addition.
    target=dg.AssetSelection.all() - EVALUATED_IN_THE_CODE_LOCATION,
    default_status=dg.DefaultSensorStatus.RUNNING,
)

#: `use_user_code_server=True` is Dagster's documented remedy for a custom condition: this sensor is
#: evaluated in the code location, beside the class. It is Beta and capped at 500 targets; it has
#: two. RUNNING IN CODE, because a user-defined automation sensor defaults to STOPPED, and a sensor
#: that ships stopped is a lane that does not exist.
code_location_automation = dg.AutomationConditionSensorDefinition(
    name="symbology_rungs",
    target=EVALUATED_IN_THE_CODE_LOCATION,
    use_user_code_server=True,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Evaluates conditions the AssetDaemon cannot deserialise — the OpenFIGI rungs' re-ask is a "
        "Python subclass — inside the code location."
    ),
)
