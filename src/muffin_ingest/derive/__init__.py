"""Computation over data we already hold. No provider, no network, no rate limit.

Deriving needs no provider, so it is arithmetic — which changes what a backlog MEANS for anything
in here: there is no queue, only a re-computation, and a "pending" count is a drift counter rather
than work waiting.
"""
