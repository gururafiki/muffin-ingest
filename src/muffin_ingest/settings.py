"""Configuration, read from the environment at CALL time rather than at import.

Read at import, a missing variable kills the process before `argv` is parsed — which is how the
quarter-threshold guard lost its offline mode and stopped running in CI at all. Every accessor here
is a function for that reason, and every provider base URL defaults to the REAL origin so the
read-through cache stays removable without an outage.

DART IS THE ONE EXCEPTION and it is deliberate: Deno could not reach it at all (TLS 1.2 static-RSA
against rustls, which implements forward-secret key exchange only), so for that provider the proxy
hop is a correctness dependency rather than an optimisation. Python's OpenSSL can speak to it
directly, so the default here is honest — but the cache is still preferred, because a filed document
is immutable and DART answers in ~3.5 s from this node.
"""

from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def database_url() -> str:
    """The DIRECT Postgres connection, not PostgREST.

    PostgREST is the read path for muffin-ui and stays that way. A writer needs transactions
    spanning claim -> write -> complete, `for update skip locked`, COPY, and a role with its own
    statement timeout — and PostgREST's own limits have cost four separate defects here, the
    quietest being `PGRST_DB_MAX_ROWS` silently truncating a 5,000-row request to 1,000.
    """
    url = os.environ.get("INGEST_DATABASE_URL")
    if not url:
        raise RuntimeError(
            "INGEST_DATABASE_URL is unset; the ingestion worker connects to Postgres directly"
        )
    return url


def cache_base() -> str:
    """The read-through cache in front of every provider."""
    return _env("HTTP_CACHE_URL", "http://http-cache:8080")


def provider_base(provider: str, real_origin: str) -> str:
    """Where to reach a provider: the cache location if one is configured, else the real origin.

    A provider with no location keeps working and silently bypasses the cache for ever, which
    nothing in production can report — so CI compares this table against the proxy's own locations
    in both directions.
    """
    override = os.environ.get(f"{provider.upper().replace('-', '_')}_BASE_URL")
    return override or real_origin


def user_agent() -> str:
    """SEC MANDATES a descriptive User-Agent, and it is not part of the cache key.

    Two callers differing only by header share a cache entry, so this being wrong is invisible
    until SEC blocks the node.
    """
    return _env("MUFFIN_USER_AGENT", "muffin-market/1.0 (+https://github.com/gururafiki/muffin)")
