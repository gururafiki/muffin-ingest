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
import re


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


def raw_root() -> str:
    """Where stage 1 lands what a provider actually said, before anything interprets it.

    A SEPARATE MOUNT FROM `DAGSTER_HOME`, which is read-only on purpose — Dagster's telemetry tried
    to write into it and crash-looped the daemon. And on `/mnt/data` rather than `/`, which is a
    45 GB boot volume that every image pull needs.
    """
    return _env("MUFFIN_RAW_ROOT", "/var/lib/muffin-ingest/raw")


#: The cache's own locations, which are what `provider` must name. Kept here rather than inferred,
#: because the proxy's path is the contract and a typo silently becomes a direct call — the failure
#: mode that "keeps working and silently bypasses the cache for ever".
CACHE_LOCATIONS = frozenset(
    {
        "sec",
        "sec-data",
        "sec-fts",
        "openfigi",
        "dart",
        "nse",
        "nse-archives",
        "cninfo",
        "cninfo-static",
        "wikidata",
        "yahoo",
        "alphavantage",
        "tiingo",
        "openbb",
    }
)


def provider_base(provider: str, real_origin: str) -> str:
    """Where to reach a provider: the cache location if one is configured, else the real origin.

    THREE SOURCES, IN ORDER, and the middle one was missing until the FX lane needed it. A
    per-provider `<NAME>_BASE_URL` wins, because that is how the edge functions are configured and
    an operator pointing one provider somewhere else must be able to. Otherwise `HTTP_CACHE_URL`
    plus the provider's own location — one variable for every provider, which is what the Swarm
    stack actually sets. Failing both, the REAL origin, so removing the cache degrades nothing.

    `HTTP_CACHE_URL` HAD BEEN SET SINCE THE SERVICE WAS CREATED AND READ BY NOTHING. The compose
    file's comment beside it says providers are reached "through the cache, exactly as the edge
    function reaches them", which was true of no Python provider: this function looked only for the
    per-provider variable, and the stack sets none of those for the ingest services. So the first
    direct-HTTP provider would have gone straight out while every piece of configuration said
    otherwise — nothing in production can report that, which is why it is worth a rule rather than
    a fix at one call site.

    A provider name that is not one of the proxy's own locations raises rather than quietly
    producing a URL the cache will 404 on.
    """
    override = os.environ.get(f"{provider.upper().replace('-', '_')}_BASE_URL")
    if override:
        return override

    cache = os.environ.get("HTTP_CACHE_URL")
    if cache:
        if provider not in CACHE_LOCATIONS:
            raise RuntimeError(
                f"{provider!r} is not one of http-cache's locations ({sorted(CACHE_LOCATIONS)}); "
                f"a name the proxy does not serve becomes a 404 wearing a cache miss's clothes"
            )
        return f"{cache.rstrip('/')}/{provider}"

    return real_origin


#: SEC wants a contact address and refuses a crawler that advertises a WEBSITE instead.
_CONTACT = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
#: A scheme, or a bare `host.tld` — both are refused even when an email is also present.
#: Applied to the string with EMAILS REMOVED, because `ops@example.com` contains `example.com`
#: and a lookaround clever enough to tell them apart is a lookaround nobody will read correctly.
_HOSTNAME = re.compile(r"https?://|\b[\w-]+\.[a-z]{2,}\b", re.IGNORECASE)


def user_agent() -> str:
    """SEC MANDATES a descriptive User-Agent, and it is not part of the cache key.

    Two callers differing only by header share a cache entry, so this being wrong is invisible
    until SEC blocks the node.

    IT WANTS A CONTACT ADDRESS AND REFUSES A WEBSITE. Measured against
    `www.sec.gov/files/company_tickers.json` on 2026-09-12, two paced rounds, identical both times:

        403   1,924 B   muffin-market/1.0 (+https://github.com/…/muffin)      <- the deployed value
        403   1,924 B   muffin-market/1.0 (+https://github.com/…/muffin; ops@example.com)
        200 797,931 B   muffin-market/1.0 (ops@example.com)
        403   1,924 B   muffin-market/1.0 (example.org)

    THE SECOND ROW IS THE ONE THAT MATTERS: adding an email does not rescue a User-Agent that
    also carries a URL, so "must contain an email" is necessary and NOT sufficient. An unpaced
    first pass suggested a URL was tolerated alongside an email; it did not reproduce when the
    probes were spaced, and the paced result is the one recorded here.

    This function had NO CALLER until the registry lane needed it — the one HTTP provider in the
    repo sends its own browser string — so the mandated header was configured, deployed, and had
    never once been used, carrying a value that could only ever have been refused. Exactly what
    its own docstring warned would be invisible.

    SO THERE IS NO DEFAULT NOW. A default that is guaranteed to 403 is worse than none: it turns
    a configuration error into a provider outage, one the 403 body does not explain and which
    looks identical to being blocked. Refusing names the variable and the required shape at the
    moment of the call, before any request leaves.
    """
    value = _env("MUFFIN_USER_AGENT", "")
    if not _CONTACT.search(value):
        raise RuntimeError(
            "MUFFIN_USER_AGENT must be set and must contain a contact email address; SEC answers "
            '403 without one. Example: "muffin-market/1.0 (ops@example.com)".'
        )
    if _HOSTNAME.search(_CONTACT.sub("", value)):
        raise RuntimeError(
            f"MUFFIN_USER_AGENT contains a hostname or URL ({value!r}); SEC answers 403 to those "
            f"even when a contact email is also present — measured, twice, paced. Give the email "
            f"alone."
        )
    return value
