"""Once-only alert claims: per-listing state machine + cross-source deal clusters.

Every alert candidate is *claimed* exactly once before it is dispatched. A claim answers
two questions atomically:

1. **Listing state machine** (key ``{prefix}{alerts}:l:{listing_key}``, a hash holding
   the alerted price ``p``, the claim time ``ts`` and the claim token ``tok``)::

       absent ──claim──▶ NEW            (listing stored, TTL = dedup.listing_ttl_hours)
       stored ──claim──▶ PRICE_DROP     if  new <= prev * (1 - min_drop_pct)
                                        and prev - new >= min_drop_abs
                                        (price replaced, TTL refreshed)
       stored ──claim──▶ DUPLICATE      otherwise (same price, price went up, small drop;
                                        nothing is written, so the TTL is *not* refreshed
                                        and a listing that stays up re-alerts once the
                                        re-alert window expires)

   The stored price is always the *last alerted* price, so a later drop is measured
   against what a human actually saw, never against an intermediate price increase.

2. **Cross-source clusters** (key ``{prefix}{alerts}:c:{product_key}:{identity}:{bucket}``):
   the same Best Buy deal is typically seen by the retailer endpoint, Slickdeals and
   r/buildapcsales within minutes. Before a listing becomes NEW, the claim looks at the
   cluster keys of buckets ``b-1``, ``b`` and ``b+1`` (``b = price_bucket(total_price)``,
   geometric buckets of ``dedup.price_bucket_pct``), so prices that straddle a bucket
   boundary still collide. Any hit owned by a *different* listing yields
   CROSS_SOURCE_DUPLICATE and the listing key is **not** written (if the other alert
   is rolled back, this source may still alert later). NEW and PRICE_DROP claims set
   the centre key ``b`` (TTL = ``dedup.cluster_ttl_hours``). Only RETAIL/AGGREGATOR
   items with a resolvable store identity (:func:`cluster_identity`) take part: two
   different people selling the same GPU for the same price on Marketplace/eBay are
   two deals, not one. A PRICE_DROP is not gated by the cluster: it is the listing a
   human already acted on (or chose not to) getting cheaper.

Atomicity
---------
``RedisDeduplicator`` runs the whole decision as ONE Lua script (``EVALSHA`` via
``register_script``; redis-py reloads it transparently after a ``SCRIPT FLUSH`` or a
fail-over), so 50 workers on several nodes claiming the same listing concurrently get
exactly one NEW. All keys carry the ``{alerts}`` hash tag so they live in one Redis
Cluster slot, as multi-key scripts require.

Lua gotcha: numbers returned from a script to Redis are truncated to integers
(``1234.56`` would come back as ``1234``). Prices therefore travel as strings in both
directions: the client sends ``repr(round(price, 2))``, the script stores and returns
that text verbatim, and Python parses it back with ``float()`` — bit-exact.

Rollback
--------
If every dispatch target fails, the pipeline calls :meth:`Deduplicator.rollback` so a
later observation (or another node) can alert again. Each claim writes a random claim
token into everything it creates; the rollback script only touches keys that *still*
carry that token. A newer claim that happened in between (a later PRICE_DROP, or a
re-claim after an earlier rollback) owns the keys by then and is never clobbered.
Rolling back a NEW deletes the listing and its cluster key; rolling back a PRICE_DROP
restores the previous price, token and remaining TTL, and the previous cluster value.
The undo state travels in ``DedupDecision.rollback`` (plain JSON, no secrets).

``MemoryDeduplicator`` implements identical semantics (same epsilon, same float
arithmetic, same cluster value encoding) with dicts, lazy expiry through an
injectable clock, an :class:`asyncio.Lock` and a periodic purge of expired entries.
It is the single-node backend and the pipeline's fallback when Redis is unreachable;
``claim`` on the Redis backend lets Redis errors propagate so that fallback can kick in.
"""

from __future__ import annotations

import abc
import asyncio
import logging
import math
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from deal_radar.config_schema import AppConfig
from deal_radar.core.logs import get_logger
from deal_radar.core.metrics import Metrics
from deal_radar.engine.types import DealItem, DedupDecision, DedupStatus, SourceKind

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio import Redis

log = get_logger("dedup")

HASH_TAG = "{alerts}"

# Tolerance for the PRICE_DROP thresholds so an exact 5 % / $10 drop counts as a drop in
# both backends despite binary floating point (prices are cents, so 1e-6 is invisible).
DROP_EPSILON = 1e-6

# Registrable-domain labels / store keys that are peer-to-peer marketplaces, deal
# communities or link infrastructure rather than a store: never a cluster identity.
_NON_RETAIL_IDENTITIES = frozenset(
    {
        "ebay", "facebook", "fb", "offerup", "craigslist", "reddit", "redd", "slickdeals",
        "mercari", "swappa", "kijiji", "letgo", "nextdoor", "poshmark", "hardwareswap",
        "imgur", "bit", "tinyurl", "t", "youtube", "youtu", "twitter", "x", "discord",
    }
)

# Spelling variants used by different sources for the same store ("B&H" on Reddit,
# "B&H Photo" on Slickdeals, bhphotovideo.com in links) → one canonical key.
_IDENTITY_ALIASES: dict[str, str] = {
    "amazoncom": "amazon",
    "amzn": "amazon",
    "bby": "bestbuy",
    "bh": "bhphotovideo",
    "bhphoto": "bhphotovideo",
    "bandh": "bhphotovideo",
    "bandhphoto": "bhphotovideo",
    "bhphotovideocom": "bhphotovideo",
    "delltechnologies": "dell",
    "hpstore": "hp",
    "hpinc": "hp",
    "lgelectronics": "lg",
    "samsungepp": "samsung",
    "samsungeppedu": "samsung",
    "bjswholesale": "bjs",
    "bjswholesaleclub": "bjs",
    "officedepotofficemax": "officedepot",
    "officemax": "officedepot",
    "thehomedepot": "homedepot",
    "microsoftstore": "microsoft",
    "googlestore": "google",
    "applestore": "apple",
    "walmartcom": "walmart",
    "targetcom": "target",
    "neweggcom": "newegg",
    "microcentercom": "microcenter",
}

# Canonical keys of common stores (used to strip a trailing TLD typed into a store name).
_KNOWN_STORES = frozenset(_IDENTITY_ALIASES.values()) | frozenset(
    {
        "bestbuy", "newegg", "walmart", "target", "microcenter", "costco", "samsclub", "adorama", "antonline",
        "woot", "staples", "gamestop", "abt", "crutchfield", "monoprice", "nvidia", "lowes", "kohls", "macys",
        "asus", "acer", "msi", "sony", "lenovo", "qvc", "meh", "zotac", "evga", "gigabyte", "corsair", "nzxt",
    }
)

# Short links whose registrable domain says nothing about the store label.
_DOMAIN_IDENTITIES: dict[str, str] = {"amzn.to": "amazon", "a.co": "amazon", "amzn.com": "amazon"}

_MULTI_PART_SUFFIXES = frozenset(
    {"co.uk", "org.uk", "com.au", "net.au", "co.nz", "co.jp", "com.br", "com.mx", "co.in", "com.sg", "co.kr", "com.tr"}
)
_TLD_SUFFIXES = ("com", "net", "us", "ca")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_HOSTLIKE = re.compile(r"^(?:https?://)?(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?:/.*)?$")


# --------------------------------------------------------------------------- pure helpers


def price_bucket(price: float, bucket_pct: float) -> int:
    """Geometric price bucket: ``floor(log(price) / log(1 + bucket_pct))``; ``price <= 0`` → 0.

    Neighbouring buckets differ by ``bucket_pct`` in price, so checking ``b-1..b+1``
    matches any two prices within roughly ``bucket_pct`` of each other.
    """
    if not price > 0 or not math.isfinite(price):
        return 0
    return math.floor(math.log(price) / math.log1p(bucket_pct))


def _registrable_domain(host: str) -> str:
    labels = [p for p in host.lower().strip(".").split(".") if p]
    if len(labels) >= 3 and ".".join(labels[-2:]) in _MULTI_PART_SUFFIXES:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def _identity_from_domain(host: str) -> str | None:
    host = host.lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return None
    domain = _registrable_domain(host)
    if domain in _DOMAIN_IDENTITIES:
        return _DOMAIN_IDENTITIES[domain]
    label = _NON_ALNUM.sub("", domain.split(".", 1)[0])
    return _IDENTITY_ALIASES.get(label, label) or None


def _identity_from_name(name: str) -> str | None:
    text = name.strip().lower()
    if not text:
        return None
    if _HOSTLIKE.match(text) and " " not in text:
        host = urlsplit(text if "://" in text else f"//{text}").hostname or ""
        return _identity_from_domain(host)
    key = _NON_ALNUM.sub("", text)
    if not key:
        return None
    if key in _IDENTITY_ALIASES:
        return _IDENTITY_ALIASES[key]
    for suffix in _TLD_SUFFIXES:  # "Best Buy .com", "Newegg-com": a known store plus a TLD
        base = key[: -len(suffix)]
        if key.endswith(suffix) and base:
            if base in _IDENTITY_ALIASES:
                return _IDENTITY_ALIASES[base]
            if base in _KNOWN_STORES:
                return base
    return key


def _identity_from_url(url: str) -> str | None:
    try:
        host = urlsplit(url).hostname or ""
    except ValueError:
        return None
    return _identity_from_domain(host) if host else None


def cluster_identity(item: DealItem) -> str | None:
    """Store identity used for cross-source suppression, or ``None`` to never cluster.

    Only RETAIL and AGGREGATOR items cluster. The store comes from ``item.retailer``
    (normalised: lower-case alphanumerics, known aliases folded, ``bestbuy.com`` and
    ``Best Buy`` both → ``bestbuy``), else from the registrable domain of
    ``item.best_url``. Marketplaces and deal communities (eBay, Facebook, OfferUp,
    Craigslist, Reddit, Slickdeals, ...) are not stores: different sellers there are
    different deals, so they yield ``None``.
    """
    if item.source_kind not in (SourceKind.RETAIL, SourceKind.AGGREGATOR):
        return None
    identity: str | None = None
    if item.retailer:
        identity = _identity_from_name(item.retailer)
    if identity is None:
        identity = _identity_from_url(item.best_url)
    if identity is None or identity in _NON_RETAIL_IDENTITIES:
        return None
    return identity


def _price_text(price: float) -> str:
    """Canonical decimal text of a price: what the Lua script stores and returns."""
    value = float(price)
    if not math.isfinite(value):
        raise ValueError(f"cannot claim a non-finite price: {price!r}")
    return repr(round(value, 2) + 0.0)  # + 0.0 folds -0.0 into 0.0


def _cluster_member(token: str, price_text: str, listing_key: str) -> str:
    """Cluster key value: ``token|price|listing_key`` (token and price never contain '|')."""
    return f"{token}|{price_text}|{listing_key}"


def _parse_member(value: str) -> tuple[str, str, str]:
    token, _, rest = value.partition("|")
    price, _, listing_key = rest.partition("|")
    return token, price, listing_key


def _to_float(text: str | None) -> float | None:
    if text is None or text == "":
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _is_price_drop(price: float, previous: float, min_drop_pct: float, min_drop_abs: float) -> bool:
    """Exactly the comparison the Lua script performs (same IEEE-754 double arithmetic)."""
    return price <= previous * (1 - min_drop_pct) + DROP_EPSILON and (previous - price) >= min_drop_abs - DROP_EPSILON


@dataclass(frozen=True, slots=True)
class _ClaimPlan:
    """Everything a backend needs for one claim, computed once in Python."""

    listing_key: str  # DealItem.listing_key (stored in cluster values)
    listing_redis_key: str
    price_text: str
    price: float
    cluster_keys: tuple[str, ...]  # (centre, lower, upper) or () when not clustering


# --------------------------------------------------------------------------- interface


class Deduplicator(abc.ABC):
    """Atomic once-only claim of an alert for a listing (see module docstring)."""

    backend: str = "abstract"

    def __init__(self, config: AppConfig, *, metrics: Metrics | None = None) -> None:
        self.config = config
        dd = config.dedup
        self.prefix = config.storage.redis_key_prefix
        self.listing_ttl_ms = max(1, int(round(dd.listing_ttl_hours * 3_600_000)))
        self.cluster_ttl_ms = max(1, int(round(dd.cluster_ttl_hours * 3_600_000)))
        self.min_drop_pct = float(dd.min_drop_pct)
        self.min_drop_abs = float(dd.min_drop_abs)
        self.bucket_pct = float(dd.price_bucket_pct)
        self.cross_source = bool(dd.cross_source)
        self._m_decisions = None
        self._m_rollbacks = None
        if metrics is not None:
            self._m_decisions = metrics.counter("dedup_decisions_total", "Dedup claim decisions", ("backend", "status"))
            self._m_rollbacks = metrics.counter("dedup_rollbacks_total", "Dedup claim rollbacks", ("backend", "result"))

    # ------------------------------------------------------------------ keys

    def listing_redis_key(self, listing_key: str) -> str:
        return f"{self.prefix}{HASH_TAG}:l:{listing_key}"

    def cluster_key(self, product_key: str, identity: str, bucket: int) -> str:
        return f"{self.prefix}{HASH_TAG}:c:{product_key}:{identity}:{bucket}"

    def _plan(self, item: DealItem, product_key: str) -> _ClaimPlan:
        price_text = _price_text(item.total_price)
        price = float(price_text)
        cluster_keys: tuple[str, ...] = ()
        if self.cross_source:
            identity = cluster_identity(item)
            if identity is not None:
                bucket = price_bucket(price, self.bucket_pct)
                cluster_keys = (
                    self.cluster_key(product_key, identity, bucket),
                    self.cluster_key(product_key, identity, bucket - 1),
                    self.cluster_key(product_key, identity, bucket + 1),
                )
        return _ClaimPlan(
            listing_key=item.listing_key,
            listing_redis_key=self.listing_redis_key(item.listing_key),
            price_text=price_text,
            price=price,
            cluster_keys=cluster_keys,
        )

    # ------------------------------------------------------------------ decisions

    def _decision(self, plan: _ClaimPlan, reply: list[str], token: str, claimed_at: float) -> DedupDecision:
        """Build a DedupDecision from the (backend-independent) claim reply.

        Reply layouts (all strings):
          ``["new"]``
          ``["price_drop", prev_price, prev_token, prev_ts, prev_ttl_ms, cluster_prev, cluster_prev_ttl_ms]``
          ``["duplicate", prev_price]``
          ``["cross_source_duplicate", owner_price, matched_key, owner_listing_key]``
        """
        status = DedupStatus(reply[0])
        centre = plan.cluster_keys[0] if plan.cluster_keys else None
        if status is DedupStatus.NEW:
            decision = DedupDecision(
                status=status,
                keys=[plan.listing_redis_key, *([centre] if centre else [])],
                rollback={
                    "backend": self.backend,
                    "mode": "new",
                    "token": token,
                    "listing": plan.listing_redis_key,
                    "cluster": centre,
                    "claimed_at": claimed_at,
                },
            )
        elif status is DedupStatus.PRICE_DROP:
            decision = DedupDecision(
                status=status,
                previous_price=_to_float(reply[1]),
                keys=[plan.listing_redis_key, *([centre] if centre else [])],
                rollback={
                    "backend": self.backend,
                    "mode": "price_drop",
                    "token": token,
                    "listing": plan.listing_redis_key,
                    "cluster": centre,
                    "claimed_at": claimed_at,
                    "prev_price": reply[1],
                    "prev_token": reply[2],
                    "prev_ts": reply[3],
                    "prev_ttl_ms": int(_to_float(reply[4]) or 0),
                    "cluster_prev": reply[5] or None,
                    "cluster_prev_ttl_ms": int(_to_float(reply[6]) or 0),
                },
            )
        elif status is DedupStatus.DUPLICATE:
            decision = DedupDecision(status=status, previous_price=_to_float(reply[1]), keys=[plan.listing_redis_key])
        else:
            decision = DedupDecision(status=status, previous_price=_to_float(reply[1]), keys=[reply[2]])
            if log.isEnabledFor(logging.DEBUG):  # avoid building the record on the hot path
                log.debug(
                    "cross-source duplicate",
                    extra={"listing": plan.listing_key, "duplicate_of": reply[3], "cluster_key": reply[2]},
                )
        if self._m_decisions is not None:
            self._m_decisions.inc(backend=self.backend, status=status.value)
        return decision

    def _rollback_state(self, decision: DedupDecision) -> dict[str, Any] | None:
        """The undo state if ``decision`` is a rollback-able claim made by this backend."""
        if not decision.should_alert or not decision.rollback:
            return None
        state = decision.rollback
        if state.get("backend") != self.backend or state.get("mode") not in ("new", "price_drop") or not state.get("token"):
            log.warning(
                "ignoring rollback of a claim made by another deduplicator",
                extra={"backend": self.backend, "claim_backend": state.get("backend")},
            )
            return None
        return state

    def _count_rollback(self, undone: bool) -> None:
        if self._m_rollbacks is not None:
            self._m_rollbacks.inc(backend=self.backend, result="undone" if undone else "superseded")

    # ------------------------------------------------------------------ API

    @abc.abstractmethod
    async def claim(self, item: DealItem, product_key: str) -> DedupDecision:
        """Atomically decide NEW / PRICE_DROP / DUPLICATE / CROSS_SOURCE_DUPLICATE for ``item.total_price``."""

    @abc.abstractmethod
    async def rollback(self, decision: DedupDecision) -> None:
        """Undo a NEW/PRICE_DROP claim if no newer claim superseded it (no-op otherwise)."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Release resources (the Redis client itself is owned by the caller)."""


# --------------------------------------------------------------------------- Redis

# KEYS[1] listing hash; KEYS[2] centre cluster key; KEYS[3], KEYS[4] neighbour cluster keys
# (KEYS[2..4] absent when the item does not cluster).
# ARGV[1] price text, ARGV[2] claim time, ARGV[3] claim token, ARGV[4] listing TTL ms,
# ARGV[5] cluster TTL ms, ARGV[6] min_drop_pct, ARGV[7] min_drop_abs, ARGV[8] listing_key,
# ARGV[9] epsilon. Every value returned is a string (see module docstring).
CLAIM_LUA = r"""
local lkey = KEYS[1]
local price = tonumber(ARGV[1])
local eps = tonumber(ARGV[9])
local member = ARGV[3] .. '|' .. ARGV[1] .. '|' .. ARGV[8]
local clustered = #KEYS >= 2

local prev = redis.call('HGET', lkey, 'p')
if prev then
  local prev_n = tonumber(prev)
  if prev_n and price <= prev_n * (1 - tonumber(ARGV[6])) + eps and (prev_n - price) >= tonumber(ARGV[7]) - eps then
    local old = redis.call('HMGET', lkey, 'tok', 'ts')
    local old_pttl = redis.call('PTTL', lkey)
    redis.call('HSET', lkey, 'p', ARGV[1], 'ts', ARGV[2], 'tok', ARGV[3])
    redis.call('PEXPIRE', lkey, ARGV[4])
    local c_prev, c_pttl = '', '0'
    if clustered then
      local cur = redis.call('GET', KEYS[2])
      if cur then
        c_prev = cur
        c_pttl = tostring(redis.call('PTTL', KEYS[2]))
      end
      redis.call('SET', KEYS[2], member, 'PX', ARGV[5])
    end
    return {'price_drop', prev, old[1] or '', old[2] or '', tostring(old_pttl), c_prev, c_pttl}
  end
  return {'duplicate', prev}
end

if clustered then
  for i = 2, #KEYS do
    local owner = redis.call('GET', KEYS[i])
    if owner then
      local s1 = string.find(owner, '|', 1, true)
      local s2 = s1 and string.find(owner, '|', s1 + 1, true)
      local owner_listing = ''
      local owner_price = ''
      if s2 then
        owner_listing = string.sub(owner, s2 + 1)
        owner_price = string.sub(owner, s1 + 1, s2 - 1)
      end
      -- our own stale cluster entry (listing expired before its cluster key) is no duplicate
      if owner_listing ~= ARGV[8] then
        return {'cross_source_duplicate', owner_price, KEYS[i], owner_listing}
      end
    end
  end
end

redis.call('HSET', lkey, 'p', ARGV[1], 'ts', ARGV[2], 'tok', ARGV[3])
redis.call('PEXPIRE', lkey, ARGV[4])
if clustered then
  redis.call('SET', KEYS[2], member, 'PX', ARGV[5])
end
return {'new'}
"""

# KEYS[1] listing hash; KEYS[2] cluster key (optional).
# ARGV[1] claim token, ARGV[2] mode ('new' | 'price_drop'), ARGV[3] previous price text,
# ARGV[4] previous token, ARGV[5] previous ts, ARGV[6] remaining listing TTL ms to restore,
# ARGV[7] previous cluster value ('' = none), ARGV[8] remaining cluster TTL ms to restore.
# Returns a bitmask: 1 = listing undone, 2 = cluster key undone.
ROLLBACK_LUA = r"""
local undone = 0
local token = ARGV[1]
if redis.call('HGET', KEYS[1], 'tok') == token then
  local ttl = tonumber(ARGV[6])
  if ARGV[2] == 'price_drop' and ARGV[3] ~= '' and ttl and ttl > 0 then
    redis.call('HSET', KEYS[1], 'p', ARGV[3], 'tok', ARGV[4], 'ts', ARGV[5])
    redis.call('PEXPIRE', KEYS[1], ttl)
  else
    redis.call('DEL', KEYS[1])
  end
  undone = undone + 1
end
if #KEYS >= 2 then
  local cur = redis.call('GET', KEYS[2])
  if cur and string.sub(cur, 1, string.len(token) + 1) == token .. '|' then
    local cttl = tonumber(ARGV[8])
    if ARGV[7] ~= '' and cttl and cttl > 0 then
      redis.call('SET', KEYS[2], ARGV[7], 'PX', cttl)
    else
      redis.call('DEL', KEYS[2])
    end
    undone = undone + 2
  end
end
return undone
"""


class RedisDeduplicator(Deduplicator):
    """Cluster-wide dedup: one EVALSHA round trip per claim (~0.1-1 ms on the same VM)."""

    backend = "redis"

    def __init__(self, redis: "Redis", config: AppConfig, *, metrics: Metrics | None = None) -> None:
        super().__init__(config, metrics=metrics)
        self.redis = redis
        self._claim_script = redis.register_script(CLAIM_LUA)
        self._rollback_script = redis.register_script(ROLLBACK_LUA)
        self._epsilon = repr(DROP_EPSILON)
        self._args_tail = (
            str(self.listing_ttl_ms),
            str(self.cluster_ttl_ms),
            repr(self.min_drop_pct),
            repr(self.min_drop_abs),
        )

    async def claim(self, item: DealItem, product_key: str) -> DedupDecision:
        plan = self._plan(item, product_key)
        token = uuid.uuid4().hex
        claimed_at = time.time()
        raw = await self._claim_script(
            keys=[plan.listing_redis_key, *plan.cluster_keys],
            args=[plan.price_text, repr(claimed_at), token, *self._args_tail, plan.listing_key, self._epsilon],
        )
        reply = [_text(part) for part in raw]
        return self._decision(plan, reply, token, claimed_at)

    async def rollback(self, decision: DedupDecision) -> None:
        state = self._rollback_state(decision)
        if state is None:
            return
        elapsed_ms = max(0, int((time.time() - float(state.get("claimed_at") or 0.0)) * 1000))
        listing_ttl = max(0, int(state.get("prev_ttl_ms") or 0) - elapsed_ms)
        cluster_ttl = max(0, int(state.get("cluster_prev_ttl_ms") or 0) - elapsed_ms)
        keys = [state["listing"]]
        if state.get("cluster"):
            keys.append(state["cluster"])
        undone = await self._rollback_script(
            keys=keys,
            args=[
                state["token"],
                state["mode"],
                state.get("prev_price") or "",
                state.get("prev_token") or "",
                state.get("prev_ts") or "",
                str(listing_ttl),
                state.get("cluster_prev") or "",
                str(cluster_ttl),
            ],
        )
        self._count_rollback(bool(int(undone) & 1))
        log.info(
            "dedup claim rolled back" if int(undone) & 1 else "dedup rollback skipped: superseded by a newer claim",
            extra={"listing": state["listing"], "mode": state["mode"], "undone": int(undone)},
        )

    async def close(self) -> None:
        # The client is shared (bus, vision cache, leases) and closed by its owner.
        return None


# --------------------------------------------------------------------------- memory


@dataclass(slots=True)
class _ListingEntry:
    price_text: str
    ts: str
    token: str
    expires_at: float


@dataclass(slots=True)
class _ClusterEntry:
    value: str  # same ``token|price|listing_key`` encoding as the Redis backend
    expires_at: float


class MemoryDeduplicator(Deduplicator):
    """Process-local dedup with exactly the Redis backend's semantics.

    Expiry is lazy (checked on access) plus a sweep of everything expired at most once
    per ``purge_interval`` seconds of the injected clock, so memory stays bounded by
    the TTLs without a background task.
    """

    backend = "memory"

    def __init__(
        self,
        config: AppConfig,
        *,
        clock: Callable[[], float] = time.time,
        metrics: Metrics | None = None,
        purge_interval: float = 60.0,
    ) -> None:
        super().__init__(config, metrics=metrics)
        self.clock = clock
        self.purge_interval = purge_interval
        self._listings: dict[str, _ListingEntry] = {}
        self._clusters: dict[str, _ClusterEntry] = {}
        self._lock = asyncio.Lock()
        self._last_purge = clock()

    def __len__(self) -> int:
        """Stored listing entries (expired ones linger until the next purge)."""
        return len(self._listings)

    @property
    def cluster_count(self) -> int:
        return len(self._clusters)

    def purge_expired(self, now: float | None = None) -> int:
        """Drop every expired listing/cluster entry; returns how many were removed."""
        now = self.clock() if now is None else now
        dead_l = [k for k, e in self._listings.items() if e.expires_at <= now]
        for key in dead_l:
            del self._listings[key]
        dead_c = [k for k, e in self._clusters.items() if e.expires_at <= now]
        for key in dead_c:
            del self._clusters[key]
        self._last_purge = now
        return len(dead_l) + len(dead_c)

    def _listing(self, key: str, now: float) -> _ListingEntry | None:
        entry = self._listings.get(key)
        if entry is not None and entry.expires_at <= now:
            del self._listings[key]
            return None
        return entry

    def _cluster(self, key: str, now: float) -> _ClusterEntry | None:
        entry = self._clusters.get(key)
        if entry is not None and entry.expires_at <= now:
            del self._clusters[key]
            return None
        return entry

    def _ttl_ms(self, expires_at: float, now: float) -> int:
        return max(0, int((expires_at - now) * 1000))

    async def claim(self, item: DealItem, product_key: str) -> DedupDecision:
        plan = self._plan(item, product_key)
        token = uuid.uuid4().hex
        async with self._lock:
            now = self.clock()
            if now - self._last_purge >= self.purge_interval:
                self.purge_expired(now)
            reply = self._claim_locked(plan, token, now)
        return self._decision(plan, reply, token, now)

    def _claim_locked(self, plan: _ClaimPlan, token: str, now: float) -> list[str]:
        listing_expiry = now + self.listing_ttl_ms / 1000.0
        cluster_expiry = now + self.cluster_ttl_ms / 1000.0
        member = _cluster_member(token, plan.price_text, plan.listing_key)
        entry = self._listing(plan.listing_redis_key, now)
        if entry is not None:
            previous = float(entry.price_text)
            if not _is_price_drop(plan.price, previous, self.min_drop_pct, self.min_drop_abs):
                return [DedupStatus.DUPLICATE.value, entry.price_text]
            reply = [
                DedupStatus.PRICE_DROP.value,
                entry.price_text,
                entry.token,
                entry.ts,
                str(self._ttl_ms(entry.expires_at, now)),
                "",
                "0",
            ]
            self._listings[plan.listing_redis_key] = _ListingEntry(plan.price_text, repr(now), token, listing_expiry)
            if plan.cluster_keys:
                current = self._cluster(plan.cluster_keys[0], now)
                if current is not None:
                    reply[5] = current.value
                    reply[6] = str(self._ttl_ms(current.expires_at, now))
                self._clusters[plan.cluster_keys[0]] = _ClusterEntry(member, cluster_expiry)
            return reply
        for key in plan.cluster_keys:
            owner = self._cluster(key, now)
            if owner is None:
                continue
            _, owner_price, owner_listing = _parse_member(owner.value)
            if owner_listing != plan.listing_key:
                return [DedupStatus.CROSS_SOURCE_DUPLICATE.value, owner_price, key, owner_listing]
        self._listings[plan.listing_redis_key] = _ListingEntry(plan.price_text, repr(now), token, listing_expiry)
        if plan.cluster_keys:
            self._clusters[plan.cluster_keys[0]] = _ClusterEntry(member, cluster_expiry)
        return [DedupStatus.NEW.value]

    async def rollback(self, decision: DedupDecision) -> None:
        state = self._rollback_state(decision)
        if state is None:
            return
        token: str = state["token"]
        async with self._lock:
            now = self.clock()
            elapsed = max(0.0, now - float(state.get("claimed_at") or now))
            undone = 0
            entry = self._listing(state["listing"], now)
            if entry is not None and entry.token == token:
                prev_ttl = int(state.get("prev_ttl_ms") or 0) / 1000.0 - elapsed
                if state["mode"] == "price_drop" and state.get("prev_price") and prev_ttl > 0:
                    self._listings[state["listing"]] = _ListingEntry(
                        price_text=state["prev_price"],
                        ts=state.get("prev_ts") or "",
                        token=state.get("prev_token") or "",
                        expires_at=now + prev_ttl,
                    )
                else:
                    del self._listings[state["listing"]]
                undone |= 1
            cluster_key = state.get("cluster")
            if cluster_key:
                current = self._cluster(cluster_key, now)
                if current is not None and _parse_member(current.value)[0] == token:
                    prev_value = state.get("cluster_prev")
                    prev_cttl = int(state.get("cluster_prev_ttl_ms") or 0) / 1000.0 - elapsed
                    if prev_value and prev_cttl > 0:
                        self._clusters[cluster_key] = _ClusterEntry(prev_value, now + prev_cttl)
                    else:
                        del self._clusters[cluster_key]
                    undone |= 2
        self._count_rollback(bool(undone & 1))
        log.info(
            "dedup claim rolled back" if undone & 1 else "dedup rollback skipped: superseded by a newer claim",
            extra={"listing": state["listing"], "mode": state["mode"], "undone": undone},
        )

    async def close(self) -> None:
        async with self._lock:
            self._listings.clear()
            self._clusters.clear()


# --------------------------------------------------------------------------- factory


def build_deduplicator(config: AppConfig, redis: "Redis | None" = None, *, metrics: Metrics | None = None) -> Deduplicator:
    """Redis-backed (cluster-wide) when a client is given, else process-local."""
    if redis is not None:
        return RedisDeduplicator(redis, config, metrics=metrics)
    return MemoryDeduplicator(config, metrics=metrics)


__all__ = [
    "CLAIM_LUA",
    "DROP_EPSILON",
    "HASH_TAG",
    "ROLLBACK_LUA",
    "Deduplicator",
    "MemoryDeduplicator",
    "RedisDeduplicator",
    "build_deduplicator",
    "cluster_identity",
    "price_bucket",
]
