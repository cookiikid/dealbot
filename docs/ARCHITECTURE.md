# DealRadar — Architecture & Latency Reasoning (Phase 1)

> Scope: why public deal feeds are late, where every second of delay comes from on
> each platform, what DealRadar does about each one (and what it deliberately does
> not do), how work is split between Google Cloud and the two local RTX machines,
> the exact anomaly-scoring function, and how alerts are deduplicated exactly once
> across a distributed fleet.
>
> Platform facts in this document were verified live on 2026-10-06 by the research
> pass that preceded implementation (official docs, live probes from a datacenter
> IP, open-source reference implementations). Where a number is an estimate it is
> marked "≈".

---

## 1. The honest model of "speed"

A deal alert is late for three independent reasons, and only one of them is about
network latency:

```
 t0  price/inventory changes at the origin (retailer DB, seller posts listing)
 t1  change becomes observable (API/search index/feed/CDN edge reflects it)      ← visibility lag
 t2  we observe it (next poll after t1)                                          ← polling lag  ≈ T/2 on average
 t3  we decide it is a real deal and send it (normalize/filter/score/dedup)      ← processing lag (< 50 ms target)
 t4  the human's phone buzzes (Discord/Telegram push)                            ← delivery lag  ≈ 0.2-1 s
```

For a poller with interval `T` and uniform arrival, the polling lag is uniformly
distributed on `[0, T]`: mean `T/2`, p95 `0.95·T`. **Polling lag dominates
everything else by two to four orders of magnitude**, so the engine is designed
around three levers:

1. **Watch the earliest observable signal** (`t1` as close to `t0` as possible):
   first-party APIs and the *forum* feed instead of curated frontpages; seller
   marketplaces sorted by creation time.
2. **Make each poll cheap so `T` can be small inside every quota**: narrow queries,
   persistent keep-alive connections, conditional requests where honoured,
   change-detection so unchanged results cost zero downstream work, and
   quota-aware schedulers that spend the whole budget evenly.
3. **Never get blocked** — a 403/429/checkpoint turns `T` into hours. Politeness
   (per-host token buckets, one leader poller per source, consistent client
   identity) is a latency feature, not a courtesy.

The `< 50 ms` internal-processing target is real and measured
(`dealradar_pipeline_internal_ms` histogram), but it is the smallest term in the
budget. Anyone claiming "zero delay" from network micro-optimisation is optimising
the wrong term.

### 1.1 Why Twitter / public aggregators are late

| Stage | Typical delay | Cause |
|---|---|---|
| Human or bot discovers the deal | minutes | most public bots poll curated feeds (Slickdeals frontpage, Reddit hot) rather than origins |
| Curation / promotion | **3-12 h** for Slickdeals frontpage | the frontpage feed's `pubDate` is the *promotion* time, not thread creation (verified) |
| Affiliate wrapping | 30 s – 5 min | links rewritten through Amazon Associates / Impact / CJ / Rakuten, sometimes batched or manually reviewed |
| Gatekeeping | arbitrary | private "cook groups" monetise exclusivity and post publicly later, if at all |
| Social delivery | seconds – minutes | timeline ranking, API posting limits, followers not watching |

DealRadar skips every one of these by reading the sources the aggregators
themselves read.

---

## 2. Latency hierarchy, platform by platform

| Tier | Signal | Visibility lag (t0→t1) | DealRadar poll `T` | Mean detection lag |
|---|---|---|---|---|
| **0** | First-party retailer endpoints (Best Buy Products API, Shopify product JSON, generic JSON) | seconds (CDN TTL / API refresh) | 15-20 s per endpoint | ≈ 10 s |
| **0** | eBay Browse `sort=newlyListed` | seconds-minutes (search index) | ≈ 3 min, quota-derived (§2.2) | ≈ 1.5 min |
| **0-1** | Facebook Marketplace search sorted by creation time (home IP) | FB listing review ≈ 1-10 min | 4 min ±15 % | ≈ 2 min + FB review |
| **1** | Slickdeals **Hot Deals forum** RSS (forum 9) | **0-22 s** after thread creation (measured) | 30 s | ≈ 15-35 s |
| **1** | Reddit `/new` via OAuth (r/buildapcsales, r/hardwareswap) | ≈ 10-30 s | 6 s | ≈ 15-35 s |
| **2** | Slickdeals frontpage / popular RSS | 3-12 h (promotion time) | minutes | quality signal, not speed |
| **3** | Twitter/X bots, public Discord/Telegram channels | minutes – hours | — | not used |

### 2.1 First-party retailer endpoints (`sources/retail_endpoints.py`)

*Where the delay is.* The price lives in the retailer's pricing service; what you
can see is a cached rendering of it. Three layers add delay: the HTML page cache
(minutes, and the heaviest response), the CDN edge cache for product JSON (seconds
to minutes), and the API's own refresh cadence.

*What we do.*
- **Official APIs first.** Best Buy's Products API returns `salePrice`,
  `onlineAvailability` and `priceUpdateDate` for up to 100 SKUs per call. A batched
  JSON call is ~1-3 KB per product instead of ~1 MB of HTML, with no bot wall.
- **Shopify storefronts** expose `/products/<handle>.js` (integer cents) and
  `/collections/<c>/products.json` (decimal strings) without authentication.
  This is the classic low-latency monitor technique for drops on Shopify stores.
- **Generic JSON adapter.** Any JSON endpoint (including an app backend that the
  operator is entitled to use) can be mapped with dotted paths in YAML. Adding a
  store needs no code.
- **Keep-alive + DNS cache.** One process-wide `aiohttp` connector reuses TLS
  sessions per host. A cold HTTPS request costs DNS + TCP + TLS 1.3 = 2-3 RTT before
  the first byte; a warm one costs 1 RTT.
- **Conditional requests** (`If-None-Match` / `If-Modified-Since`) when the origin
  honours them. A 304 is a ~200-byte "nothing changed".
- **Cache-busting, honestly.** Cloudflare, Fastly and Akamai include the query
  string in their default cache key, so a nonce parameter (`?_=<ns>`) does force an
  *edge* miss. In practice it rarely buys fresher data:
  * Cloudflare does not cache HTML/JSON by default. Retailer JSON was already
    uncached (`cf-cache-status: DYNAMIC` on Shopify and Slickdeals, verified).
  * The real staleness often lives in an **application cache behind the CDN**.
    Newegg's `ProductRealtime` JSON advertises `max-age=10`, but the `Age` header
    shows a ~60-65 s internal refresh that ignores random parameters and `no-cache`
    request headers.
  * Every busted request is extra origin load with a bot-like signature.

  Cache-busting is therefore **opt-in per endpoint** (`cache_bust: true`). The
  better tools are conditional requests where an ETag exists (Shopify returns a
  weak `page_cache` ETag and a genuine 304), reading the `Age` header to learn the
  true refresh interval, and polling at that interval. Request-side
  `Cache-Control: no-cache` has no documented effect and is not used.
- **Mobile app backends.** Retail apps often call private REST/GraphQL APIs that
  skip HTML caches. DealRadar supports them through the generic JSON adapter, but
  ships no reverse-engineered private endpoints. They change without notice, they
  are usually covered by the app's terms, and fighting a bot manager with a Python
  TLS fingerprint is a losing, account-endangering game. The supported fast path is
  official APIs plus the generic adapter.

*Reachability from a GCP IP (probed live, 2026-10-06):*

| Endpoint | Result | Use |
|---|---|---|
| Best Buy Products API (`api.bestbuy.com`) | ✅ 5 req/s, 50k/day. Quota exhaustion is a **403**, like a bad key. Keys are not issued to free-mail addresses. | primary |
| Shopify `/products/<h>.js`, `/products.json` | ✅ uncached at edge; ETag → 304. Sporadic 429 + Cloudflare challenge HTML. | primary |
| Newegg `ProductRealtime` | ✅ JSON; ~60 s app-level cache | poll ≥ 60 s |
| Target RedSky | ❌ **HTTP 435** HUMAN/PerimeterX block JSON (must not be parsed as "out of stock") | home node only |
| bestbuy.com web, Walmart, B&H, Micro Center, Amazon pages | ❌ Akamai reset / PerimeterX / Cloudflare challenge / captcha | not polled |
| Amazon PA-API 5.0 | ❌ deprecated (403); successor Creators API needs 10 qualifying sales/30 days | via generic adapter if eligible |

### 2.2 eBay (`sources/ebay_api.py`)

*Facts (verified).* The Finding and Shopping APIs were shut down on 2025-02-04, so
the **Browse API** is the only open way to search active listings. Endpoint:
`GET /buy/browse/v1/item_summary/search` with an application token from the
client-credentials grant (`expires_in` 7200 s, at most 1,000 mints/day). The default
quota is **5,000 calls/day per keyset**, shared by every worker and reset at
midnight America/Los_Angeles. eBay also has an unpublished short-burst throttle
(HTTP 429, `errorId` 2001). There is no push API for "new listing matches search".
The Feed API lags 2-48 h, and Marketplace Insights (sold comps) is closed to new
users.

*Where the delay is.* Search-index propagation (seconds) and, above all, **the quota**.

*What we do.* `sort=newlyListed`, with price/condition/buying-option filters and
one leaf category per request. The response then contains only candidate listings
and the newest ones first. A **quota-aware scheduler** spreads the budget evenly:

```
calls_per_poll   = Σ_profiles |search.terms|            (one call per term)
polls_per_day    = daily_call_budget · safety_factor / calls_per_poll
T_ebay           = max(poll_interval_seconds, 86 400 / polls_per_day)
```

Per-profile searches would cost one call per search term: 21 terms in the shipped
profiles, so `T ≈ 86 400 · 21 / 4 250 ≈ 427 s`. The shipped config instead **coalesces**
them into 9 `sources.ebay.queries` using eBay's OR syntax (`rtx (5090, 4090, 3090)` means
"rtx" AND any of the three). That gives `T ≈ 86 400 · 9 / 4 250 ≈ 183 s` and a mean lag
of ≈ 1.5 min on the free quota. The text filter assigns profiles afterwards, so wide
queries cost nothing in precision. The free Application Growth Check raises the quota.
The application token (≤ 1,000 mints/day) is cached and shared through Redis,
refreshed 5 minutes before expiry, single-flight. A fleet-wide Redis ledger counts
calls against the quota, which resets at midnight America/Los_Angeles.

### 2.3 Slickdeals (`sources/slickdeals_rss.py`)

*Facts (verified live).* All RSS feeds are generated per request (Cloudflare
`cf-cache-status: DYNAMIC`). Each returns exactly 25 items, has **no
ETag/Last-Modified**, and ignores conditional headers. The **Hot Deals forum feed**
(`newsearch.php?searchin=first&forumchoice[]=9&rss=1`) surfaced new threads
**0-22 s after creation**. The frontpage feed's `pubDate` is the promotion time,
3-12 hours later. Keyword-search RSS is fuzzy (matches body text), and
`robots.txt` disallows `/newsearch.php?*rss=*`.

*What we do.* We poll the forum feed every 30-45 s and the frontpage feed (as a
quality signal) far less often. Change detection runs in our process because the
server offers none. Search feeds are off by default and strict local regex
filtering applies to everything. A Redis lease elects a single poller across the
fleet.

### 2.4 Reddit (`sources/reddit_stream.py`)

*Facts (verified, and time-limited).* Unauthenticated `.json` has been blocked since
2026-05-28 (HTTP 403 "blocked by network security" from datacenter IPs). Logged-out
RSS retires on **2026-11-13**. New Data API access requests close on **2026-10-31**,
unregistered apps lose access on 2027-01-12, and all public Data API access ends in
**March 2027**. App-only OAuth gives **100 QPM per client id**, shared by all
workers. Reddit forbids spoofing browser User-Agents.

*What we do.* OAuth app-only polling of `/r/<sub>/new?raw_json=1` every 6 s with an
honest User-Agent, one leased leader, and fullname-based dedup. We never use the
fragile `before` cursor: removed anchor posts make it return stale slices.
r/hardwareswap posts are parsed from `[H]/[W]` titles and markdown bodies, and
are emitted as `LOCAL` listings so they get the local-marketplace risk policy.
The source is behind a kill-switch and is **disabled unless OAuth credentials are
configured**.

### 2.5 Facebook Marketplace, OfferUp, Craigslist (`sources/fb_marketplace.py`, `offerup.py`, `craigslist.py`)

*Where the delay is.* Marketplace's own listing review (minutes) and the
platform's tolerance for automated sessions. The real risk is not latency but a
checkpoint that stops the source for hours or costs the account.

*What we do.*
- **Real browser, persistent session.** One Chromium context restored from
  `storage_state.json`, logged in once by the operator through the `--login` helper.
  The page is reused across polls, because launching browsers per poll is slow and
  looks suspicious.
- **Read the data layer, not the DOM.** The first batch of results is embedded in
  the page as Relay JSON (`<script type="application/json" data-sjs>`). Later batches
  arrive as `POST /api/graphql/` responses, which may start with `for (;;);` and
  hold several JSON documents per body. Both share the path
  `data.marketplace_search.feed_units.edges[].node.listing` (`id`,
  `marketplace_listing_title`, `listing_price.amount` in major units,
  `primary_listing_photo.image.uri`, `location.reverse_geocode`, `is_sold` /
  `is_pending`). We walk the JSON generically for listing nodes, so path and
  class-name churn do not matter. The DOM grid is virtualised and is only a fallback.
- **Logged in, on purpose.** In 2026 logged-out search redirects to `/login` for
  most clients. When logged in, Facebook ignores the URL `radius` and uses the
  account's saved radius. An unknown city slug silently redirects to the account
  location, so the page URL is re-checked after navigation. `fbcdn` photo URLs
  expire, so vision runs promptly.
- **Consistency over randomisation.** IP geolocation (a home connection — the
  laptop node), browser geolocation pin, `timezone_id`, `locale` and
  `Accept-Language` all agree. The User-Agent is not rotated for a logged-in
  session, because cookies are bound to the device fingerprint and a changing UA
  invalidates trust. The stealth init script only removes automation tells
  (`navigator.webdriver`, empty plugin arrays, missing `window.chrome`).
- **Human pacing and backoff.** Searches are sequential with randomised pauses,
  a 4-minute interval and 1-day recency. A checkpoint or login wall raises
  `SourceBlocked`, which pauses the source for hours and notifies the operator. It
  does not retry harder.
- **OfferUp:** the search page's `__NEXT_DATA__` (`searchFeedResponse.looseTiles`),
  or its Apollo endpoint `POST /api/graphql` (`GetModularFeed`). Location comes from
  the `ou.location` cookie. Feed tiles carry no post date.
- **Craigslist:** RSS is dead (403). The site's own JSON API
  (`sapi.craigslist.org/web/v8/postings/search/full`) returns positional arrays
  decoded against `data.decode` (`postingId = minPostingId + item[0]`). It is polled
  at ≤ 1 request/s from the home node.

*What we deliberately don't do:* CAPTCHA solving, account rotation or farming,
residential proxy pools, or TLS-fingerprint impersonation. Each of these turns a
personal tool into abuse infrastructure, and none makes alerts meaningfully faster
than a well-paced honest session.

### 2.6 Delivery (`dispatchers/`)

Discord webhooks and the Telegram Bot API typically reach a phone in ~1-3 s.
Neither publishes latency numbers, and the dominant risk is client-side
notification settings, not the API. The dispatchers never block one another: routes
fan out concurrently with a per-target timeout. Verified specifics:

* **Discord.** Webhooks are called on the versioned `/api/v10/` path. Link buttons
  (style 5) on non-application webhooks render **only** with `?with_components=true`;
  otherwise they are silently dropped. The 6,000-character embed budget is shared by
  all embeds in a message. `allowed_mentions` is always sent explicitly, so a listing
  title containing `@everyone` can never ping anyone. Rate limits are read from
  `X-RateLimit-*` headers (per-webhook limits are undocumented). 401/403/404
  disable the target permanently, because webhook calls are unauthenticated and
  Cloudflare bans IPs after 10,000 invalid requests in 10 minutes.
* **Telegram.** HTML parse mode with `&`, `<` and `>` escaped; text ≤ 4,096 and
  captions ≤ 1,024 characters after entity parsing. Limits are ~1 msg/s per chat and
  20 msg/min per group. A 429 carries an integer `parameters.retry_after`. A photo URL
  Telegram cannot fetch (`failed to get HTTP URL content`) falls back to `sendMessage`
  with a link preview.

Price errors ping a role (Discord) and bypass silent mode (Telegram). Medium alerts
are delivered silently.

---

## 3. Resource orchestration: Google Cloud vs. the local GPUs

```
                         Tailscale (WireGuard mesh, no public ports)
   ┌──────────────────────────────────────────┐        ┌───────────────────────────────┐
   │ GCP VM  us-east1  e2-micro (Always Free)   │        │ Desktop · RTX 3060 12 GB       │
   │  ├ Valkey 8: dedup Lua, Streams bus, leases│◄──────►│  └ Ollama vision API           │
   │  ├ collectors: eBay, Slickdeals, Reddit,   │ HTTP   │     qwen3-vl:4b (+8b escalate) │
   │  │   retail endpoints                      │        └───────────────────────────────┘
   │  ├ processor: filter · score · dedup ·     │        ┌───────────────────────────────┐
   │  │   dispatch, SQLite/WAL history          │◄──────►│ Laptop · RTX 3080 8 GB         │
   │  └ ops: /metrics /status /ws               │ XADD   │  ├ collector: FB Marketplace,  │
   └──────────────────────────────────────────┘ stream │  │   OfferUp (residential IP)   │
                                                         │  └ standby lease holder +     │
                                                         │     fallback vision model     │
                                                         └───────────────────────────────┘
```

| Component | Where | Why there |
|---|---|---|
| Valkey (dedup, stream, leases) | GCP VM | always on, same host as the processor (sub-ms Lua), single source of truth |
| API/feed collectors (eBay, Slickdeals, Reddit, retail) | GCP VM | always on, stable egress IP for official APIs. **Region: us-east1** (Always Free). us-east4/Ashburn is ~10-15 ms closer to AWS us-east-1, but every target answers from a CDN edge 1-10 ms from any US-East region, and server think time (100-500 ms) and poll intervals (seconds) dwarf it. The "RTT < 5 ms to retail origins" goal is not meaningful for edge-served traffic, so the free tier wins. Telegram's Bot API is in Amsterdam (~75-90 ms from US-East) regardless of region. |
| Processor + history DB | GCP VM | needs every collector's stream; the history model is in-memory with a SQLite (WAL) or Postgres warm start |
| Vision model (Ollama) | Desktop RTX 3060 12 GB | free inference; 12 GB holds a 4B VLM and an 8B escalation model resident at Q4 (`OLLAMA_MAX_LOADED_MODELS=2`; verify with `ollama ps`) |
| FB Marketplace / OfferUp collectors | Laptop RTX 3080 8 GB | residential IP that matches the logged-in session's history and location |
| Fallback | Laptop | holds standby leases for API sources (takes over within `lease_ttl_seconds` if the VM dies); can serve `qwen3-vl:4b-instruct` (Q4 fits 8 GB) if the desktop is off |

**Vision is on the critical path only where it pays.** It runs only for `LOCAL`
listings that survived the text filter *and* have a preliminary score ≥
`vision.min_prelim_score`. That is typically a handful per hour, not thousands.

Realistic numbers (2026 benchmarks: PhotoPrism, Ollama 0.32-0.35, 720 px, 8-12 GB
cards):

| Model (Ollama tag) | Short-answer p50 | Notes |
|---|---|---|
| `minicpm-v4.6:1b` | ≈ 0.6 s | smallest; weakest on fine detail |
| `gemma4:e2b` | ≈ 0.7 s | ≈ 208 visual tokens per image: cheapest prefill |
| `qwen3.5:4b` | ≈ 0.9 s | **thinks by default — must send `think: false`** (6.4 s otherwise) |
| `qwen3-vl:4b-instruct` | ≈ 1.2 s | default primary (no thinking); `qwen3-vl:8b-instruct` for escalation |

* **< 200 ms is not achievable with a generative VLM returning JSON.** A 3060 does
  ≈ 0.25-0.4 s per image for a single-token answer and ≈ 0.5-1.5 s for a small JSON
  object. Sub-200 ms needs an encoder classifier (SigLIP2 zero-shot / linear probe)
  or Florence-2-base captioning with greedy decoding. That is a future pre-filter;
  the gate placement above makes the current latency acceptable.
* **Image tokens drive latency.** Qwen-family models spend one token per 28-32 px
  tile, so images are downscaled to ≤ 512 px before upload. `num_ctx` stays constant
  (changing it reloads the model) and `keep_alive` keeps the weights resident.
* **Confidence is self-reported and uncalibrated.** A schema-constrained answer
  guarantees the shape, not the truth. Negative verdicts need ≥ 0.6 confidence, and
  a single box photo next to genuine photos of the card does not condemn a listing.
* Ollama has no authentication: it binds to the desktop's tailnet IP only. A 503
  from its queue (`OLLAMA_MAX_QUEUE`) degrades to `UNCERTAIN` rather than retrying.

Verdicts are cached per image URL, so re-observations never re-infer. Facebook CDN
photo URLs are signed and expire, so vision runs within seconds of discovery.

---

## 4. Ingestion engineering details

* **Unified contracts** (`engine/types.py`): sources emit `RawListing` (prices may
  still be strings like `"$1,199 OBO"`). The normalizer is the single owner of
  price, condition, URL and text parsing.
* **Change detection** (`sources/base.py`): each ingestor keeps an LRU of
  `listing_key → (price, shipping, stock, title)`. Only new or changed listings are
  emitted. A 100-result eBay page with no changes therefore costs zero pipeline work.
* **Stale guard**: `max_item_age_minutes` drops old feed items on restart.
* **Failure isolation**: per-source circuit breaker, exponential backoff with full
  jitter, setup retries, and `SourceBlocked` / `SourceAuthError` for long pauses with
  operator notices.
* **Bus** (`engine/bus.py`): in single-node mode it is an in-process bounded queue
  (backpressure). In distributed mode it is a Redis Stream consumer group. Collectors
  `XADD`, processors `XREADGROUP`, and a crashed processor's un-acked entries are
  reclaimed with `XAUTOCLAIM`.
* **Leases** (`SourceLease`): `SET key node NX PX ttl` with compare-and-extend in Lua
  gives active/passive fail-over per source. Only the lease holder polls, so quotas
  shared per API key (eBay, Reddit) are never double-spent.

---

## 5. Anomaly scoring: `S ∈ [0, 100]`

Static thresholds ("4090 < $900") rot as hardware depreciates and cannot recognise
glitches on variants nobody configured. DealRadar combines a static prior with
robust, time-decayed statistics of what it actually observes. The implementation is
`engine/anomaly.py`; its docstring is the normative definition.

### 5.1 Inputs

| Symbol | Meaning |
|---|---|
| `P` | item total price (price + known shipping) |
| `band` | profile/variant price band: `reference_{new,refurb,used}`, `floor`, `target`, `ceiling` |
| `h` | history samples for `product_key | condition_class`, one sample per listing (updates replace) |
| `w_i` | sample weight `= source_weight(src_i) · 0.5^(age_i / half_life)` (half-life 14 d), window 60 d, ≤ 400 samples |
| `n_eff` | Kish effective sample size `(Σw)² / Σw²` |
| `med, Q1, Q3, MAD` | weighted median, quartiles, weighted median absolute deviation |

### 5.2 Market reference (shrinkage from prior to evidence)

```
c_hist = 1 − exp(−n_eff / n0)                      n0 = reference_prior_strength = 8
M      = c_hist · med + (1 − c_hist) · ref          when n ≥ min_samples_for_stats (6)
M      = ref                                        otherwise (config prior only)
```

With two samples the market price stays near the configured reference. With ~50
consistent observations it is ≈ the observed median. This is how the model follows
depreciation without anyone editing the config.

### 5.3 Components

```
Δ      = (M − P) / M                                         discount vs. market
D      = clamp(Δ / Δ_sat, 0, 1)                              Δ_sat = 0.45
z      = (med − P) / max(1.4826 · MAD, 0.02 · med)           robust z-score (MAD scaled to σ)
Zc     = clamp((z − z_lo) / (z_hi − z_lo), 0, 1)             z_lo = 1, z_hi = 4
I      = clamp((Q1 − P) / (3 · IQR), 0, 1)                   0 at Q1, 0.5 at the Tukey mild fence, 1 at the extreme fence
S_stat = (Zc + I) / 2                                        (= D while n < 6)
T      = 1 if P ≤ target; 0 if P ≥ ceiling; else (ceiling − P)/(ceiling − target)
O      = w_d·D + w_s·S_stat + w_t·T                          (0.45, 0.30, 0.25)
```

*Why robust statistics.* Scams and bait listings are themselves outliers. A mean
and standard deviation would be dragged down by the very prices we want to flag.
The median and MAD have a 50 % breakdown point, and `0.02·med` floors the scale so
a run of identical prices cannot divide by zero.

### 5.4 Confidence

```
c_src   = source reliability (retail 1.0, eBay 0.95, Reddit 0.9, Slickdeals 0.88, FB/OfferUp 0.7, Craigslist 0.65)
          + (1 − c_src) · vision_trust_boost   when the vision model saw the genuine item (conf ≥ 0.6)
c_match = text-match strength (1.0, −0.15 unknown variant, −0.1 ambiguous profile)
c_ref   = 0.5 + 0.5·c_hist (history) | 0.5 (config prior) | 0.35 (sparse history, no prior) | 0.25 (nothing)
C       = c_src · c_match · c_ref
```

### 5.5 Scam risk (noisy-OR)

Independent risk signals `r_i` (probabilities from `scoring.risk_probabilities`)
combine as `R = 1 − Π(1 − r_i)`:

| Signal | r | Trigger |
|---|---|---|
| `bait_price` | 0.95 | `P < floor` ($1 bait, accessory priced as the product) |
| `placeholder_price` | 0.60 | seller-priced sources (local/eBay): `P ∈ {1, 1234, 9999, 12345, …}`; any source: `P ≤ 5` |
| `extreme_discount_low_trust` | 0.50 | `Δ ≥ 0.55` on a source with reliability < 0.8 |
| `title_price_mismatch` | 0.45 | title advertises a price outside ×0.5–×2 of the listing price |
| `low_feedback` / `poor_feedback_pct` | 0.35 / 0.30 | eBay seller < 5 feedback / < 97 % positive |
| `no_image` | 0.15 | local listing without photos |
| text rule groups | 0.08–0.60 | e.g. `payment_only_red_flag` (Zelle/crypto only) 0.6, `deposit_request` 0.55, `untested_as_is` 0.4 |
| `vision_box_only` / `vision_parts_only` / `vision_damaged` | 0.95 / 0.95 / 0.90 | negative verdict with confidence ≥ 0.6 |

Hard-reject rules ("box only", "for parts", rentals, `$50/mo`, WTB, accessories,
sold markers) never reach the scorer; they are deterministic regex groups in the
text filter, with negation masking ("no cracks", "never mined").

### 5.6 Final score, gates and severity

```
S = 100 · O · (λ + (1 − λ)·C) · (1 − R)^γ          λ = 0.4,  γ = 2
```

* `λ` keeps a low-confidence observation from being zeroed: a strong discount seen
  on a weak source is down-weighted, not ignored.
* `γ = 2` penalises risk super-linearly: `R = 0.3` costs 51 % of the score.
* **Hard gates** (score kept for analytics, no alert): `P > ceiling`,
  `R ≥ 0.85` (probable scam), `P ≤ 0`, explicitly out of stock.
* **Price error**: `Δ ≥ 0.5` on a source with reliability ≥ 0.9, `R ≤ 0.3`, and
  condition new or refurbished. The result is forced to CRITICAL with
  `is_price_error = true`, and the score is raised to `max(S, 85)`. The model's own
  value is kept in `components.S_model`, because a config-only reference caps
  confidence at 0.5, so even a 60 %-off first-party glitch would otherwise read ≈ 70.
* **Severity**: CRITICAL ≥ 85, HIGH ≥ 70, MEDIUM ≥ max(55, `profile.min_score`).

Worked examples computed by the shipped implementation are in §5.7.

### 5.7 Worked examples (generated by `engine/anomaly.py` with the shipped config)

Setup for B–H: 30 observed used RTX 4090 sales around $1,700 (σ ≈ $90) from eBay and
Reddit over the last 30 days. The resulting market reference is
`M = 0.961·median + 0.039·1750 ≈ $1,721` (`c_hist = 1 − e^{−n_eff/8} ≈ 0.96`).

| # | Scenario | P | Δ | D | S_stat | T | C | R | **S** | Outcome |
|---|---|---|---|---|---|---|---|---|---|---|
| A | Best Buy API, RTX 5090 FE, new | $999.99 | 0.58 vs $2,399 (config) | 1.00 | 1.00 | 1.00 | 0.50 | 0 | **85** (model 70) | CRITICAL **price error** |
| B | FB Marketplace RTX 4090 FE | $1,150 | 0.33 | 0.74 | 1.00 | 1.00 | 0.69 | 0 | **71.6** | HIGH |
| C | B + vision GENUINE (0.9) | $1,150 | 0.33 | 0.74 | 1.00 | 1.00 | 0.83 | 0 | **79.4** | HIGH (trust boost: c_src 0.70 → 0.85) |
| D | B + "Zelle only, shipping only" | $1,150 | 0.33 | 0.74 | 1.00 | 1.00 | 0.69 | 0.77 | **3.8** | dropped (payment 0.6 ⊕ story 0.35 ⊕ mention 0.12) |
| E | $1,234 placeholder | $1,234 | 0.28 | 0.63 | 1.00 | 1.00 | 0.69 | 0.60 | **10.8** | dropped |
| F | $120 bait | $120 | 0.93 | 1.00 | 1.00 | 1.00 | 0.69 | 0.975 | 0.1 | **rejected: scam_risk** |
| G | B + vision BOX_ONLY (0.95) | $1,150 | 0.33 | 0.74 | 1.00 | 1.00 | 0.69 | 0.95 | 0.2 | **rejected: scam_risk** |
| H | eBay seller with 0 feedback | $1,250 | 0.27 | 0.61 | 1.00 | 1.00 | 0.93 | 0.35 | **33.4** | below threshold (would be ≈ 79 from an established seller) |

Reading the table:

* **A** shows why the price-error rule exists. With only a config prior, confidence is
  capped at 0.5, so the model alone says 70. A 58 %-off *first-party, new, in-stock*
  observation with zero risk is exactly the glitch DealRadar is built to catch, and is
  forced to CRITICAL.
* **B → C** is the vision boost: the same listing gains 8 points once the local GPU
  confirms the card is really in the photo.
* **D–G** show the multiplicative `(1 − R)²` penalty dominating any discount. Scams are
  *cheap by construction*, so discount size alone can never page anyone.

---

## 6. Distributed deduplication: exactly one alert, < 2 ms

**Problem.** Several workers can observe the same deal at once: a retail endpoint
poll, the Slickdeals thread, the Reddit post, the eBay listing on two nodes during a
lease hand-over. We want **one** alert per deal, a **new** alert when the price drops
meaningfully, and no lost alert if delivery fails.

**Mechanism** (`engine/dedup.py`): a single Lua script executed with `EVALSHA` runs
atomically inside Valkey, so there is no check-then-set race:

```
listing key   dr:{alerts}:l:<source>:<id>          hash {price, ts, token}     TTL 72 h
cluster key   dr:{alerts}:c:<product>:<retailer>:<bucket>                       TTL 12 h
bucket        = floor( ln(P) / ln(1 + 0.02) )       ← 2 %-wide log buckets; b−1, b, b+1 are checked

claim(P):
  if listing exists:
      if P ≤ prev·(1 − 5 %) and prev − P ≥ $10 → update, return PRICE_DROP(prev)
      else                                       → return DUPLICATE(prev)
  if any cluster key b−1..b+1 exists            → return CROSS_SOURCE_DUPLICATE
  set listing {P, token} + cluster key b        → return NEW
```

* **Price drops** re-alert ($1,400 → $800 is a drop of 43 %); flapping and price
  rises do not.
* **Cross-source suppression** keys on product + retailer identity + log price
  bucket. The same Best Buy deal arriving via Slickdeals and Reddit is one alert.
  Marketplace and local listings never cluster: two different sellers at the same
  price are two deals.
* **Effectively-once delivery.** True exactly-once delivery is impossible across a
  network. The claim is at-most-once and carries a token. If *every* dispatch target
  fails, the pipeline calls `rollback`, a second Lua script that restores the
  previous state only if the token still matches, so a newer concurrent claim is
  never clobbered. The next observation can then alert again.
* **Hash tag `{alerts}`** keeps every key the script touches in one cluster slot,
  so the script stays valid on Redis Cluster.
* **Latency.** One round trip on the same VM is ≈ 0.2-1 ms. Over Tailscale from a
  home collector it is one RTT, but collectors never run dedup; processors do.
* **Degradation.** If Valkey is unreachable, the processor falls back to a
  process-local deduplicator with identical semantics. It logs loudly and keeps
  alerting; the guarantee shrinks from fleet-wide to per-process.

---

## 7. Pipeline and observability

```
RawListing ─► normalize (≈20 µs) ─► text filter (≈50-300 µs) ─► prelim score (≈0.1 ms)
          ─► [vision, LOCAL candidates only, ≈0.3-1.5 s] ─► final score ─► history + snapshot (async batch)
          ─► severity gate ─► Lua claim (≈1 ms) ─► router ─► Discord ║ Telegram ║ WebSocket (concurrent)
```

* `dealradar_pipeline_internal_ms` — internal overhead per listing (excludes vision
  and dispatch I/O); the test-suite asserts p95 < 50 ms.
* `dealradar_alert_end_to_end_ms` — collector receive → dispatch complete.
* `dealradar_source_poll_ms`, `dealradar_source_up`, `dealradar_http_request_ms{host}`,
  `dealradar_dispatch_ms{target}`, `dealradar_pipeline_outcomes_total{stage,reason}`.
* `/status` returns per-source health (state, consecutive failures, last error,
  next poll), bus backlog and history size.

---

## 8. Compliance and failure modes

| Area | Position |
|---|---|
| eBay | Official API under the API License. The license restricts deriving statistics from eBay data, so eBay's weight in the shared price model is configurable (`scoring.source_history_weights.ebay`; set it to 0 to compare eBay listings only against other sources and config references). Production keysets must handle Marketplace Account Deletion notifications or obtain an exemption. |
| Reddit | OAuth only, honest User-Agent, one leader, ≤ 100 QPM. Hard end dates are documented in §2.4; the source is a kill-switch away from removal. |
| Slickdeals | ToS (effective 2026-03-05) restricts automated access, and `robots.txt` disallows search RSS. The defaults poll only the moderator-published forum/frontpage feeds at low rates, with a single poller. Alerts are private. |
| Facebook / OfferUp / Craigslist | Personal-use session automation on the operator's own account and connection, conservative pacing, immediate stand-down on checkpoints. Review each platform's terms before enabling. |
| Secrets | `.env` / Secret Manager only; `SecretStr` everywhere; tokens are never logged (the Telegram token is in the URL path and is redacted). |
| Exposure | No public ports: SSH through IAP, Valkey/Ollama/ops bound to Tailscale or loopback, ops endpoints token-protected. |

| Failure | Behaviour |
|---|---|
| Source 5xx / timeouts | jittered exponential backoff → circuit breaker → operator notice; other sources unaffected |
| Checkpoint / 403 wall | `SourceBlocked`: long pause + notice; no retry storm |
| Valkey down | processor: local dedup fallback. Collector-only node with Redis bus: publish fails, the source backs off. |
| Vision box down | circuit breaker → `ERROR` verdict → `on_error: allow` adds a small `vision_unverified` risk (or `reject`) |
| All dispatch targets down | dedup claim rolled back; the alert is retried on the next observation |
| Processor crash | un-acked stream entries reclaimed by `XAUTOCLAIM` on restart or by another processor |
| VM loss | laptop standby leases take over API sources within `lease_ttl_seconds` |
