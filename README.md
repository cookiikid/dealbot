# DealRadar

**Autonomous, self-hosted, low-latency deal & pricing-error detection engine.**

DealRadar watches first-party retailer endpoints, the eBay Browse API, community deal
feeds (Slickdeals' Hot Deals forum, Reddit) and local marketplaces (Facebook
Marketplace, OfferUp, Craigslist). It normalizes every listing into one schema,
rejects scams and noise with deterministic regex rules (plus an optional local GPU
vision check), scores price anomalies with robust statistics, and pushes alerts to
Discord, Telegram and a WebSocket feed. Alerts are deduplicated exactly once across
a fleet of workers with an atomic Redis Lua state machine.

```
 sources ──► RawListing ──► Redis Stream / in-proc queue ──► processor
  eBay Browse API            (collector nodes)                normalize → text filter → score
  Slickdeals forum RSS                                        → [vision on RTX 3060] → re-score
  Reddit (OAuth)                                              → atomic dedup claim (Lua)
  Best Buy / Shopify / JSON                                   → router → Discord / Telegram / WS
  FB Marketplace (Playwright, home IP)
  OfferUp / Craigslist
```

| Document | What it covers |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Phase 1: latency hierarchy per platform, cloud vs. local GPU resource split, the anomaly-scoring math, distributed dedup design |
| [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) | Step-by-step local setup and Google Cloud deployment on a $200 credit |
| [`deal_radar/config.yaml`](deal_radar/config.yaml) | The master configuration (profiles, rules, scoring, routes), heavily commented |

## Repository layout

```
deal_radar/
├── config.yaml             # master config: profiles, price bands, negative regex, routes
├── config_schema.py        # Pydantic v2 models — unknown keys and bad regexes fail fast
├── main.py                 # orchestrator: roles, warm start, graceful shutdown, CLI modes
├── core/                   # shared infrastructure
│   ├── http.py             #   tuned aiohttp client: identities, ETag cache, backoff, host limits
│   ├── backoff.py          #   full-jitter exponential backoff, Retry-After handling
│   ├── ratelimit.py        #   token buckets, per-host registry, circuit breaker
│   ├── metrics.py          #   Prometheus text exposition (zero deps)
│   ├── logs.py             #   structured JSON logging
│   └── server.py           #   /healthz /readyz /metrics /status /alerts/recent
├── db/
│   ├── database.py         # async SQLite/PostgreSQL (SQLAlchemy 2), batch recorder, Redis pool
│   └── models.py           # listings, price_snapshots, alerts
├── sources/
│   ├── base.py             # abstract ingestor: polling loop, change detection, leases, health
│   ├── ebay_api.py         # eBay Browse API, OAuth client-credentials, quota-aware scheduling
│   ├── reddit_stream.py    # r/buildapcsales + r/hardwareswap via OAuth
│   ├── slickdeals_rss.py   # Hot Deals forum / frontpage RSS with spec extraction
│   ├── retail_endpoints.py # Best Buy API, Shopify JSON, Target RedSky, Newegg, generic JSON
│   ├── fb_marketplace.py   # Playwright, persisted session, GraphQL interception, geo-pinning
│   ├── stealth.py          # consistent browser fingerprint + human pacing helpers
│   ├── offerup.py          # OfferUp search JSON
│   ├── craigslist.py       # Craigslist search API
│   └── registry.py         # lazy source registry
├── engine/
│   ├── types.py            # the data contracts (RawListing, DealItem, ScoreResult, Alert...)
│   ├── normalizer.py       # price/condition/URL/text normalization → DealItem
│   ├── text_filter.py      # compiled regex rules, negation-aware, profile/variant matching
│   ├── vision_filter.py    # Ollama / OpenAI-compatible (vLLM) local vision client
│   ├── anomaly.py          # robust, depreciation-aware anomaly scoring S ∈ [0, 100]
│   ├── dedup.py            # Redis Lua once-only claims, price-drop re-alerts, rollback
│   ├── bus.py              # in-process queue or Redis Streams consumer group
│   └── pipeline.py         # per-listing pipeline + bounded worker pool
├── dispatchers/
│   ├── router.py           # severity/category routes, quiet hours, flood guard
│   ├── discord.py          # rich embeds + link buttons, 429-aware
│   ├── telegram.py         # HTML messages/photos + inline buttons
│   ├── websocket.py        # live JSON alert feed with replay
│   └── base.py             # channel interface + shared formatting
├── deploy/
│   ├── Dockerfile          # multi-stage: runtime / browser targets
│   ├── docker-compose.yml  # Valkey + DealRadar + browser collector + Ollama
│   ├── gcp_deploy.sh       # Compute Engine (+ optional Cloud Run) deployment
│   ├── vm_startup.sh       # VM bootstrap: Docker, swap, sysctls, Tailscale
│   └── .env.example
├── tests/                  # offline test-suite (pytest)
├── requirements.txt
└── requirements-dev.txt
```

## Quick start (local, five minutes)

```bash
python3.11 -m venv .venv && . .venv/bin/activate        # Python 3.11+
pip install -r deal_radar/requirements-dev.txt
cp deal_radar/deploy/.env.example .env                    # fill in what you have; everything is optional

python -m deal_radar.main --check-config                  # validates config + shows what would run
python -m deal_radar.main --once --dry-run                # one poll of each enabled source, alerts to console
python -m deal_radar.main --dry-run                       # run continuously, console alerts only
python -m deal_radar.main                                 # real alerts to Discord/Telegram
```

Then open `http://127.0.0.1:8080/metrics` (Prometheus) or connect to
`ws://127.0.0.1:8080/ws?token=<HTTP_AUTH_TOKEN>` for the live alert feed.

### CLI

| Flag | Meaning |
|---|---|
| `--config PATH` | config file (default `deal_radar/config.yaml`, env `DEALRADAR_CONFIG`) |
| `--env-file PATH` | `.env` to load before interpolation (default `./.env`) |
| `--check-config` | validate and print a summary (profiles, sources on this node, targets) |
| `--once [--sources a,b]` | poll each enabled source once, run the pipeline, print outcomes |
| `--dry-run` | route every alert to the console only |
| `--roles collector[,processor]` | override `app.roles` (e.g. laptop = `collector`) |
| `--node-id ID` | override `app.node_id` |
| `--log-level LEVEL` | DEBUG / INFO / WARNING / ERROR |

### Facebook Marketplace login (home machine)

```bash
python -m playwright install chromium
python -m deal_radar.sources.fb_marketplace --config deal_radar/config.yaml --login   # headed browser: log in once
python -m deal_radar.sources.fb_marketplace --config deal_radar/config.yaml --check   # verifies the saved session
```

The session is stored in `data/fb_storage_state.json` (mode 0600, git-ignored).

## Adding a product class

Add one block under `profiles:` — no code changes:

```yaml
  - id: rtx_5080
    name: NVIDIA GeForce RTX 5080 16GB
    category: gpu
    match:
      any: ['\brtx\s*-?\s*5080\b']
      none: ['\b5080\s*(?:super|ti)\b', '\b(?:laptop|mobile)\b']
    price: { reference_new: 1099, reference_used: 900, floor: 300, target: 850, ceiling: 1050 }
    search: { terms: ["rtx 5080"], ebay_category_ids: ["27386"] }
    vision_hint: a desktop graphics card with its cooler and fans visible
```

`reference_*` bootstraps the market price until enough observed history exists; the
scorer then shifts weight to the time-decayed robust median of real observations
(see the scoring section of `docs/ARCHITECTURE.md`).

## Tests

```bash
python -m pytest -q                      # whole offline suite
python -m pytest deal_radar/tests/test_filters.py -q   # scam/noise edge cases
```

The suite never touches the network: upstream APIs are emulated with recorded-shape
fixtures, `aioresponses` or local `aiohttp` test servers; Redis tests use `fakeredis`
and, when the binary exists, a throwaway `redis-server`.

## Responsible use

DealRadar is built for an individual hunting hardware for their own use. Several
sources forbid automated access in their Terms of Service (Facebook, Slickdeals,
OfferUp, Craigslist) and eBay's API License restricts derived statistics. The
defaults are deliberately polite: official APIs first, per-host rate limits, one
leader poller per source, no CAPTCHA solving, no account farming. Keep alerts
private and review each platform's terms before enabling it. Details are in
`docs/ARCHITECTURE.md` under "Compliance and failure modes".
