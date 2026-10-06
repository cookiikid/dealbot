# DealRadar — Setup & Deployment Guide (Phase 4)

This guide takes you from a fresh clone to the full three-machine deployment:

| Machine | Role | Runs |
|---|---|---|
| **GCP VM** (`e2-micro` Always Free, us-east1) | primary: collector + processor | Valkey, DealRadar (eBay, Slickdeals, Reddit, retail), dedup, scoring, alerts |
| **Desktop** (RTX 3060 12 GB) | vision API | Ollama with `qwen3-vl:4b-instruct` (+ optional `qwen3-vl:8b-instruct` escalation) |
| **Laptop** (RTX 3080 8 GB) | local collector + standby | Facebook Marketplace / OfferUp via Playwright on your home IP |

You can stop at any step: step 1 alone is a working single-machine DealRadar.

---

## 0. Prerequisites

* Python **3.11+**, Git, Docker Engine 24+ with Compose v2.24+ (for the container path).
* Accounts / keys (all optional — enable what you have):
  * **Discord**: Server Settings → Integrations → Webhooks → *New Webhook* → copy URL
    (one per channel: price errors, GPUs, displays, general, ops).
    For role pings: enable Developer Mode, right-click the role → *Copy Role ID*.
  * **Telegram**: talk to `@BotFather` → `/newbot` → token. Send your bot a message,
    then open `https://api.telegram.org/bot<TOKEN>/getUpdates` to read your `chat.id`
    (groups/channels have negative ids like `-100…`).
  * **eBay**: <https://developer.ebay.com> → *Application Keys* → Production keyset
    (`Client ID`, `Client Secret`). Before the first production call eBay requires
    the keyset to either subscribe to *Marketplace Account Deletion* notifications or
    request an exemption (choose "I do not persist eBay user data"). The default
    Browse quota is 5,000 calls/day; the free *Application Growth Check* raises it.
  * **Reddit** (time-limited, see `ARCHITECTURE.md` §2.4): an approved OAuth app at
    <https://www.reddit.com/prefs/apps> (type *script*), registered at
    <https://developers.reddit.com/app-registration>. Without credentials the source
    stays off — unauthenticated access has been blocked since May 2026.
  * **Best Buy**: <https://developer.bestbuy.com> → API key (5 requests/s).
  * **Tailscale** (free personal plan): <https://login.tailscale.com> → *Settings →
    Keys → Generate auth key* (reusable, pre-approved) for the VM.

---

## 1. Local single-machine run (any OS)

```bash
git clone <your fork> dealbot && cd dealbot
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r deal_radar/requirements-dev.txt
cp deal_radar/deploy/.env.example .env
$EDITOR .env          # at minimum: a Discord webhook or Telegram bot, HTTP_AUTH_TOKEN
```

Validate, then dry-run:

```bash
python -m deal_radar.main --check-config
python -m deal_radar.main --once --dry-run                 # one poll per source, console output
python -m pytest -q                                        # offline test-suite
```

Run for real:

```bash
python -m deal_radar.main
```

* SQLite history is written to `data/dealradar.db` (WAL mode).
* No Redis is needed in single-node mode: dedup and the bus are in-process.
* Ops endpoints: `http://127.0.0.1:8080/healthz`, `/readyz`, `/metrics`,
  `/status` and `/alerts/recent` (send `Authorization: Bearer $HTTP_AUTH_TOKEN`),
  live feed `ws://127.0.0.1:8080/ws?token=$HTTP_AUTH_TOKEN&replay=20`.

### 1.1 Same thing with Docker

```bash
echo "REDIS_PASSWORD=$(openssl rand -hex 24)" >> .env
docker compose --env-file .env -f deal_radar/deploy/docker-compose.yml up -d --build
docker compose --env-file .env -f deal_radar/deploy/docker-compose.yml logs -f dealradar
```

This starts Valkey + DealRadar with the Redis Streams bus — the exact topology the
GCP VM runs.

---

## 2. Desktop (RTX 3060 12 GB): local vision API

1. Install the NVIDIA driver and Docker with the **NVIDIA Container Toolkit**
   (Windows: Docker Desktop with WSL2 GPU support; or install Ollama natively).
2. Join your tailnet: `tailscale up`, note the IP: `tailscale ip -4` → e.g. `100.64.0.10`.
3. Start Ollama bound to the tailnet IP only and pull the models:

   ```bash
   echo "OLLAMA_BIND=100.64.0.10" >> .env
   echo "VISION_MODEL=qwen3-vl:4b-instruct" >> .env
   echo "VISION_ESCALATION_MODEL=qwen3-vl:8b-instruct" >> .env
   docker compose --env-file .env -f deal_radar/deploy/docker-compose.yml --profile vision up -d ollama ollama-pull
   curl http://100.64.0.10:11434/api/tags           # both models listed
   ```

   Native alternative: install Ollama, set `OLLAMA_HOST=100.64.0.10:11434`,
   `OLLAMA_KEEP_ALIVE=30m`, `OLLAMA_MAX_LOADED_MODELS=2`, then `ollama pull qwen3-vl:4b-instruct`.
4. On the processor (VM) set:
   `VISION_ENABLED=true`, `VISION_URL=http://100.64.0.10:11434`.

VRAM budget: the 4B model at Q4 uses ≈ 3.5-4.5 GB and the 8B ≈ 6-7 GB, plus KV cache and
CUDA context; check `ollama ps` that both stay resident on 12 GB (drop the escalation
model if not). Any OpenAI-compatible server works too (`VISION_BACKEND=openai`, e.g. vLLM
`--served-model-name` or LM Studio) — set `VISION_URL` to its base URL.

---

## 3. Google Cloud primary on a $200 credit

### 3.1 Cost plan (list prices verified 2026-10-06)

| Plan | Spec | ≈ USD / month | 12 months |
|---|---|---|---|
| **Default** | `e2-micro` in `us-east1`, 30 GB `pd-standard`, STANDARD network tier | **3.65** (only the in-use IPv4; compute + disk are Always Free) | ≈ 44 |
| `--performance` | `e2-small` in `us-east1`, 30 GB `pd-standard` | 12.23 + 3.65 = 15.88 | ≈ 191 |
| ✗ us-east4 e2-small + pd-balanced | Ashburn, no free tier | 19.62 | ≈ 235 (over budget) |
| ✗ Memorystore Valkey / Redis | smallest instance | +23-36 | — |
| ✗ Cloud Run, always on | 1 vCPU worker pool / instance-billed service | 31-53 | — |

Notes that matter for the bill:
* Always Free covers **one** `e2-micro` instance-month per billing account, only in
  `us-east1`/`us-central1`/`us-west1`, only with `pd-standard` (the console defaults to
  `pd-balanced`). The script passes the right flags.
* gcloud defaults to the PREMIUM network tier (1 GiB free egress); the script uses
  STANDARD (200 GiB/month free).
* An IPv6-only VM would avoid the IPv4 fee, but Discord, Reddit, eBay, Best Buy,
  Newegg, Craigslist and OfferUp have no AAAA records — not viable.
* `e2-micro` sustains 0.25 vCPU / 1 GB RAM: fine for Valkey + the API/feed pollers +
  the processor (swap is added, containers are memory-capped). Chromium and vision
  run at home, by design.
* Free-trial credits expire (the standard trial is 90 days). The budgets created by
  `budget` use `--credit-types-treatment=exclude-all-credits` so alerts show the real
  burn while credits are paying for it.

### 3.2 Deploy

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
export PROJECT_ID=YOUR_PROJECT_ID
export TAILSCALE_AUTHKEY=tskey-auth-...            # optional but recommended
export BILLING_ACCOUNT=XXXXXX-XXXXXX-XXXXXX        # gcloud billing accounts list

# .env for the VM (REDIS_PASSWORD is required)
cp deal_radar/deploy/.env.example .env && $EDITOR .env

deal_radar/deploy/gcp_deploy.sh init      # APIs, service account, IAP-only SSH firewall, secrets
deal_radar/deploy/gcp_deploy.sh budget    # $15/month + $200/year budgets, real burn (credits excluded)
deal_radar/deploy/gcp_deploy.sh create    # e2-micro VM + bootstrap (Docker, swap, sysctls, Tailscale)
                                          # (add --performance for an e2-small)
deal_radar/deploy/gcp_deploy.sh push      # ship the code + .env, docker compose up -d --build
deal_radar/deploy/gcp_deploy.sh logs      # follow logs
deal_radar/deploy/gcp_deploy.sh status    # /status JSON (sources, backlog, dispatch targets)
deal_radar/deploy/gcp_deploy.sh tunnel    # then open http://localhost:8080/metrics
```

What `create` sets up:
* Debian 12, Shielded VM, OS Login, **no public SSH** (IAP range only), service
  account with log/metric writer + access to the two secrets only.
* `vm_startup.sh` (re-runs every boot, idempotent): Docker Engine + compose plugin,
  2 GB swap, network sysctls for many keep-alive HTTPS connections, Tailscale join,
  `.env` materialised from Secret Manager with mode 0600, stack restarted.

Updating: edit code or `.env`, then `gcp_deploy.sh push` (it uploads a new secret
version and rebuilds). Tear down: `gcp_deploy.sh destroy`.

### 3.3 Valkey on your tailnet (for the laptop collector)

Valkey listens on `127.0.0.1` only. When a Tailscale auth key was stored by `init`,
`vm_startup.sh` joins the tailnet and runs `tailscale serve --tcp=6379`, which
publishes the port **to tailnet members only**. There is no VPC firewall rule for 6379,
ever. Find the VM's tailnet address with `tailscale ip -4` (or the admin console) and
use it in the laptop's `PRIMARY_REDIS_URL`. Valkey is also password-protected.

---

## 4. Laptop (RTX 3080 8 GB): Facebook Marketplace collector

1. Join the tailnet. Clone the repo, create `.env` with:

   ```bash
   LOCAL_NODE_ID=home-laptop
   PRIMARY_REDIS_URL=redis://:<REDIS_PASSWORD>@100.101.102.103:6379/0
   FB_ENABLED=true
   FB_NODES=home-laptop
   HOME_LAT=...  HOME_LON=...  FB_CITY_SLUG=...  FB_RADIUS_KM=60
   APP_TIMEZONE=America/Chicago            # must match where you actually are
   ```

2. Log in to Facebook once in a real (headed) browser window — this writes
   `data/fb_storage_state.json` (0600):

   ```bash
   python -m venv .venv && . .venv/bin/activate && pip install -r deal_radar/requirements.txt
   python -m playwright install chromium
   python -m deal_radar.sources.fb_marketplace --config deal_radar/config.yaml --login
   python -m deal_radar.sources.fb_marketplace --config deal_radar/config.yaml --check
   ```

3. Run the collector (container or bare):

   ```bash
   docker compose --env-file .env -f deal_radar/deploy/docker-compose.yml --profile local up -d --build dealradar-browser
   # or
   APP_ROLES=collector BUS_BACKEND=redis REDIS_URL=$PRIMARY_REDIS_URL NODE_ID=home-laptop python -m deal_radar.main
   ```

Listings flow into the VM's Redis Stream; the VM scores, runs vision on the desktop
and alerts. If Facebook shows a checkpoint the source pauses for
`checkpoint_pause_minutes` and the ops channel gets a notice — log in again with
`--login` when that happens.

**Standby for the VM:** set `REDDIT_ENABLED` / `SLICKDEALS_ENABLED=true` on the
laptop too. Those sources have `lease_ttl_seconds`, so the laptop stays in `standby`
while the VM holds the lease and takes over automatically if the VM disappears.

---

## 5. Operating it

| Task | How |
|---|---|
| Is everything polling? | `gcp_deploy.sh status` → `sources[].state` (`ok`, `standby`, `open`, `setup_failed`) |
| Latency | `/metrics`: `dealradar_pipeline_internal_ms`, `dealradar_alert_end_to_end_ms`, `dealradar_source_poll_ms` |
| Too many alerts | raise `profiles[].min_score` or `scoring.severity.medium`; add `max_per_minute` to the route |
| Missed a deal | `python -m deal_radar.main --once --dry-run --sources slickdeals` shows each listing's stage and score |
| New product class | add a `profiles:` block, `--check-config`, `push` |
| Refresh price anchors | edit `reference_*` quarterly; the observed-history median takes over as data accumulates |
| Backups | `data/dealradar.db` (SQLite) — `sqlite3 data/dealradar.db ".backup backup.db"`; Valkey AOF lives in the `valkey-data` volume |
| Budget | the budget alert emails at 25/50/75/90/100 %; `gcloud billing projects describe $PROJECT_ID` |

### Troubleshooting

* `--check-config` prints the exact YAML path of any error (unknown keys and invalid
  regexes fail fast by design).
* `source blocked` notices: back off — raise `poll_interval_seconds`, verify the
  account in a normal browser, re-run `--login` for Facebook.
* Discord `404`/`401`: the webhook was deleted; DealRadar disables that target to
  avoid Discord's invalid-request IP bans — fix the URL and restart.
* Vision `ERROR` verdicts: `curl $VISION_URL/api/tags` from the VM over Tailscale;
  check `OLLAMA_BIND`.
