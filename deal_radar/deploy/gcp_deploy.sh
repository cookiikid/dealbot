#!/usr/bin/env bash
# =============================================================================
#  DealRadar — Google Cloud deployment
#
#  Primary target: ONE Compute Engine VM running Valkey + DealRadar (docker compose).
#  A 24/7 polling loop is the worst case for request-billed serverless, so the
#  cheapest always-on footprint is a small VM. Prices verified 2026-10-06:
#
#    default        e2-micro, us-east1, 30 GB pd-standard, STANDARD network tier
#                   → Always Free compute + disk; only the in-use IPv4 is billed
#                   ≈ $3.65/month  (≈ $44/year — the $200 credit lasts years)
#    --performance  e2-small, us-east1, 30 GB pd-standard   ≈ $15.9/month
#
#  us-east4 (Ashburn) is ~10-15 ms closer to AWS us-east-1 but has no free tier and
#  costs ~12.6 % more; every target sits behind a CDN edge and poll intervals are
#  seconds, so the region choice is not worth it. Memorystore ($23-36/month) and
#  always-on Cloud Run (≈ $31-53/month) cost more than the entire VM plan.
#
#  Optional: `cloudrun` deploys an extra *collector-only* worker on Cloud Run
#  (instance-based billing, min=max=1 instance) that publishes into the VM's Valkey
#  over Direct VPC egress (≈ $53/month — redundancy only, not the primary).
#
#  Usage (from the repository root, with gcloud authenticated):
#     export PROJECT_ID=my-project
#     deal_radar/deploy/gcp_deploy.sh init          # APIs, service account, firewall, secret
#     deal_radar/deploy/gcp_deploy.sh budget        # budget alerts (needs BILLING_ACCOUNT)
#     deal_radar/deploy/gcp_deploy.sh create        # VM + bootstrap (Docker, swap, Tailscale)
#     deal_radar/deploy/gcp_deploy.sh push          # ship code + .env, (re)start the stack
#     deal_radar/deploy/gcp_deploy.sh logs | status | tunnel | ssh
#     deal_radar/deploy/gcp_deploy.sh cloudrun      # optional Cloud Run collector
#     deal_radar/deploy/gcp_deploy.sh destroy
# =============================================================================
set -Eeuo pipefail

# ------------------------------------------------------------------ settings
PERFORMANCE=false
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --performance) PERFORMANCE=true ;;
    --free-tier) PERFORMANCE=false ;;  # the default; kept for backwards compatibility
    *) ARGS+=("$arg") ;;
  esac
done
set -- "${ARGS[@]+"${ARGS[@]}"}"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
# Always Free covers one e2-micro + 30 GB pd-standard in us-east1, us-central1 or us-west1.
REGION="${REGION:-us-east1}"
if [[ "$PERFORMANCE" == true ]]; then
  MACHINE_TYPE="${MACHINE_TYPE:-e2-small}"
else
  MACHINE_TYPE="${MACHINE_TYPE:-e2-micro}"
fi
DISK_TYPE="${DISK_TYPE:-pd-standard}"
DISK_SIZE_GB="${DISK_SIZE_GB:-30}"
# STANDARD tier: 200 GiB/month free egress (gcloud defaults to PREMIUM: 1 GiB free).
NETWORK_TIER="${NETWORK_TIER:-STANDARD}"
ZONE="${ZONE:-${REGION}-b}"
VM_NAME="${VM_NAME:-dealradar-primary}"
NETWORK="${NETWORK:-default}"
SUBNET="${SUBNET:-default}"
SA_NAME="${SA_NAME:-dealradar-vm}"
ENV_FILE="${ENV_FILE:-.env}"
ENV_SECRET="${ENV_SECRET:-dealradar-env}"
TAILSCALE_SECRET="${TAILSCALE_SECRET:-dealradar-tailscale-authkey}"
REMOTE_DIR="${REMOTE_DIR:-/opt/dealradar}"
NET_TAG="${NET_TAG:-dealradar}"
BUDGET_AMOUNT="${BUDGET_AMOUNT:-200}"
BUDGET_MONTHLY="${BUDGET_MONTHLY:-15}"
AR_REPO="${AR_REPO:-dealradar}"
RUN_SERVICE="${RUN_SERVICE:-dealradar-collector}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
COMPOSE="docker compose --env-file ${REMOTE_DIR}/.env -f ${REMOTE_DIR}/deal_radar/deploy/docker-compose.yml"

log()  { printf '\033[1;36m[dealradar]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[dealradar] WARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[dealradar] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }
trap 'die "command failed at line $LINENO: $BASH_COMMAND"' ERR

require() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed"; }

preflight() {
  require gcloud
  [[ -n "$PROJECT_ID" ]] || die "set PROJECT_ID (or run: gcloud config set project <id>)"
  gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q . || die "run: gcloud auth login"
  gcloud config set project "$PROJECT_ID" >/dev/null 2>&1
}

ssh_vm() {
  # IAP tunnelling: no public SSH port is needed (firewall only admits Google's IAP range).
  gcloud compute ssh "$VM_NAME" --zone "$ZONE" --tunnel-through-iap --quiet -- "$@"
}

secret_upsert() {  # name file
  local name="$1" file="$2"
  if gcloud secrets describe "$name" >/dev/null 2>&1; then
    gcloud secrets versions add "$name" --data-file="$file" >/dev/null
  else
    gcloud secrets create "$name" --replication-policy=automatic --data-file="$file" >/dev/null
  fi
  gcloud secrets add-iam-policy-binding "$name" \
    --member="serviceAccount:${SA_EMAIL}" --role=roles/secretmanager.secretAccessor >/dev/null
}

# ------------------------------------------------------------------ commands

cmd_init() {
  preflight
  log "enabling APIs"
  gcloud services enable compute.googleapis.com secretmanager.googleapis.com iap.googleapis.com \
    logging.googleapis.com monitoring.googleapis.com billingbudgets.googleapis.com >/dev/null

  if ! gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
    log "creating service account ${SA_EMAIL}"
    gcloud iam service-accounts create "$SA_NAME" --display-name="DealRadar VM" >/dev/null
  fi
  for role in roles/logging.logWriter roles/monitoring.metricWriter; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SA_EMAIL}" \
      --role="$role" --condition=None >/dev/null
  done

  if ! gcloud compute firewall-rules describe dealradar-allow-iap-ssh >/dev/null 2>&1; then
    log "firewall: SSH only from Identity-Aware Proxy (35.235.240.0/20)"
    gcloud compute firewall-rules create dealradar-allow-iap-ssh --network "$NETWORK" \
      --direction INGRESS --action ALLOW --rules tcp:22 --source-ranges 35.235.240.0/20 \
      --target-tags "$NET_TAG" >/dev/null
  fi

  if ! gcloud compute firewall-rules describe dealradar-allow-tailscale >/dev/null 2>&1; then
    log "firewall: Tailscale WireGuard (udp/41641) for direct peer connections"
    gcloud compute firewall-rules create dealradar-allow-tailscale --network "$NETWORK" \
      --direction INGRESS --action ALLOW --rules udp:41641 --source-ranges 0.0.0.0/0 \
      --target-tags "$NET_TAG" >/dev/null
  fi

  [[ -f "$ENV_FILE" ]] || die "missing $ENV_FILE (copy deal_radar/deploy/.env.example to .env and fill it in)"
  grep -q '^REDIS_PASSWORD=..*' "$ENV_FILE" || die "$ENV_FILE must define REDIS_PASSWORD"
  log "storing ${ENV_FILE} in Secret Manager as '${ENV_SECRET}'"
  secret_upsert "$ENV_SECRET" "$ENV_FILE"

  if [[ -n "${TAILSCALE_AUTHKEY:-}" ]]; then
    log "storing Tailscale auth key in Secret Manager as '${TAILSCALE_SECRET}'"
    local tmp; tmp="$(mktemp)"; printf '%s' "$TAILSCALE_AUTHKEY" > "$tmp"
    secret_upsert "$TAILSCALE_SECRET" "$tmp"; rm -f "$tmp"
  else
    warn "TAILSCALE_AUTHKEY not set: the VM will not join your tailnet (laptop/desktop nodes can't reach Valkey)"
  fi
  log "init complete"
}

cmd_budget() {
  preflight
  [[ -n "${BILLING_ACCOUNT:-}" ]] || die "set BILLING_ACCOUNT (gcloud billing accounts list)"
  # exclude-all-credits: alerts reflect real burn even while credits pay the bill.
  # Budgets only alert — they never stop resources.
  log "monthly budget ${BUDGET_MONTHLY} USD (alerts 25/50/90 % actual, 100 % forecast)"
  gcloud billing budgets create --billing-account="$BILLING_ACCOUNT" \
    --display-name="dealradar-monthly" \
    --budget-amount="${BUDGET_MONTHLY}USD" --calendar-period=month \
    --credit-types-treatment=exclude-all-credits \
    --filter-projects="projects/${PROJECT_ID}" \
    --threshold-rule=percent=0.25 --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 \
    --threshold-rule=percent=1.0,basis=forecasted-spend >/dev/null
  local start end
  start="$(date -u +%Y-%m-%d)"
  end="$(date -u -d '+1 year' +%Y-%m-%d 2>/dev/null || date -u -v+1y +%Y-%m-%d)"
  log "lifetime budget ${BUDGET_AMOUNT} USD from ${start} to ${end}"
  if ! gcloud billing budgets create --billing-account="$BILLING_ACCOUNT" \
      --display-name="dealradar-credit-${BUDGET_AMOUNT}" \
      --budget-amount="${BUDGET_AMOUNT}USD" --start-date="$start" --end-date="$end" \
      --credit-types-treatment=exclude-all-credits \
      --filter-projects="projects/${PROJECT_ID}" \
      --threshold-rule=percent=0.25 --threshold-rule=percent=0.5 --threshold-rule=percent=0.75 \
      --threshold-rule=percent=0.9 >/dev/null; then
    warn "custom-period budget rejected by this gcloud version; the monthly budget is still active"
  fi
  log "budgets created (alerts go to billing admins by email)"
}

cmd_create() {
  preflight
  if gcloud compute instances describe "$VM_NAME" --zone "$ZONE" >/dev/null 2>&1; then
    log "VM ${VM_NAME} already exists"; return 0
  fi
  log "creating ${MACHINE_TYPE} in ${ZONE} (${DISK_SIZE_GB} GB ${DISK_TYPE})"
  gcloud compute instances create "$VM_NAME" \
    --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" \
    --network "$NETWORK" --subnet "$SUBNET" \
    --image-family debian-12 --image-project debian-cloud \
    --boot-disk-size "${DISK_SIZE_GB}GB" --boot-disk-type "$DISK_TYPE" \
    --network-tier "$NETWORK_TIER" \
    --labels app=dealradar \
    --service-account "$SA_EMAIL" --scopes cloud-platform \
    --tags "$NET_TAG" \
    --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring \
    --metadata enable-oslogin=TRUE,dealradar-env-secret="$ENV_SECRET",dealradar-tailscale-secret="$TAILSCALE_SECRET",dealradar-dir="$REMOTE_DIR" \
    --metadata-from-file startup-script="${SCRIPT_DIR}/vm_startup.sh" >/dev/null
  log "waiting for bootstrap (Docker install) to finish"
  local i
  for i in $(seq 1 60); do
    if ssh_vm "test -f /var/lib/dealradar/bootstrapped" >/dev/null 2>&1; then
      log "VM ready"; return 0
    fi
    sleep 10
  done
  warn "bootstrap still running; check: gcloud compute instances get-serial-port-output ${VM_NAME} --zone ${ZONE}"
}

cmd_push() {
  preflight
  [[ -f "$ENV_FILE" ]] && secret_upsert "$ENV_SECRET" "$ENV_FILE"
  local bundle; bundle="$(mktemp -t dealradar-XXXX.tar.gz)"
  log "packaging source"
  if git -C "$REPO_ROOT" rev-parse --git-dir >/dev/null 2>&1; then
    git -C "$REPO_ROOT" archive --format=tar.gz -o "$bundle" HEAD
  else
    tar -C "$REPO_ROOT" --exclude=.venv --exclude=.git --exclude=data --exclude='__pycache__' -czf "$bundle" .
  fi
  log "uploading to ${VM_NAME}:${REMOTE_DIR}"
  gcloud compute scp "$bundle" "${VM_NAME}:/tmp/dealradar.tar.gz" --zone "$ZONE" --tunnel-through-iap --quiet
  rm -f "$bundle"
  ssh_vm "sudo bash -s" <<REMOTE
set -euo pipefail
mkdir -p ${REMOTE_DIR}
tar -xzf /tmp/dealradar.tar.gz -C ${REMOTE_DIR}
rm -f /tmp/dealradar.tar.gz
/usr/local/bin/dealradar-fetch-env
cd ${REMOTE_DIR}
${COMPOSE} up -d --build --remove-orphans
${COMPOSE} ps
REMOTE
  log "deployed. Follow logs with: $0 logs"
}

cmd_logs()   { preflight; ssh_vm "sudo ${COMPOSE} logs -f --tail 200 dealradar"; }
cmd_ssh()    { preflight; ssh_vm; }
cmd_status() {
  preflight
  ssh_vm "sudo bash -s" <<REMOTE
set -euo pipefail
token="\$(grep -E '^HTTP_AUTH_TOKEN=' ${REMOTE_DIR}/.env | head -n1 | cut -d= -f2-)"
curl -fsS -H "Authorization: Bearer \${token}" http://127.0.0.1:8080/status | head -c 20000
echo
REMOTE
}
cmd_tunnel() {
  preflight
  log "forwarding localhost:8080 -> ${VM_NAME}:8080 (Ctrl-C to stop)"
  gcloud compute ssh "$VM_NAME" --zone "$ZONE" --tunnel-through-iap -- -N -L 8080:127.0.0.1:8080
}

cmd_cloudrun() {
  preflight
  log "optional Cloud Run collector (always-on, ~\$53/month)"
  gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com >/dev/null
  if ! gcloud artifacts repositories describe "$AR_REPO" --location "$REGION" >/dev/null 2>&1; then
    gcloud artifacts repositories create "$AR_REPO" --repository-format=docker --location "$REGION" >/dev/null
  fi
  local image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/dealradar:$(date +%Y%m%d%H%M%S)"
  local build_cfg; build_cfg="$(mktemp)"
  cat > "$build_cfg" <<YAML
steps:
  - name: gcr.io/cloud-builders/docker
    args: ["build", "-f", "deal_radar/deploy/Dockerfile", "--target", "runtime", "-t", "${image}", "."]
images: ["${image}"]
YAML
  gcloud builds submit "$REPO_ROOT" --config "$build_cfg" >/dev/null
  rm -f "$build_cfg"
  local vm_ip; vm_ip="$(gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --format='value(networkInterfaces[0].networkIP)')"
  [[ -n "$vm_ip" ]] || die "primary VM not found; run 'create' first"
  [[ -n "${REDIS_PASSWORD:-}" ]] || die "export REDIS_PASSWORD (same value as in .env)"
  if ! gcloud compute firewall-rules describe dealradar-allow-valkey-internal >/dev/null 2>&1; then
    local range; range="$(gcloud compute networks subnets describe "$SUBNET" --region "$REGION" --format='value(ipCidrRange)')"
    gcloud compute firewall-rules create dealradar-allow-valkey-internal --network "$NETWORK" \
      --direction INGRESS --action ALLOW --rules tcp:6379 --source-ranges "$range" --target-tags "$NET_TAG" >/dev/null
    warn "set REDIS_BIND=0.0.0.0 in .env and re-run 'push' so Valkey listens on the VM's internal IP"
  fi
  gcloud run deploy "$RUN_SERVICE" --image "$image" --region "$REGION" \
    --no-allow-unauthenticated --port 8080 \
    --cpu 1 --memory 512Mi --min-instances 1 --max-instances 1 --no-cpu-throttling \
    --network "$NETWORK" --subnet "$SUBNET" --vpc-egress private-ranges-only \
    --set-secrets "/secrets/dealradar.env=${ENV_SECRET}:latest" \
    --set-env-vars "DEALRADAR_ENV_FILE=/secrets/dealradar.env,NODE_ID=cloudrun-${REGION},APP_ROLES=collector,BUS_BACKEND=redis,REDIS_URL=redis://:${REDIS_PASSWORD}@${vm_ip}:6379/0,FB_ENABLED=false,OFFERUP_ENABLED=false,HTTP_PORT=8080" \
    >/dev/null
  log "Cloud Run collector deployed; sources with lease_ttl_seconds fail over between it and the VM"
}

cmd_destroy() {
  preflight
  read -r -p "Delete VM ${VM_NAME}, its disk, firewall rules and Cloud Run service? [y/N] " answer
  [[ "$answer" == "y" || "$answer" == "Y" ]] || { log "aborted"; return 0; }
  gcloud compute instances delete "$VM_NAME" --zone "$ZONE" --quiet || true
  gcloud compute firewall-rules delete dealradar-allow-iap-ssh dealradar-allow-tailscale dealradar-allow-valkey-internal --quiet 2>/dev/null || true
  gcloud run services delete "$RUN_SERVICE" --region "$REGION" --quiet 2>/dev/null || true
  log "secrets were kept; delete with: gcloud secrets delete ${ENV_SECRET}"
}

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

main() {
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    init) cmd_init "$@" ;;
    budget) cmd_budget "$@" ;;
    create) cmd_create "$@" ;;
    push|deploy) cmd_push "$@" ;;
    logs) cmd_logs "$@" ;;
    ssh) cmd_ssh "$@" ;;
    status) cmd_status "$@" ;;
    tunnel) cmd_tunnel "$@" ;;
    cloudrun) cmd_cloudrun "$@" ;;
    destroy) cmd_destroy "$@" ;;
    all) cmd_init && cmd_create && cmd_push ;;
    -h|--help|help|"") usage 0 ;;
    *) warn "unknown command: $cmd"; usage 1 ;;
  esac
}

main "$@"
