#!/usr/bin/env bash
# =============================================================================
#  DealRadar VM bootstrap (GCE startup-script; runs as root on every boot, idempotent)
#   * Docker Engine + compose plugin from Docker's apt repository
#   * swap (e2-micro/e2-small have 1-2 GB RAM; Chromium-free stack fits, swap absorbs spikes)
#   * network sysctls for many short-lived HTTPS polls
#   * Tailscale (if an auth key secret exists) so home nodes reach Valkey privately
#   * helper to materialize .env from Secret Manager, then (re)start the stack
# =============================================================================
set -Eeuo pipefail
exec > >(tee -a /var/log/dealradar-bootstrap.log) 2>&1

META=http://metadata.google.internal/computeMetadata/v1
md() { curl -fsS -H "Metadata-Flavor: Google" "${META}/instance/attributes/$1" 2>/dev/null || true; }

# Read a Secret Manager secret through the REST API with the VM's service-account token.
# (No dependency on the gcloud CLI being present in the image.)
getsecret() {
  local token project
  token="$(curl -fsS -H "Metadata-Flavor: Google" "${META}/instance/service-accounts/default/token" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')" || return 1
  project="$(curl -fsS -H "Metadata-Flavor: Google" "${META}/project/project-id")" || return 1
  curl -fsS -H "Authorization: Bearer ${token}" \
    "https://secretmanager.googleapis.com/v1/projects/${project}/secrets/$1/versions/latest:access" \
    | python3 -c 'import sys,json,base64;sys.stdout.write(base64.b64decode(json.load(sys.stdin)["payload"]["data"]).decode())'
}

ENV_SECRET="$(md dealradar-env-secret)"; ENV_SECRET="${ENV_SECRET:-dealradar-env}"
TS_SECRET="$(md dealradar-tailscale-secret)"; TS_SECRET="${TS_SECRET:-dealradar-tailscale-authkey}"
APP_DIR="$(md dealradar-dir)"; APP_DIR="${APP_DIR:-/opt/dealradar}"

echo "[bootstrap] $(date -Is) starting (dir=${APP_DIR})"
export DEBIAN_FRONTEND=noninteractive

# ---------------------------------------------------------------- Docker
if ! command -v docker >/dev/null 2>&1; then
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  . /etc/os-release
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
mkdir -p /etc/docker
if [[ ! -f /etc/docker/daemon.json ]]; then
  cat > /etc/docker/daemon.json <<'JSON'
{ "log-driver": "json-file", "log-opts": { "max-size": "20m", "max-file": "5" }, "live-restore": true }
JSON
  systemctl restart docker
fi
systemctl enable --now docker

# ---------------------------------------------------------------- swap
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# ---------------------------------------------------------------- sysctl
cat > /etc/sysctl.d/90-dealradar.conf <<'SYSCTL'
# Many concurrent keep-alive HTTPS connections; fast reuse of ephemeral ports.
net.ipv4.ip_local_port_range = 10240 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fastopen = 3
net.core.somaxconn = 4096
net.ipv4.tcp_keepalive_time = 120
vm.swappiness = 10
# Valkey recommends overcommit for background saves.
vm.overcommit_memory = 1
SYSCTL
sysctl --system >/dev/null

# ---------------------------------------------------------------- Tailscale
if TS_KEY="$(getsecret "${TS_SECRET}" 2>/dev/null)" && [[ -n "${TS_KEY}" ]]; then
  if ! command -v tailscale >/dev/null 2>&1; then
    curl -fsSL https://tailscale.com/install.sh | sh
  fi
  systemctl enable --now tailscaled
  if ! tailscale status >/dev/null 2>&1; then
    tailscale up --authkey="${TS_KEY}" --hostname="$(hostname)" --ssh=false
  fi
  echo "[bootstrap] tailscale ip: $(tailscale ip -4 2>/dev/null | head -n1)"
  # Expose Valkey to the tailnet only (it stays bound to 127.0.0.1 on the host), so the
  # laptop collector can XADD into the stream. No VPC firewall rule for 6379 ever exists.
  tailscale serve --bg --tcp=6379 tcp://127.0.0.1:6379 >/dev/null 2>&1 \
    || echo "[bootstrap] WARN: tailscale serve for Valkey failed (older tailscale?)"
fi

# ---------------------------------------------------------------- env helper
{
  echo '#!/usr/bin/env bash'
  echo 'set -euo pipefail'
  echo "META=${META}"
  declare -f getsecret
  echo 'umask 077'
  echo "mkdir -p ${APP_DIR}"
  echo "getsecret '${ENV_SECRET}' > '${APP_DIR}/.env.tmp'"
  echo "mv '${APP_DIR}/.env.tmp' '${APP_DIR}/.env'"
} > /usr/local/bin/dealradar-fetch-env
chmod 0755 /usr/local/bin/dealradar-fetch-env

# ---------------------------------------------------------------- (re)start stack after reboots
mkdir -p "${APP_DIR}" /var/lib/dealradar
if [[ -f "${APP_DIR}/deal_radar/deploy/docker-compose.yml" ]]; then
  /usr/local/bin/dealradar-fetch-env || echo "[bootstrap] WARN: could not fetch env secret ${ENV_SECRET}"
  docker compose --env-file "${APP_DIR}/.env" -f "${APP_DIR}/deal_radar/deploy/docker-compose.yml" up -d --remove-orphans
fi

touch /var/lib/dealradar/bootstrapped
echo "[bootstrap] $(date -Is) done"
