# syntax=docker/dockerfile:1.7
# =============================================================================
#  DealRadar — multi-stage image
#    target "runtime" (default): API/feed sources + processor   (~180 MB)
#    target "browser":           + Playwright Chromium for fb_marketplace (~900 MB)
#  Build context = repository root:
#    docker build -f deal_radar/deploy/Dockerfile -t dealradar:latest .
#    docker build -f deal_radar/deploy/Dockerfile --target browser -t dealradar:browser .
# =============================================================================
ARG PYTHON_VERSION=3.12

# ---------------------------------------------------------------- builder
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY deal_radar/requirements.txt ./requirements.txt
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --upgrade pip wheel \
 && /opt/venv/bin/pip install -r requirements.txt

# ---------------------------------------------------------------- runtime
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEALRADAR_CONFIG=/app/deal_radar/config.yaml \
    DEALRADAR_ENV_FILE=/app/.env \
    LOG_JSON=true
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 --shell /usr/sbin/nologin dealradar
COPY --from=builder /opt/venv /opt/venv
WORKDIR /app
COPY deal_radar ./deal_radar
RUN mkdir -p /app/data && chown -R dealradar:dealradar /app/data
USER dealradar
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
  CMD python -c "import os,sys,urllib.request as u; sys.exit(0 if u.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('HTTP_PORT','8080'), timeout=2).status == 200 else 1)"
# tini reaps zombies (Chromium spawns many) and forwards SIGTERM for graceful shutdown
ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "deal_radar.main"]

# ---------------------------------------------------------------- browser
FROM runtime AS browser
USER root
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright
RUN python -m playwright install --with-deps chromium \
 && chmod -R a+rX /opt/ms-playwright \
 && rm -rf /var/lib/apt/lists/*
USER dealradar
