#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

RAY_HEAD_PORT="${RAY_HEAD_PORT:-6379}"
RAY_DASHBOARD_PORT="${RAY_DASHBOARD_PORT:-8265}"
RAY_TEMP_DIR="${RAY_TEMP_DIR:-/tmp/ray_env_probe_mode3}"

ray stop --force >/dev/null 2>&1 || true

unset PROBE_BEFORE || true
unset PROBE_AFTER || true
unset PROBE_RUNTIME || true

ray start --head \
  --port="$RAY_HEAD_PORT" \
  --dashboard-host=0.0.0.0 \
  --dashboard-port="$RAY_DASHBOARD_PORT" \
  --include-dashboard=true \
  --disable-usage-stats \
  --temp-dir="$RAY_TEMP_DIR"

RUNTIME_ENV_JSON='{
  "env_vars": {
    "PROBE_RUNTIME": "set-by-ray-job-runtime-env-json"
  }
}'

ray job submit \
  --address="http://127.0.0.1:${RAY_DASHBOARD_PORT}" \
  --runtime-env-json="$RUNTIME_ENV_JSON" \
  -- python env_probe/env_probe.py \
  --init auto \
  --set-after-init PROBE_AFTER=set-in-job-driver-after-ray-init

ray stop --force >/dev/null 2>&1 || true
