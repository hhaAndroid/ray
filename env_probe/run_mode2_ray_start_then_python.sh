#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

RAY_HEAD_PORT="${RAY_HEAD_PORT:-6379}"
RAY_DASHBOARD_PORT="${RAY_DASHBOARD_PORT:-8265}"
RAY_TEMP_DIR="${RAY_TEMP_DIR:-/tmp/ray_env_probe_mode2}"

ray stop --force >/dev/null 2>&1 || true

export PROBE_BEFORE="set-in-shell-before-ray-start"
unset PROBE_AFTER || true
unset PROBE_RUNTIME || true

ray start --head \
  --port="$RAY_HEAD_PORT" \
  --dashboard-host=0.0.0.0 \
  --dashboard-port="$RAY_DASHBOARD_PORT" \
  --include-dashboard=true \
  --disable-usage-stats \
  --temp-dir="$RAY_TEMP_DIR"

python env_probe/env_probe.py \
  --init auto \
  --set-after-init PROBE_AFTER=set-in-driver-after-ray-init

ray stop --force >/dev/null 2>&1 || true
