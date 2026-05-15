#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export PROBE_BEFORE="set-in-shell-before-python"
unset PROBE_AFTER || true
unset PROBE_RUNTIME || true

python env_probe/env_probe.py \
  --init local \
  --set-after-init PROBE_AFTER=set-in-driver-after-ray-init
