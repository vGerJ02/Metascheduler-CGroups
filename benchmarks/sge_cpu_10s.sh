#!/usr/bin/env bash
set -euo pipefail

start_ts=$(date +%s)
while [ $(( $(date +%s) - start_ts )) -lt 10 ]; do
  : # busy loop to keep CPU active for ~10s
done
