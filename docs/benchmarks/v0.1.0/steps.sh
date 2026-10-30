#!/usr/bin/env bash
# Per-phase resource tables of docs/benchmarks/v0.1.0/README.md (NAME.steps.md), from the samples of each run.
# Usage: docs/benchmarks/v0.1.0/steps.sh RUN... (a run named baseline-* uses the one-instance columns)
set -euo pipefail
cd "$(dirname "$0")/raw"
tool=../../../../bin/tools/rebound-load
[ -x "$tool.exe" ] && tool="$tool.exe"
MiB=0.00000095367431640625
stack="app-1 CPU %=deploy-app-1-1:cpu_pct:mean,app-2 CPU %=deploy-app-2-1:cpu_pct:mean,Redis CPU %=deploy-redis-1:cpu_pct:mean,nginx CPU %=deploy-nginx-1:cpu_pct:mean,app-1 mem MiB=deploy-app-1-1:mem_bytes:max*$MiB,app-2 mem MiB=deploy-app-2-1:mem_bytes:max*$MiB,Redis used MiB=redis:used_memory:max*$MiB,Redis ops/s=redis:instantaneous_ops_per_sec:mean,Redis clients=redis:connected_clients:max,app-1 goroutines=deploy-app-1-1:goroutines:max,app-2 goroutines=deploy-app-2-1:goroutines:max"
solo="solo CPU %=rebound-solo:cpu_pct:mean,solo mem MiB=rebound-solo:mem_bytes:max*$MiB,solo RSS MiB=rebound-solo:rss_bytes:max*$MiB,solo goroutines=rebound-solo:goroutines:max"
for run in "$@"; do
  case "$run" in
    baseline-*) "$tool" steps -report "$run.json" -in "$run.host.csv,$run.rebound-solo.csv" -columns "$solo" ${SETTLE:+-settle "$SETTLE"} >"$run.steps.md" ;;
    *) "$tool" steps -report "$run.json" -in "$run.host.csv,$run.deploy-app-1-1.csv,$run.deploy-app-2-1.csv" -columns "$stack" ${SETTLE:+-settle "$SETTLE"} >"$run.steps.md" ;;
  esac
  echo "$run.steps.md"
done
