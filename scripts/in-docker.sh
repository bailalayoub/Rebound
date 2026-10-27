#!/usr/bin/env bash
# Runs make targets inside the CI-parity container (Go 1.26, gcc for -race, pinned linters), after
# a check that warns when the container's timers fire late (internal/timercheck).
# Usage: scripts/in-docker.sh [target...]   (default: check)
# Env: DOCKER_RUN_ARGS for extra `docker run` flags (e.g. --network to reach Redis).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
if pwd -W >/dev/null 2>&1; then
  # Git Bash on Windows: hand Docker a native path and stop MSYS from rewriting ":/src".
  root="$(cd "$root" && pwd -W)"
  export MSYS_NO_PATHCONV=1
fi
docker build -q -t rebound-dev -f "$root/scripts/dev.Dockerfile" "$root" >/dev/null
# shellcheck disable=SC2086 # DOCKER_RUN_ARGS is intentionally word-split.
docker run --rm ${DOCKER_RUN_ARGS:-} \
  -v "$root:/src" -v rebound-gomod:/go/pkg/mod -v rebound-gocache:/root/.cache/go-build \
  -w /src -e REDIS_ADDR -e REDIS_CLUSTER_ADDR -e RUNS rebound-dev \
  bash -c 'go run ./internal/timercheck; exec make "$@"' in-docker "${@:-check}"
