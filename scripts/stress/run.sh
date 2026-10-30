#!/usr/bin/env bash
# The stress campaign of docs/benchmarks/v0.1.0/README.md. Each subcommand is one test; every output it
# relies on is kept under $OUT (default docs/benchmarks/v0.1.0/raw). Never part of `make check`.
#
#   scripts/stress/run.sh up | down | env
#   scripts/stress/run.sh load NAME [rebound-load run flags]     # one harness run on the deploy network
#   scripts/stress/run.sh ramp | soak | spike | storm | faults | slow | baseline
#
# The harness runs in a container on the compose network, pinned to CPUs 8-11 (the stack is on
# 0-7, scripts/stress/compose.yml), built without -race: it is a throughput measurement. Before
# every run the VM timers are checked (internal/timercheck -v) and the result kept with the run.
# Env: OUT, BASE_URL (default http://nginx:8080), LOAD_CPUS (8-11), SEED, NGINX (shipped|tuned) and
# REDIS (shipped|aof), both applied by `up`.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
hostroot="$root"
if pwd -W >/dev/null 2>&1; then
  # Git Bash on Windows: hand Docker native paths and stop MSYS from rewriting them.
  hostroot="$(cd "$root" && pwd -W)"
  export MSYS_NO_PATHCONV=1
fi
out="${OUT:-$root/docs/benchmarks/v0.1.0/raw}"
mkdir -p "$out"
hostout="$out"
if pwd -W >/dev/null 2>&1; then hostout="$(cd "$out" && pwd -W)"; fi
compose=(docker compose -f "$hostroot/examples/deploy/docker-compose.yml" -f "$hostroot/scripts/stress/compose.yml")
# NGINX=tuned raises nginx's connection limits (scripts/stress/nginx-tuned.conf); default: as shipped.
if [ "${NGINX:-shipped}" = tuned ]; then compose+=(-f "$hostroot/scripts/stress/nginx-tuned.yml"); fi
# REDIS=aof gives Redis an append-only file (scripts/stress/redis-aof.yml); default: no persistence.
if [ "${REDIS:-shipped}" = aof ]; then compose+=(-f "$hostroot/scripts/stress/redis-aof.yml"); fi
network="${NETWORK:-deploy_default}"
base="${BASE_URL:-http://nginx:8080}"
cpus="${LOAD_CPUS:-8-11}"
exe=""
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) exe=".exe" ;; esac

log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$out/campaign.log" >&2; }

build() {
  (cd "$root" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o bin/linux/rebound-load ./cmd/rebound-load)
  (cd "$root" && go build -trimpath -o "bin/rebound-load$exe" ./cmd/rebound-load)
}

# timer NAME: the VM timer latency, kept as NAME.timer. A SLOW result invalidates the run.
timer() {
  docker run --rm --cpuset-cpus "$cpus" -v "$hostroot:/src" -v rebound-gomod:/go/pkg/mod \
    -v rebound-gocache:/root/.cache/go-build -w /src rebound-dev go run ./internal/timercheck -v \
    2>&1 | tee "$out/$1.timer"
  if grep -q SLOW "$out/$1.timer"; then
    log "$1: VM timers are slow; restart the VM (docker desktop stop; wsl --shutdown; docker desktop start)"
    exit 3
  fi
}

# host_sampler NAME INTERVAL: docker stats and Redis INFO into NAME.host.csv, in the background.
host_sampler() {
  local containers
  containers="$(docker ps --format '{{.Names}}' | grep -E '^(deploy-|rebound-solo)' | paste -sd, -)"
  local redis=""
  docker ps --format '{{.Names}}' | grep -qx deploy-redis-1 && redis=deploy-redis-1
  "$root/bin/rebound-load$exe" host-sample -containers "$containers" ${redis:+-redis "$redis"} \
    -every "$2" -out "$hostout/$1.host.csv" 2>>"$out/$1.host.err" &
  echo $! >"$out/.host.pid"
}

stop_host_sampler() {
  [ -f "$out/.host.pid" ] && kill "$(cat "$out/.host.pid")" 2>/dev/null || true
  rm -f "$out/.host.pid"
}

# app_samplers NAME INTERVAL: expvar of each instance into NAME.<app>.csv, from a container in
# the instance's network namespace (DEBUG_ADDR is 127.0.0.1 inside it).
app_samplers() {
  for app in "${APPS[@]}"; do
    docker rm -f "sampler-$app" >/dev/null 2>&1 || true
    docker run -d --name "sampler-$app" --network "container:$app" -v "$hostroot/bin/linux:/t:ro" \
      -v "$hostout:/out" gcr.io/distroless/static-debian12 /t/rebound-load sample \
      -url http://127.0.0.1:6060/debug/vars -source "$app" -every "$2" -out "/out/$1.$app.csv" >/dev/null
  done
}

stop_app_samplers() {
  for app in "${APPS[@]}"; do docker rm -f "sampler-$app" >/dev/null 2>&1 || true; done
}

APPS=(deploy-app-1-1 deploy-app-2-1)

# load NAME FLAGS...: one harness run; NAME.json (report), NAME.txt (table), NAME.progress.
load() {
  local name=$1
  shift
  timer "$name"
  # Which nginx limits this run met: as shipped (events {}) or tuned.
  docker exec deploy-nginx-1 sh -c 'grep -E "worker_(processes|connections)" /etc/nginx/nginx.conf || echo "events {} (as shipped: 1 worker, 512 connections)"' \
    >"$out/$name.nginx-mode" 2>/dev/null || echo "no nginx" >"$out/$name.nginx-mode"
  log "$name: nginx $(paste -sd' ' "$out/$name.nginx-mode")"
  log "$name: rebound-load run $*"
  local code=0
  docker run --rm --name "rebound-load-$name" --network "$network" --cpuset-cpus "$cpus" \
    --ulimit nofile=1048576:1048576 \
    --sysctl net.ipv4.ip_local_port_range="1024 65535" --sysctl net.ipv4.tcp_tw_reuse=1 \
    -v "$hostroot/bin/linux:/t:ro" -v "$hostout:/out" gcr.io/distroless/static-debian12 \
    /t/rebound-load run -base-url "$base" ${SEED:+-seed "$SEED"} -out "/out/$name.json" "$@" \
    >"$out/$name.txt" 2>"$out/$name.progress" || code=$?
  cat "$out/$name.txt"
  log "$name: exit $code"
  return 0
}

# logs NAME SINCE: what nginx and the instances logged above Info since SINCE, counted by message,
# into NAME.logs.txt, with the first 20 nginx error lines in NAME.nginx.log (a saturated nginx
# writes millions of identical lines; only their count is kept).
logs() {
  local name=$1 since=$2
  {
    if docker ps --format '{{.Names}}' | grep -qx deploy-nginx-1; then
      echo "## nginx error log, by level and message (first 60 chars)"
      docker logs --since "$since" deploy-nginx-1 2>&1 | grep -E '\[(emerg|alert|crit|error|warn)\]' |
        sed -E 's/^[0-9\/: ]+\[([a-z]+)\] [0-9#]+: \*?[0-9]* ?(.{0,60}).*/\1 \2/' | sort | uniq -c | sort -rn | head -20 || true
    fi
    for app in "${APPS[@]}"; do
      echo "## $app: WARN/ERROR lines, by message"
      docker logs --since "$since" "$app" 2>&1 | grep -E '"level":"(WARN|ERROR)"|level=(WARN|ERROR)' |
        sed -E 's/.*"msg":"([^"]*)".*/\1/; s/.*msg="([^"]*)".*/\1/' | sort | uniq -c | sort -rn | head -20 || true
    done
  } >"$out/$name.logs.txt"
  docker logs --since "$since" deploy-nginx-1 2>&1 | grep -E '\[(emerg|alert|crit|error|warn)\]' |
    head -20 >"$out/$name.nginx.log" || true
  cat "$out/$name.logs.txt"
}

# measured NAME SAMPLE_EVERY FLAGS...: a load run with the host and instance samplers around it.
measured() {
  local name=$1 every=$2
  shift 2
  local since
  since="$(date -u +%FT%TZ)"
  host_sampler "$name" "$every"
  app_samplers "$name" "$every"
  load "$name" "$@"
  stop_app_samplers
  stop_host_sampler
  logs "$name" "$since"
}

RAMP="50x1-3@3m,200x1-3@3m,500x1-3@3m,1000x1-3@3m,2000x1-3@3m"

# fault NAME AT ACTION...: a load run of 4 minutes where ACTION runs AT seconds in; every stream
# is audited. The fault's own timeline goes to NAME.fault.log.
fault() {
  local name=$1 at=$2
  shift 2
  (
    sleep "$at"
    for step in "$@"; do
      printf '%s %s\n' "$(date -u +%FT%T.%3NZ)" "$step" >>"$out/$name.fault.log"
      eval "$step" >>"$out/$name.fault.log" 2>&1
    done
    printf '%s done\n' "$(date -u +%FT%T.%3NZ)" >>"$out/$name.fault.log"
  ) &
  measured "$name" 10s -phases "${FAULT_PHASES:-200x1-3@4m}" -events "${FAULT_EVENTS:-600}" -delay-ms 50 -audit 1 -window 5s -slack 3m
  wait
}

main() {
case "${1:-}" in
  ramp)
    # Test 1: steps of streams held 3 minutes each, 1-3 readers per stream, 5 % of streams audited.
    build
    measured "${2:-ramp}" 10s -phases "${PHASES:-$RAMP}" -audit 0.05 -window 10s
    ;;
  baseline)
    # Test 1, baseline: the same ramp against one instance on the memory store, no nginx, while
    # the deploy stack is stopped.
    build
    "${compose[@]}" stop
    docker rm -f rebound-solo >/dev/null 2>&1 || true
    docker network create "$network" >/dev/null 2>&1 || true
    docker run -d --name rebound-solo --network "$network" --cpuset-cpus 0-7 -e REBOUND_STORE=memory \
      -e CONFORMANCE=1 -e INSTANCE_NAME=solo -e DEBUG_ADDR=127.0.0.1:6060 rebound-chat:dev >/dev/null
    APPS=(rebound-solo)
    BASE_URL=http://rebound-solo:8080
    base=$BASE_URL
    measured "${2:-baseline}" 10s -phases "${PHASES:-$RAMP}" -audit 0.05 -window 10s
    docker rm -f rebound-solo >/dev/null
    "${compose[@]}" start
    ;;
  soak)
    # Test 2: STREAMS (about half the knee) for 2 hours, each connection dropped with probability
    # 0.3 at a random point; samples every minute from 3 minutes before to 15 minutes after (Retention
    # is 10 minutes), so that goroutines and Redis keys can be compared with the idle baseline.
    build
    : "${STREAMS:?set STREAMS to about half the knee}"
    host_sampler "${2:-soak}" 60s
    app_samplers "${2:-soak}" 60s
    log "soak: idle baseline, 3 minutes"
    sleep 180
    since="$(date -u +%FT%TZ)"
    load "${2:-soak}" -phases "${STREAMS}x1-3@${SOAK_FOR:-2h}" -drop conn:0.3 -audit 0.02 -window 60s
    log "soak: idle after the load, 15 minutes"
    sleep 900
    stop_app_samplers
    stop_host_sampler
    logs "${2:-soak}" "$since"
    ;;
  spike)
    # Test 3: the soak load for 3 minutes (baseline p99), idle 1 minute, 5x the soak load reached in
    # 10 s and held 2 minutes, then back to the soak load for 5 minutes (recovery).
    build
    : "${STREAMS:?set STREAMS to the soak load}"
    measured "${2:-spike}" 5s -phases "${STREAMS}x1-3@3m,0x1@1m,$((5 * STREAMS))x1-3~10s@2m,${STREAMS}x1-3@5m" -audit 0.05 -window 5s
    ;;
  storm)
    # Test 4: few streams, readers dropping every 1-5 events (or every 5-50 ms) and reconnecting at
    # once with Last-Event-ID through nginx round-robin; every stream audited.
    build
    docker network inspect "$network" --format '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}' >"$out/${2:-storm}.upstreams.txt"
    measured "${2:-storm}" 10s -phases "${STORM_PHASES:-20x3@1m}" -events "${STORM_EVENTS:-3000}" -delay-ms 10 \
      -drop "${DROP:-every:1-5}" -audit 1 -window 5s
    ;;
  fault-redis)
    # Test 5a: Redis stopped for 2 s (docker stop; sleep 2; docker start) under load.
    build
    fault "${2:-fault-redis}" 60 "docker stop -t 1 deploy-redis-1" "sleep 2" "docker start deploy-redis-1"
    ;;
  fault-redis-pause)
    # Test 5a, variant: Redis frozen for 2 s (docker pause): unavailable, but its data is kept.
    build
    fault "${2:-fault-redis-pause}" 60 "docker pause deploy-redis-1" "sleep 2" "docker unpause deploy-redis-1"
    ;;
  fault-kill)
    # Test 5b: app-1 killed (SIGKILL) under load, started again 60 s later.
    build
    fault "${2:-fault-kill}" 60 "docker kill deploy-app-1-1" "sleep 60" "docker start deploy-app-1-1"
    ;;
  fault-stop)
    # Test 5c: app-2 stopped gracefully (SIGTERM, ${GRACE:-40} s before SIGKILL), started again later.
    build
    fault "${2:-fault-stop}" 60 "docker stop -t ${GRACE:-40} deploy-app-2-1" "docker start deploy-app-2-1"
    ;;
  slow)
    # Test 6: SLOW streams whose reader stops reading after 10 events for 60 s, next to a normal
    # load; slow-control is the same load without them. SLOW_AFTER above SLOW_EVENTS keeps the same
    # large streams but a reader that never pauses: the control for the volume they write.
    build
    measured "${2:-slow}" 10s -phases "${SLOW_PHASES:-200x1-3@4m}" -slow-streams "${SLOW:-6}" -slow-events "${SLOW_EVENTS:-1500}" \
      -slow-size 32768 -slow-delay-ms 5 -slow-after "${SLOW_AFTER:-10}" -slow-pause 60s -audit 0.05 -window 10s
    ;;
  slow-control)
    build
    measured "${2:-slow-control}" 10s -phases "${SLOW_PHASES:-200x1-3@4m}" -audit 0.05 -window 10s
    ;;
  up)
    "${compose[@]}" build
    "${compose[@]}" up -d
    ;;
  down)
    "${compose[@]}" down
    ;;
  env)
    {
      echo "## captured $(date -u +%FT%TZ)"
      echo "### host"
      powershell.exe -NoProfile -Command '(Get-CimInstance Win32_Processor).Name; "logical CPUs: " + (Get-CimInstance Win32_Processor).NumberOfLogicalProcessors; "RAM GB: {0:N1}" -f ((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB); (Get-CimInstance Win32_OperatingSystem).Caption + " " + (Get-CimInstance Win32_OperatingSystem).Version' 2>/dev/null | tr -d '\r' || uname -a
      echo "### docker"
      docker version --format 'client {{.Client.Version}}, server {{.Server.Version}}'
      docker info --format 'VM: {{.NCPU}} CPUs, {{.MemTotal}} bytes RAM, kernel {{.KernelVersion}}, {{.OperatingSystem}}'
      docker compose version
      echo "### go"
      go version
      docker run --rm rebound-dev go version
      echo "### images"
      docker exec deploy-redis-1 redis-server --version
      docker exec deploy-nginx-1 nginx -v 2>&1
      docker image inspect rebound-chat:dev --format 'rebound-chat:dev {{.Id}} built {{.Created}}'
    } | tee "$out/environment.txt"
    ;;
  build)
    build
    ;;
  load)
    shift
    build
    load "$@"
    ;;
  *)
    echo "usage: $0 up|down|env|build|load NAME [flags]" >&2
    exit 2
    ;;
esac
}

# Parsed whole before it runs: editing this file during a run cannot change the run.
main "$@"; exit $?
