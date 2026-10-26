#!/usr/bin/env bash
# Starts examples/chat with the memory store on a free port and runs the conformance tester.
set -euo pipefail
port=$(python3 -c 'import socket;s=socket.socket();s.bind(("",0));print(s.getsockname()[1]);s.close()')
PORT="$port" REBOUND_STORE=memory LLM_PROVIDER=fake CONFORMANCE=1 ./bin/rebound-chat & pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT
./scripts/wait-for.sh "http://localhost:$port/healthz" 30
./bin/rebound-conformance --base-url "http://localhost:$port" --runs "${RUNS:-200}" --drop-rate 0.3 --concurrency 3
