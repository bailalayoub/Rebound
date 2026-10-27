#!/usr/bin/env bash
# wait-for.sh <url> <seconds>
url="$1"; secs="${2:-30}"
for ((i=0; i<secs*2; i++)); do curl -fsS "$url" >/dev/null 2>&1 && exit 0; sleep 0.5; done
echo "timeout waiting for $url"; exit 1
