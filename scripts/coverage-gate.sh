#!/usr/bin/env bash
# Per-package coverage thresholds (docs/project/code-standards.md). Usage: coverage-gate.sh coverage.out
# Statement-weighted: the same percentage `go test -cover` prints for each package.
set -euo pipefail
profile="${1:-coverage.out}"
module="github.com/bailalayoub/rebound"
thresholds="
$module 85
$module/internal/core 85
$module/store/memory 85
$module/store/redis 85
$module/sse 90
$module/httpx 85
$module/ginx 80
"
awk -v thresholds="$thresholds" '
BEGIN {
  n = split(thresholds, lines, "\n")
  for (i = 1; i <= n; i++) if (split(lines[i], f, " ") == 2) { min[f[1]] = f[2]; order[++k] = f[1] }
}
NR == 1 { next } # "mode: ..." header
{
  # $1 = "<import path>/<file>.go:<range>"; keyed by block so duplicated blocks count once.
  split($1, loc, ":"); pkg = loc[1]; sub(/\/[^\/]*$/, "", pkg)
  stmts[$1] = $2; pkgOf[$1] = pkg
  if ($3 > 0) hit[$1] = 1
}
END {
  for (b in stmts) { total[pkgOf[b]] += stmts[b]; if (b in hit) covered[pkgOf[b]] += stmts[b] }
  fail = 0
  for (i = 1; i <= k; i++) {
    p = order[i]
    if (!(p in total) || total[p] == 0) { printf "coverage: %s not in profile yet (skipped)\n", p; continue }
    pct = 100 * covered[p] / total[p]
    if (pct < min[p]) { printf "coverage: %s %.1f%% < %d%%\n", p, pct, min[p]; fail = 1 }
    else printf "coverage: %s %.1f%% OK (min %d%%)\n", p, pct, min[p]
  }
  exit fail
}' "$profile"
