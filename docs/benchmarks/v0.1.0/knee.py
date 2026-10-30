# Knee table of docs/benchmarks/v0.1.0/README.md: per step, delivered events/s against the ideal
# (streams x events/s per stream x mean readers per stream) and p99 against the previous step.
# Usage: python knee.py REPORT.json... (from docs/benchmarks/v0.1.0/raw)
import json, sys
for path in sys.argv[1:]:
    r = json.load(open(path))
    per_stream = 1000 / r["stream"]["delay_ms"]
    print(f"## {path} (seed {r['seed']}, timer {r['timer_10ms_before_ms']}/{r['timer_10ms_after_ms']} ms)")
    print("| step | streams in flight | readers/stream | ideal ev/s | delivered ev/s | efficiency | lat p99 ms | p99 vs previous | ttfe p99 ms | http errors | integrity | pass |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    prev = None
    for p in r["phases"]:
        streams = int(p["name"].split("x")[0])
        rps = p["readers"] / p["streams_started"] if p["streams_started"] else 0
        ideal = streams * per_stream * rps
        eff = p["events_per_s"] / ideal if ideal else 0
        p99 = p["latency"]["p99_ms"]
        ratio = f"{p99 / prev:.1f}x" if prev else "-"
        errs = sum(p["http_errors"].values())
        integ = sum(p["integrity"].values())
        print(f"| {p['name']} | {streams} | {rps:.2f} | {ideal:.0f} | {p['events_per_s']:.0f} | {eff:.0%} | {p99:.1f} | {ratio} | {p['ttfe']['p99_ms']:.1f} | {errs} | {integ} | {p['passed']} |")
        prev = p99
    print()
