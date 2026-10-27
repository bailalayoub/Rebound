# Spike recovery of docs/STRESS-v0.1.0.md: the per-window event latency p99 around the spike, and
# the time from the end of the spike phase until the windowed p99 is back at or under the baseline
# (the p99 of the first phase as a whole) and stays there for the rest of the run.
# Usage: python spike.py REPORT.json (from docs/stress-v0.1.0/raw)
import json, sys
r = json.load(open(sys.argv[1]))
phases = r["phases"]
base, spike, after = phases[0], phases[2], phases[3]
limit = base["latency"]["p99_ms"]
print(f"## {sys.argv[1]} (seed {r['seed']}, timer {r['timer_10ms_before_ms']}/{r['timer_10ms_after_ms']} ms)")
print(f"baseline p99 ({base['name']}): {limit:.1f} ms; spike {spike['name']} from {spike['from_s']:.1f} s to {spike['to_s']:.1f} s")
inside = [w for w in r["windows"] if base["from_s"] < w["t_s"] <= base["to_s"] and w["latency"]["count"]]
print(f"baseline windows over the baseline p99: {sum(w['latency']['p99_ms'] > limit for w in inside)} of {len(inside)}")
print("| t s | target | active | started | ev/s | lat p50 ms | lat p99 ms | ttfe p99 ms | over baseline |")
print("|---|---|---|---|---|---|---|---|---|")
recovered = None
for w in r["windows"]:
    t, p99 = w["t_s"], w["latency"]["p99_ms"]
    over = w["latency"]["count"] > 0 and p99 > limit
    if t >= spike["from_s"] - 10:
        print(f"| {t:.0f} | {w['target_streams']} | {w['active_streams']} | {w['streams_started']} | {w['events_per_s']:.0f} | "
              f"{w['latency']['p50_ms']:.1f} | {p99:.1f} | {w['ttfe']['p99_ms']:.1f} | {'yes' if over else ''} |")
    if t > spike["to_s"]:
        if over:
            recovered = None
        elif recovered is None:
            recovered = t
if recovered is None:
    print("not back under the baseline p99 for good before the run ended")
else:
    # A window is reported at its end: the window ending at `recovered` is the first one of the run
    # of windows under the baseline, so p99 was back within that window.
    print(f"back under the baseline p99 for good from the window ending at {recovered:.0f} s: "
          f"{recovered - spike['to_s']:.0f} s after the spike phase ended")
# The windowed p99 is noisy (it alternates from one 5 s window to the next in the baseline too), so
# also report recovery against the band of windowed p99 seen during the baseline phase.
band = max(w["latency"]["p99_ms"] for w in inside)
print(f"baseline windowed p99 band: {min(w['latency']['p99_ms'] for w in inside):.1f} - {band:.1f} ms")
back = None
for w in r["windows"]:
    if w["t_s"] > spike["to_s"] and w["latency"]["count"]:
        if w["latency"]["p99_ms"] > band:
            back = None
        elif back is None:
            back = w["t_s"]
print(f"back inside the band for good from the window ending at {back:.0f} s: {back - spike['to_s']:.0f} s after the spike phase ended")
