# Progress

Journal of work on the repository. Newest entries last. Results and their raw outputs live in
[benchmarks/v0.1.0](../benchmarks/v0.1.0/README.md) and [raw/](../benchmarks/v0.1.0/raw).

## Stress campaign v0.1.0 (branch `test/stress-v0.1.0`)

### Blockers

None: no test revealed a defect in the library. Every failure in the campaign has a measured cause
outside it (see the report).

### 2026-10-26

- Read the spec (G1–G7), docs/06 and the example. The private project notes,
  `docs/AUDIT-FINAL-v0.1.0.md` and a previous `docs/PROGRESS.md` are not part of the public
  repository; the dependency allowlist is rule 7 of CONTRIBUTING.md (stdlib + the allowlist is all
  the harness uses).
- Environment: Docker Desktop failed to start (WSL could not create its VM, HCS 0x800705aa, with
  3.7 GB of RAM free); fixed by the owner. Host port 8080 is held by another Windows service, so the
  stress overlay publishes nginx on 127.0.0.1:18080; the harness reaches it on the compose network.
- Tooling: `internal/loadtest` (timestamped test producer, checker, phases driver, samplers, trend
  fit, SVG charts), `cmd/rebound-load`, `scripts/stress/run.sh`, Makefile targets `load`,
  `load-baseline`, `soak`, `spike`, `storm`, `faults`, `slow` (none in `make check`).
  `internal/timercheck -v` prints the measured timer latency, kept with each run.
- The example mounts `POST /loadtest/streams` with `CONFORMANCE=1` (the conformance producer has no
  timestamps, so it cannot give an event latency) and serves pprof/expvar on a loopback
  `DEBUG_ADDR`.
- Checker verification: a fake server injects a gap, a duplicate, a missing terminal, terminals that
  differ between readers, events from another instance or producer run, a payload of another seq
  and a log that differs from what readers got; the checker reports each one
  (`TestChecker_ReportsEachInjectedViolation`), and nothing on a clean server or the real Hub.
- An edit to `run.sh` while it ran made bash liable to execute a fragment of the new text: the run
  was aborted and restarted, and the script now parses whole before it runs.
- ramp-1 (nginx as shipped): the knee is nginx itself. `examples/deploy/nginx.conf` has `events {}`, i.e. one
  worker with 512 connections; from 200 streams it logs `worker_connections are not enough`
  (1 377 661 times) and clients see transport errors. The instances logged no warning or error;
  every failed stream still retained was `done` at seq 401 in Redis.
- ramp-tuned-1 (nginx limits raised by the stress overlay only, HTTP block unchanged): no HTTP
  error at any step; Redis reaches 100 % of its single core from 200 streams and throughput flattens
  at 6–8 k delivered events/s. Missing terminals at 1000–2000 streams are readers giving up at their
  deadline on streams slowed by the saturated store; the streams checked were `done` in Redis. The
  reader now reports these connections as `deadline` instead of `read_error`.
- Added start (POST → 201) and connect (GET → headers) latencies, to decompose time to first event.
- ramp-fine-1 and ramp-fine-2 (50–300 streams, nginx tuned): the knee is 150 streams in both runs
  (delivery 90 % of offered, p99 event latency 70 → 210 ms); integrity clean at every step.
- baseline-1 (one instance, memory store, no nginx): knee at 1000 streams.
- ramp-shipped-fine-1 (nginx as shipped, 50–150 streams): clean at 50 and 100; at 150, 10 379
  transport errors from nginx, readers retried, integrity still clean.
- `rebound-load steps` aggregates the samples of each phase into one table row.
- soak-1 (75 streams, 30 % of connections dropped, 2 h): PASS. No integrity violation over 25 275
  streams; heap, goroutines, fds and Redis flat; RSS +0.3 %/h in the second hour (R² ≈ 0.1);
  goroutines back to 7, Redis keys back to 0 within 11.6 minutes of the end.
- spike-1 (75 → idle → 375 streams in 10 s → 75): PASS; windowed p99 back inside the baseline
  band 35 s after the spike phase (the time for the spike's last streams to finish).
- storm-1, storm-2 (drops every 1–5 events) and storm-ms-1 (every 5–50 ms): PASS; about 1 000
  resumes per reader (2 300 in the timed variant), half of them on the other instance, no gap,
  duplicate, move or restart.
- Faults at the soak load: Redis paused 2 s and Redis restarted with AOF: PASS (appends retried,
  no duplicate). Redis restarted as shipped (no persistence): the 75 streams in flight are lost
  and readers get 404, as docs/how-it-works.md documents; not a library defect. `docker kill`
  app-1: PASS, `producer_lost` 15.5–20.5 s after the kill; nginx's default connect timeout holds
  requests routed to the dead instance for tens of seconds. `docker stop` app-2: PASS in its three
  outcomes (`done` within the grace, `server_shutdown` at 30 s, `producer_lost` when Docker's
  default 10 s SIGKILLs first). Added `fault-redis-pause`, `REDIS=aof` and `FAULT_EVENTS` to the
  campaign script for these variants.
- Slow clients: every slow connection is released by `httpx: write timeout` and the reader
  resumes without loss. slow-1 counted the slow readers' late events in the latency of the normal
  streams; the harness now keeps them out of the windows (tested). slow-heavy-control-1 (the same
  large streams, read without pausing) is the control for the volume they write; slow-3 is on par
  with it. slow-2 had one 2.9 s stall of all streams that slow-1 and slow-3 did not reproduce;
  cause not found, reported as is.
- Report written (docs/benchmarks/v0.1.0/README.md) with a summary table, the deployment findings and what
  could not be tested; README "Performance" section with the measured knees.
- Slow clients: the harness counted the slow readers' late events in the latency of the normal
  streams (slow-1); they now stay out of the windows (`TestRun_SlowReaderIsReleasedAndResumes`
  asserts it). slow-2 and slow-3 against a control and a control with the same large streams read
  normally: PASS; every slow reader released by `httpx: write timeout` and resumed without loss,
  the other streams on par with the heavy control. One 2.9 s stall in slow-2 did not recur in
  slow-1 or slow-3 (empty Redis); cause not found.
- Report written: docs/benchmarks/v0.1.0/README.md (summary, per-test tables, findings, limits); README
  "Performance" section; the example's `/loadtest/streams` and `DEBUG_ADDR` documented.
