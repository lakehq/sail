# Jev delayed-mock benchmark

Build and install the patched native extension using the project development instructions, then run:

```sh
hatch run python scripts/jev/benchmark.py --rows 64 256 --repeat 3 --output /tmp/jev-benchmark.json
```

The benchmark uses loopback-only HTTP and Spark Connect servers. It never calls TypeSafe and needs no real API key. Each measurement starts a fresh process and mock service. It compares concurrency 1 and 8, partitions 1 and 4, and distinct states, repeated states, and structured states containing a 32 KiB payload. Every mock request delays its response by 20 ms. Cluster task retries are disabled to isolate the Jev HTTP behavior.

The command fails if requests do not overlap, worker resource caps are exceeded, repeated states do not batch, resource instrumentation is absent, or distinct-state median throughput fails to improve. It reports row/request throughput, observed HTTP concurrency, reservation peaks from Jev debug logs, and process peak RSS. The mock retains counters rather than all request bodies.

## Recorded measurements

[benchmark-results.json](./benchmark-results.json) contains all 72 measurements and comparisons recorded on 2026-09-24 UTC, against baseline `44c43dbc75c88b7c8c519559c1a580247aa8e981` plus the Jev implementation. Environment:

- Apple M2 Pro, 10 CPU cores, 16 GiB RAM; macOS 26.6.2.
- Python 3.11.9 and Rust 1.97.1.
- Development profile, optimization level 0; `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, `CARGO_INCREMENTAL=0`, `CARGO_BUILD_JOBS=4`.
- Native extension loaded from the dedicated worktree's build output, with `PYTHONPATH` pointing to that worktree's `python` directory.
- 8 active attempts, 16 pending groups, 16 MiB reserved request bytes, target 64 questions / 256 KiB, and a 1 MiB hard request limit. The concurrency-one comparisons change only the active-attempt cap.

These are synthetic measurements of an unoptimized development build, not production throughput claims. Early 64-row measurements overlapped Rust unit-test compilation; compilation had finished before the 256-row measurements. The numbers below are medians of three 256-row measurements:

| Workload | Partitions | Rows/s, concurrency 1 | Rows/s, concurrency 8 | Speedup | Requests at concurrency 8 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Distinct states | 1 | 38.6 | 253.0 | 6.55x | 256 |
| Distinct states | 4 | 38.2 | 266.4 | 6.97x | 256 |
| Repeated state | 1 | 1001.0 | 1402.3 | 1.40x | 4 |
| Repeated state | 4 | 1047.7 | 1518.2 | 1.45x | 4 |
| Structured 32 KiB states | 1 | 31.8 | 169.4 | 5.33x | 256 |
| Structured 32 KiB states | 4 | 30.8 | 233.1 | 7.57x | 256 |

At concurrency 8, median requests/second were 253.0 / 266.4 for distinct states, 21.9 / 23.7 for repeated states, and 169.4 / 233.1 for large structured states, for one / four partitions respectively. Every comparison obeyed its configured concurrency cap. Observed peaks reached 8 HTTP attempts, 16 pending groups, and 16 MiB of reserved request bytes.

Increasing total rows from 64 to 256 left the pending-group and reserved-byte maxima at 16 and 16 MiB. Maximum process RSS across those two sets was 250.8 and 279.2 MiB respectively. RSS includes the Python client, native runtime, mock HTTP threads, and required input/output Arrow storage; it is not a measurement of queued bodies alone. The large-state workload necessarily retains more input data as row counts grow. Separate reservation gauges establish bounded queued request memory.

These measurements support the defaults of 8 active attempts and 16 pending groups: independent requests overlap across partitions, while queued encoded bodies stay within a fixed reservation budget. The 64-question target reduces 256 repeated-state rows to four requests. The byte targets and hard limit are conservative Sail resource controls, not observed provider limits or universally optimal settings.

The full matrix's native build preceded the final codec fix for Python UDFs named `jev_*` and reservation-counter cleanup. The final build passed all 77 Jev/contract tests, including the Python UDF override regression. A [final spot check](./benchmark-final-spot.json), using 256 rows, four partitions, and concurrency 8, measured 271.8 rows/s for distinct states (256 requests; peaks of 8 active attempts, 16 pending groups, 16 MiB reservations) and 1502.3 rows/s for repeated states (4 requests; peaks of 4 attempts, 4 groups, 4 MiB). These spot timings may include concurrent regression tests and baseline compilation; they verify final behavior and bounds, not production throughput. No live-service behavior or production workload was measured.

Subsequent production edits only resolved Clippy warnings about automatic dereferencing and a nested conditional. They do not change the measured request scheduling or resource controls. All 77 Jev/contract tests passed again against the exact delivery build after those edits.
