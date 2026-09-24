# Jev delayed-mock benchmark

Build and install the native extension using the project development instructions, then run:

```sh
hatch run python scripts/jev/benchmark.py --rows 64 256 --repeat 3 --output /tmp/jev-benchmark.json
```

The benchmark uses loopback-only HTTP and Spark Connect servers. It never calls TypeSafe and needs no real API key. Each measurement starts a fresh process and mock service. It compares concurrency 1 and 8, partitions 1 and 4, and distinct states, repeated states, and structured states containing a 32 KiB payload. Every mock request delays its response by 20 ms. Cluster task retries are disabled to isolate the Jev HTTP behavior.

The command fails if requests do not overlap, worker resource caps are exceeded, repeated states do not batch, resource instrumentation is absent, or distinct-state median throughput fails to improve. It reports row/request throughput, observed HTTP concurrency, reservation peaks from Jev debug logs, and process peak RSS. The mock retains counters rather than all request bodies.

Keep generated reports outside version control and attach relevant results to the PR or CI run. Record the commit, build profile, machine specifications, and command alongside each report. These synthetic measurements validate batching and concurrency; they do not establish live-service or production throughput.
