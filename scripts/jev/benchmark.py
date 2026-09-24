"""Reproducible delayed-loopback benchmark for Sail's Jev SQL functions.

Run after installing the patched native extension:
    hatch run python scripts/jev/benchmark.py --rows 64 256 --repeat 3 --output /tmp/jev-benchmark.json

Every measurement starts a fresh process so worker limits and peak RSS are isolated.
The mock retains counters only. Input/output Arrow storage and the mock's HTTP threads
are included in RSS. Queue reservation peaks come from the Jev debug log, not from
the public SQL API. Missing instrumentation is reported as a verification gap.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path


def measure(args):
    from pysail.testing.jev import JevMock

    with JevMock(delay=args.delay_ms / 1000, keep_requests=False) as mock:
        settings = {
            "RUST_LOG": "sail_function::scalar::jev=debug",
            "SAIL_MODE": "local-cluster",
            "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(args.partitions),
            "TYPESAFE_JEV_MAX_CONCURRENCY": str(args.concurrency),
            "TYPESAFE_JEV_MAX_PENDING_REQUESTS": str(args.pending_requests),
            "TYPESAFE_JEV_MAX_PENDING_BYTES": str(args.pending_bytes),
            "TYPESAFE_JEV_BATCH_TARGET_QUESTIONS": str(args.target_questions),
            "TYPESAFE_JEV_BATCH_TARGET_BYTES": str(args.target_bytes),
            "TYPESAFE_JEV_MAX_REQUEST_BYTES": str(args.hard_bytes),
            "TYPESAFE_BASE_URL": mock.url,
            "TYPESAFE_API_KEY": "benchmark-dummy-key",
            "TYPESAFE_DEFAULT_MODEL": "jev-benchmark",
        }
        os.environ.update(settings)
        # Import after configuring the process. No external server is used.
        from pyspark.sql import SparkSession

        from pysail.spark import SparkConnectServer

        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        spark = SparkSession.builder.remote(f"sc://127.0.0.1:{port}").create()
        try:
            spark.sql("SELECT 1").collect()
            if args.workload == "repeated":
                state = "'same state'"
            elif args.workload == "large":
                state = f"parse_json(to_json(named_struct('id', id, 'payload', repeat('x', {args.payload_bytes}))))"
            else:
                state = "concat('row-', id)"
            # State expressions are fixed above; counts and payload sizes are parsed as integers.
            query = f"SELECT jev_noul({state}, 'yes?') AS j FROM range(0, {args.row_count}, 1, {args.partitions})"  # noqa: S608
            rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            started = time.perf_counter()
            rows = spark.sql(query).collect()
            elapsed = time.perf_counter() - started
            rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if len(rows) != args.row_count or any(row.j is None for row in rows):
                message = "Jev evaluation changed row cardinality or returned an unexpected null"
                raise RuntimeError(message)
            if mock.peak_active > args.concurrency:
                message = "Worker concurrency cap was exceeded"
                raise RuntimeError(message)
            if args.concurrency > 1 and args.workload != "repeated" and mock.peak_active <= 1:
                message = "Independent HTTP requests did not overlap"
                raise RuntimeError(message)
            if (
                args.workload == "repeated"
                and args.row_count > args.partitions
                and mock.request_count >= args.row_count
            ):
                message = "Same-state requests did not batch"
                raise RuntimeError(message)
            divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
            return {
                "workload": args.workload,
                "rows": len(rows),
                "partitions": args.partitions,
                "concurrency": args.concurrency,
                "mock_delay_ms": args.delay_ms,
                "payload_bytes": args.payload_bytes if args.workload == "large" else 0,
                "elapsed_seconds": elapsed,
                "rows_per_second": len(rows) / elapsed,
                "requests_per_second": mock.request_count / elapsed,
                "requests": mock.request_count,
                "peak_concurrent_attempts_observed_by_mock": mock.peak_active,
                "request_bytes": mock.request_bytes,
                "largest_request_bytes": mock.peak_request_bytes,
                "peak_pending_groups": None,
                "peak_pending_bytes": None,
                "pending_measurement_note": "Parent process extracts reservation peaks from Jev debug logs.",
                "peak_process_rss_mib": rss_after / divisor,
                "peak_process_rss_before_query_mib": rss_before / divisor,
                "limits": {key: value for key, value in settings.items() if key.startswith("TYPESAFE_JEV_")},
            }
        finally:
            spark.stop()
            server.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", nargs="+", type=int, default=[64, 256])
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--delay-ms", type=float, default=20)
    parser.add_argument("--payload-bytes", type=int, default=32768)
    parser.add_argument("--pending-requests", type=int, default=16)
    parser.add_argument("--pending-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--target-questions", type=int, default=64)
    parser.add_argument("--target-bytes", type=int, default=256 * 1024)
    parser.add_argument("--hard-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--workload", choices=["distinct", "repeated", "large"], help=argparse.SUPPRESS)
    parser.add_argument("--row-count", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--partitions", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--concurrency", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        print("JEV_BENCHMARK " + json.dumps(measure(args)))  # noqa: T201
        return
    minimum_rows = 8
    if args.repeat < 1 or any(rows < minimum_rows for rows in args.rows) or args.delay_ms <= 0:
        parser.error("use at least one repeat, at least eight rows, and a positive mock delay")
    runs = []
    for row_count in args.rows:
        for workload in ["distinct", "repeated", "large"]:
            for partitions in [1, 4]:
                for concurrency in [1, 8]:
                    for _ in range(args.repeat):
                        command = [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "--child",
                            "--row-count",
                            str(row_count),
                            "--workload",
                            workload,
                            "--partitions",
                            str(partitions),
                            "--concurrency",
                            str(concurrency),
                        ]
                        for name in [
                            "delay_ms",
                            "payload_bytes",
                            "pending_requests",
                            "pending_bytes",
                            "target_questions",
                            "target_bytes",
                            "hard_bytes",
                        ]:
                            command.extend(["--" + name.replace("_", "-"), str(getattr(args, name))])
                        completed = subprocess.run(command, capture_output=True, text=True, check=False, timeout=300)
                        if completed.returncode:
                            message = (
                                f"Benchmark child failed ({workload}, rows={row_count}, partitions={partitions}, "
                                f"concurrency={concurrency}):\n{completed.stdout}\n{completed.stderr}"
                            )
                            raise RuntimeError(message)
                        result = next(
                            line for line in completed.stdout.splitlines() if line.startswith("JEV_BENCHMARK ")
                        )
                        run = json.loads(result.removeprefix("JEV_BENCHMARK "))
                        peaks = re.findall(
                            r"Jev resource peaks: active_attempts=(\d+) pending_requests=(\d+) pending_bytes=(\d+)",
                            completed.stdout + completed.stderr,
                        )
                        if peaks:
                            active, groups, pending_bytes = map(
                                max, zip(*(map(int, peak) for peak in peaks), strict=True)
                            )
                            run.update(
                                {
                                    "peak_active_attempt_reservations": active,
                                    "peak_pending_groups": groups,
                                    "peak_pending_bytes": pending_bytes,
                                    "pending_measurement_note": "Measured worker reservation peaks from Jev debug logs.",
                                }
                            )
                            if (
                                active > concurrency
                                or groups > args.pending_requests
                                or pending_bytes > args.pending_bytes
                            ):
                                message = "Worker resource reservation cap was exceeded"
                                raise RuntimeError(message)
                        else:
                            run["pending_measurement_note"] = "VERIFICATION GAP: no Jev resource peak log was emitted."
                        runs.append(run)
                        print(  # noqa: T201
                            f"Completed {len(runs)}/{len(args.rows) * 12 * args.repeat}: "
                            f"{workload}, rows={row_count}, partitions={partitions}, concurrency={concurrency}",
                            file=sys.stderr,
                        )
    comparisons = []
    for row_count in args.rows:
        for workload in ["distinct", "repeated", "large"]:
            for partitions in [1, 4]:
                matching = [
                    r for r in runs if (r["rows"], r["workload"], r["partitions"]) == (row_count, workload, partitions)
                ]
                medians = {
                    cap: statistics.median(r["rows_per_second"] for r in matching if r["concurrency"] == cap)
                    for cap in [1, 8]
                }
                comparisons.append(
                    {
                        "rows": row_count,
                        "workload": workload,
                        "partitions": partitions,
                        "speedup_8_vs_1": medians[8] / medians[1],
                        "median_rows_per_second": medians,
                    }
                )
    report = {"runs": runs, "comparisons": comparisons}
    output = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    print(output, end="")  # noqa: T201
    if any(run["peak_pending_groups"] is None for run in runs):
        message = "Resource peak instrumentation was not observed; pending-work bounds remain unverified."
        raise SystemExit(message)
    if any(c["speedup_8_vs_1"] <= 1 for c in comparisons if c["workload"] in {"distinct", "large"}):
        message = "Distinct-state throughput did not improve; inspect the recorded measurements."
        raise SystemExit(message)


if __name__ == "__main__":
    main()
