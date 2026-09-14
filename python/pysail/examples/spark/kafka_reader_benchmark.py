"""Broker-free batching probe; run each case in a fresh process for peak RSS.

Run with ``python -m pysail.examples.spark.kafka_reader_benchmark``.

This measures Python/Arrow buffering, not Kafka throughput or total worker memory.
Pass --baseline and the old module source on stdin to compare identical workloads.
"""

import argparse
import importlib
import json
import resource
import sys
import time
import types
from unittest.mock import patch


class _Message:
    def __init__(self, offset, payload_bytes):
        self.index = offset
        self.payload_bytes = payload_bytes

    def error(self):
        return None

    def offset(self):
        return self.index

    def topic(self):
        return "benchmark"

    def partition(self):
        return 0

    def timestamp(self):
        return (1, 1700000000000)

    def key(self):
        return None

    def value(self):
        # Confluent Message exposes cached Python bytes; match that ownership.
        if not hasattr(self, "payload"):
            self.payload = bytes(bytearray(self.payload_bytes))
        return self.payload

    def headers(self):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--payload-bytes", type=int, default=102400)
    args = parser.parse_args()
    # Match package/native initialization in both arms; importing the current
    # module otherwise includes this RSS cost only in the non-baseline arm.
    importlib.import_module("pysail.spark")
    if args.baseline:
        module = types.ModuleType("kafka_benchmark_baseline")
        sys.modules[module.__name__] = module
        exec(compile(sys.stdin.read(), "<baseline>", "exec"), module.__dict__)  # noqa: S102
    else:
        from pysail.spark.datasource import kafka as module

    class Consumer:
        def __init__(self, config):
            self.offset = 0

        def assign(self, partitions):
            self.offset = partitions[0].offset

        def poll(self, timeout):
            message = _Message(self.offset, args.payload_bytes)
            self.offset += 1
            return message

        def close(self):
            pass

    options = {"kafka.bootstrap.servers": "unused", "subscribe": "benchmark"}
    reader = module.KafkaDataSource(options=options).reader(None)
    # Concrete endpoints avoid broker access without bypassing read()/conversion.
    parameters = {
        key: getattr(reader, key)
        for key in [
            "client_config",
            "include_headers",
            "poll_timeout_s",
            "max_batch_rows",
            "stall_timeout_s",
            "admin_timeout_s",
            "fail_on_data_loss",
        ]
    }
    for key in ["max_batch_bytes", "max_batch_wait_s"]:
        if hasattr(reader, key):
            parameters[key] = getattr(reader, key)
    partition = module.KafkaInputPartition(
        0,
        topic="benchmark",
        kafka_partition=0,
        start_offset=0,
        end_offset=args.rows,
        **parameters,
    )
    rows = batches = max_batch_bytes = 0
    started = time.monotonic()
    first_batch_s = None
    with patch.object(module, "Consumer", Consumer):
        for batch in reader.read(partition):
            if first_batch_s is None:
                first_batch_s = time.monotonic() - started
            assert batch.column("offset").to_pylist() == list(range(rows, rows + batch.num_rows))
            rows += batch.num_rows
            batches += 1
            max_batch_bytes = max(max_batch_bytes, batch.nbytes)
    assert rows == args.rows
    elapsed = time.monotonic() - started
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(
        json.dumps(
            {
                "baseline": args.baseline,
                "rows": rows,
                "payload_bytes": args.payload_bytes,
                "batches": batches,
                "max_batch_bytes": max_batch_bytes,
                "elapsed_s": elapsed,
                "first_batch_s": first_batch_s,
                "peak_rss_bytes": peak if sys.platform == "darwin" else peak * 1024,
            }
        )
    )


if __name__ == "__main__":
    main()
