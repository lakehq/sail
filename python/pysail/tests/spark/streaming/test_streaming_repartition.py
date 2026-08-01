"""Streaming plans must not be round-robin repartitioned.

Round-robin repartitioning buys parallelism, and for an unbounded input it costs
unbounded latency instead: `RepartitionExec` coalesces on the producer side,
holding rows until `batch_size` of them accumulate and only flushing the
remainder when the input ends. A stream does not end, so with the default batch
size a slow source delivers nothing for a very long time — the query reports
active the whole while.

There is no sink a Connect client can assert delivery against (`console` writes
to the server's stdout, and `memory` is not implemented), so this pins the plan
shape instead.
"""

from __future__ import annotations

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

if is_jvm_spark():
    pytest.skip("Sail streaming tests", allow_module_level=True)


def test_streaming_plan_has_no_round_robin_repartition(spark, capsys):
    query = spark.readStream.format("rate").option("rowsPerSecond", 5).load().writeStream.format("console").start()
    try:
        query.explain(extended=True)
        plan = capsys.readouterr().out
    finally:
        query.stop()

    assert "RateSourceExec" in plan, f"unexpected plan:\n{plan}"
    assert "RoundRobinBatch" not in plan, f"streaming plan was repartitioned:\n{plan}"


def test_batch_plan_still_uses_round_robin_repartition(spark, capsys):
    # The streaming case must not have disabled repartitioning everywhere:
    # a batch query still fans out to use the available parallelism.
    spark.range(1000).selectExpr("id % 7 as k").groupBy("k").count().explain(extended=True)
    plan = capsys.readouterr().out

    assert "RoundRobinBatch" in plan, f"batch plan lost its parallelism:\n{plan}"
