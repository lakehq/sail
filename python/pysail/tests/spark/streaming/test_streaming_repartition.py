"""Streaming plans must not be round-robin repartitioned.

Round-robin repartitioning buys parallelism, and for an unbounded input it costs
unbounded latency instead: `RepartitionExec` coalesces on the producer side,
holding rows until `batch_size` of them accumulate and only flushing the
remainder when the input ends. A stream does not end, so with the default batch
size a slow source delivers nothing for a very long time — the query reports
active the whole while.

There is no sink a Connect client can assert delivery against (`console` writes
to the server's stdout, and `memory` is not implemented), so these tests pin the
plan shape instead: the streaming plan snapshot must not contain a
`RepartitionExec`, and the batch plan snapshot must keep its `RoundRobinBatch`
so the streaming rule is known not to have leaked into batch planning.
"""

from __future__ import annotations

import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import streaming_explain_string

if is_jvm_spark():
    pytest.skip("Sail streaming tests", allow_module_level=True)


@pytest.mark.yamlsnapshot(group="plan")
def test_streaming_plan_is_not_repartitioned(spark, snapshot):
    query = spark.readStream.format("rate").option("rowsPerSecond", 5).load().writeStream.format("console").start()
    try:
        plan = normalize_plan_text(streaming_explain_string(query, extended=True))
    finally:
        query.stop()

    assert plan == snapshot


@pytest.mark.yamlsnapshot(group="plan")
def test_batch_plan_keeps_round_robin_repartition(spark, snapshot):
    df = spark.range(1000).selectExpr("id % 7 as k").groupBy("k").count()
    plan = normalize_plan_text(df._explain_string(extended=True))  # noqa: SLF001

    assert plan == snapshot
