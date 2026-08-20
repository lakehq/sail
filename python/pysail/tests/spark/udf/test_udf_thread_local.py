import threading

import pandas as pd
import pytest
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

from pysail.testing.spark.utils.common import is_jvm_spark

# Enough rows to produce far more batches, and therefore far more UDF calls,
# than there are worker threads, so at least one thread must be called twice.
ROW_COUNT = 1 << 20

# Created on first use rather than at import, so that nothing unpicklable is
# captured when the UDF is serialized on the client.
_state = None


def _call_count() -> int:
    """Returns how many times the calling thread has reached this function.

    The counter lives in a `threading.local`, whose per-thread storage hangs off
    the `PyThreadState` rather than the OS thread.
    """
    global _state  # noqa: PLW0603
    if _state is None:
        _state = threading.local()
    count = getattr(_state, "count", 0) + 1
    _state.count = count
    return count


@pandas_udf(LongType())
def call_count(values: pd.Series) -> pd.Series:
    return pd.Series([_call_count()] * len(values), dtype="int64")


@pytest.mark.skipif(is_jvm_spark(), reason="asserts a property of the Sail in-process execution model")
def test_thread_local_state_survives_between_udf_calls(spark):
    """Python state kept in a `threading.local` must survive between UDF calls.

    A worker thread whose `PyThreadState` is destroyed after every call gets a
    fresh `threading.local` every time, so the counter never leaves 1. Libraries
    that cache native handles per thread rely on that state surviving, and one
    that also caches a raw pointer outside the thread state is left holding a
    dangling pointer. See https://github.com/lakehq/sail/issues/2456.
    """
    df = spark.range(0, ROW_COUNT).select(call_count(col("id")).alias("count"))
    highest = df.agg({"count": "max"}).collect()[0][0]
    assert highest > 1, "every call saw an empty threading.local, so the thread state did not survive"
