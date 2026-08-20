"""Free-threading (no-GIL) regression tests for Sail's Python bridge.

These tests verify that Sail itself is sound when Python UDFs run truly
concurrently on a free-threaded CPython build:

- importing ``pysail`` keeps the GIL disabled (the ``gil_used = false``
  module declaration);
- Python UDFs execute on multiple threads with the GIL off inside the
  in-process Spark Connect server;
- results are correct and stable across repeated runs, including concurrent
  initialization of Sail's cached Python state (the per-UDF ``LazyPyObject``
  caches and the shared ``PyOnceLock`` module cache).

The tests skip themselves unless the interpreter is free-threaded *and* the
GIL is actually disabled. The Spark Connect client imports ``grpcio``, which
does not yet declare free-threading support (grpc/grpc#38762) and would
re-enable the GIL; run the tests with ``PYTHON_GIL=0`` to override:

    PYTHON_GIL=0 python3.14t -m pytest python/pysail/tests/test_free_threading.py
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

NUM_PARTITIONS = 8
NUM_ROWS = 4000
# The minimum number of distinct OS threads the probe UDF must be observed on
# for the execution to count as concurrent.
MIN_UDF_THREADS = 2


def _gil_disabled() -> bool:
    f = getattr(sys, "_is_gil_enabled", None)
    return f is not None and not f()


pytestmark = pytest.mark.skipif(
    not _gil_disabled(),
    reason=(
        "requires a free-threaded (no-GIL) interpreter with the GIL disabled; "
        "run under CPython 3.13t/3.14t with PYTHON_GIL=0 (grpcio, imported by "
        "the Spark Connect client, is not yet free-threading-safe and would "
        "otherwise re-enable the GIL; see grpc/grpc#38762)"
    ),
)


@pytest.fixture(scope="module")
def ft_remote():
    """Starts a dedicated in-process Spark Connect server with a fixed
    execution parallelism, so UDFs run concurrently across partitions.

    The execution config is read when the server is created, so the
    environment variables must be set before ``SparkConnectServer`` is
    instantiated.
    """
    keys = ("SAIL_EXECUTION__DEFAULT_PARALLELISM", "SAIL_EXECUTION__BATCH_SIZE")
    saved = {k: os.environ.get(k) for k in keys}
    os.environ["SAIL_EXECUTION__DEFAULT_PARALLELISM"] = str(NUM_PARTITIONS)
    os.environ["SAIL_EXECUTION__BATCH_SIZE"] = "128"
    try:
        from pysail.spark import SparkConnectServer

        assert _gil_disabled(), "importing pysail must not re-enable the GIL"
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        yield f"sc://localhost:{port}"
        server.stop()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@pytest.fixture(scope="module")
def ft_spark(ft_remote):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(ft_remote).getOrCreate()
    if not _gil_disabled():
        spark.stop()
        pytest.skip("the GIL was re-enabled by Spark Connect client imports; run with PYTHON_GIL=0")
    yield spark
    spark.stop()


def test_pysail_import_keeps_gil_disabled():
    """Importing pysail alone must keep the GIL disabled.

    This checks the ``#[pymodule(gil_used = false)]`` declaration on the
    native module. It runs in a subprocess with ``PYTHON_GIL`` unset so the
    interpreter's default behavior (re-enabling the GIL when a module without
    free-threading support is imported) is observable; in the parent process
    ``PYTHON_GIL=0`` would mask a missing declaration.
    """
    env = {k: v for k, v in os.environ.items() if k != "PYTHON_GIL"}
    code = "import pysail, sys; print(sys._is_gil_enabled())"
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    assert result.stdout.strip() == "False", (
        f"importing pysail re-enabled the GIL (stdout={result.stdout!r}, stderr={result.stderr!r}); "
        "is the native module missing `#[pymodule(gil_used = false)]`?"
    )


def test_gil_disabled_inside_udf_threads(ft_spark):
    """A Python UDF observes a disabled GIL and runs on multiple threads."""
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import StringType

    @F.udf(returnType=StringType())
    def probe(_i):
        import sys
        import threading

        # A little busy work so partitions genuinely overlap in time.
        acc = 0
        for k in range(1000):
            acc += k * k
        return f"{sys._is_gil_enabled()}|{threading.get_ident()}|{acc % 7}"  # noqa: SLF001

    df = ft_spark.range(NUM_ROWS).repartition(NUM_PARTITIONS)
    rows = df.select(probe(F.col("id")).alias("p")).groupBy("p").count().collect()
    assert sum(r["count"] for r in rows) == NUM_ROWS
    gil_states = {r["p"].split("|")[0] for r in rows}
    thread_ids = {r["p"].split("|")[1] for r in rows}
    assert gil_states == {"False"}, f"GIL was enabled inside UDF execution: {gil_states}"
    assert len(thread_ids) >= MIN_UDF_THREADS, "expected UDF execution on multiple threads"


def test_udf_results_correct_and_stable(ft_spark):
    """UDF results are exact and identical across repeated concurrent runs."""
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import LongType

    @F.udf(returnType=LongType())
    def mix(i):
        return (i * 2654435761) % 2147483647

    expected = sum((i * 2654435761) % 2147483647 for i in range(NUM_ROWS))
    df = ft_spark.range(NUM_ROWS).repartition(NUM_PARTITIONS)
    for _ in range(3):
        row = df.select(mix(F.col("id")).alias("v")).agg(F.sum("v").alias("s"), F.count("v").alias("c")).collect()[0]
        assert row["c"] == NUM_ROWS
        assert row["s"] == expected


def test_concurrent_udf_initialization_and_cached_state(ft_spark):
    """Distinct UDFs evaluated concurrently do not corrupt Sail's caches.

    Each UDF has its own lazily-initialized Python wrapper (``LazyPyObject``)
    and all of them share the ``PyOnceLock``-cached helper module; evaluating
    several UDFs across many partitions makes their first initialization race
    across worker threads. Running the query twice also exercises reuse of the
    cached state after initialization.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import LongType

    factors = [3, 5, 7, 11]

    def make_udf(f):
        @F.udf(returnType=LongType())
        def scaled(i):
            return i * f + (i % f)

        return scaled

    udfs = [make_udf(f) for f in factors]
    expected = [sum(i * f + (i % f) for i in range(NUM_ROWS)) for f in factors]

    df = ft_spark.range(NUM_ROWS).repartition(NUM_PARTITIONS)
    for _ in range(2):
        row = (
            df.select(*[u(F.col("id")).alias(f"c{k}") for k, u in enumerate(udfs)])
            .agg(*[F.sum(f"c{k}").alias(f"s{k}") for k in range(len(udfs))])
            .collect()[0]
        )
        got = [row[f"s{k}"] for k in range(len(udfs))]
        assert got == expected
