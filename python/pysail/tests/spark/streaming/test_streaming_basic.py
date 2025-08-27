import pytest

from pysail.tests.spark.utils import is_jvm_spark

if is_jvm_spark():
    pytest.skip("Sail streaming tests", allow_module_level=True)


def test_streaming_query(spark):
    df = spark.readStream.format("rate").load()
    query = df.writeStream.format("console").queryName("streaming-1").start()

    try:
        assert query.name == "streaming-1"
        assert isinstance(query.id, str)
        assert isinstance(query.runId, str)
        assert query.isActive
        assert isinstance(query.status.get("message"), str)
        # Sail does not report progress yet
        assert query.lastProgress is None
        assert query.recentProgress == []
        assert query.exception() is None
        query.explain()
        query.processAllAvailable()
        query.awaitTermination(0.001)
    finally:
        query.stop()


def test_streaming_query_manager(spark):
    df = spark.readStream.format("rate").load()
    query = df.writeStream.format("console").queryName("streaming-2").start()

    try:
        assert any(query.id == x.id for x in spark.streams.active)
        spark.streams.awaitAnyTermination(0.001)
        assert query.name == spark.streams.get(query.id).name
    finally:
        query.stop()
    assert all(query.id != x.id for x in spark.streams.active)
    spark.streams.awaitAnyTermination()
    spark.streams.resetTerminated()
