import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

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


def test_foreach_batch_proto_parsing(spark):
    # Regression test: Python UDF output_type must be optional for foreachBatch.
    # Previously, this would fail with "Python UDF output type is required" during
    # proto parsing because the foreachBatch callback PythonUDF has no output_type.
    # The expected behavior is to fail with "unsupported" rather than a proto error.
    df = spark.readStream.format("rate").load()

    def batch_fn(batch_df, batch_id):
        pass

    with pytest.raises(Exception, match=r"(?i)not support"):
        df.writeStream.foreachBatch(batch_fn).start()
