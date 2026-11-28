# SPDX-License-Identifier: Apache-2.0

import pytest

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Tests Sail streaming query behavior")


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


@pytest.mark.parametrize("source", ["parquet", "delta"])
@pytest.mark.parametrize("sink", ["parquet", "json"])
def test_streaming_query_to_a_file_sink_is_not_supported(spark, tmp_path, source, sink):
    path = str(tmp_path / "source")
    spark.createDataFrame([(1,)], "id INT").write.format(source).save(path)
    reader = spark.readStream.format(source)
    # A Delta Lake table has its own schema.
    frame = (reader.schema("id INT") if source == "parquet" else reader).load(path)
    writer = (
        frame.writeStream.format(sink)
        .option("path", str(tmp_path / "sink"))
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
    )
    with pytest.raises(Exception, match="file sink is not supported"):
        writer.start()
