"""Integration tests for the Kafka data source using testcontainers.

A single-node Kafka container is started once per module and torn down at
the end. Messages are seeded via a synchronous producer, then read back
through Sail's Spark Connect server via ``spark.read.format("kafka")``.
"""

from __future__ import annotations

import json
import time

import pytest

from pysail.testing.spark.utils.common import pyspark_version

pytestmark = pytest.mark.integration

if pyspark_version() < (4, 1):
    pytest.skip("Python data source requires Spark 4.1+", allow_module_level=True)

confluent_kafka = pytest.importorskip("confluent_kafka")
testcontainers_kafka = pytest.importorskip("testcontainers.kafka")

Producer = confluent_kafka.Producer
KafkaContainer = testcontainers_kafka.KafkaContainer


_TOPIC = "sail-test-topic"
_TOPIC_ALT = "sail-test-topic-alt"
_TOPIC_MSG_COUNT = 10
_TOPIC_ALT_MSG_COUNT = 5


@pytest.fixture(scope="module")
def kafka_container():
    with KafkaContainer() as container:
        yield container


@pytest.fixture(scope="module")
def bootstrap_servers(kafka_container):
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="module", autouse=True)
def _seed_messages(bootstrap_servers):
    """Produce a fixed set of messages to two topics, single partition each."""
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for i in range(_TOPIC_MSG_COUNT):
        producer.produce(
            _TOPIC,
            key=f"key-{i}".encode(),
            value=f"value-{i}".encode(),
            partition=0,
            headers=[("h1", b"v1"), ("h2", b"v2")],
        )
        producer.flush(timeout=15)
        time.sleep(0.01)
    for i in range(_TOPIC_ALT_MSG_COUNT):
        producer.produce(
            _TOPIC_ALT,
            key=f"alt-{i}".encode(),
            value=f"alt-value-{i}".encode(),
            partition=0,
        )
    producer.flush(timeout=15)


@pytest.fixture(scope="module", autouse=True)
def register_kafka(spark):
    from pysail.spark.datasource.kafka import KafkaDataSource

    spark.dataSource.register(KafkaDataSource)


@pytest.fixture
def kafka_opts(bootstrap_servers):
    return {"kafka.bootstrap.servers": bootstrap_servers}


# ---------------------------------------------------------------------------
# Basic read
# ---------------------------------------------------------------------------


def test_basic_read(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", _TOPIC).options(**kafka_opts).load()
    rows = df.collect()
    assert len(rows) == _TOPIC_MSG_COUNT
    assert all(r.topic == _TOPIC for r in rows)
    values = sorted(bytes(r.value).decode() for r in rows)
    assert values == [f"value-{i}" for i in range(_TOPIC_MSG_COUNT)]
    # timestampType must use Spark's enum values (0=CreateTime, 1=LogAppendTime),
    # not librdkafka's (1=CreateTime, 2=LogAppendTime). Default topic config is
    # CreateTime, so every row should report 0.
    assert all(r.timestampType == 0 for r in rows)


# ---------------------------------------------------------------------------
# Fixed Spark-compatible schema
# ---------------------------------------------------------------------------


def test_schema(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", _TOPIC).options(**kafka_opts).load()
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert schema_map["key"] == "binary"
    assert schema_map["value"] == "binary"
    assert schema_map["topic"] == "string"
    assert schema_map["partition"] == "int"
    assert schema_map["offset"] == "bigint"
    assert schema_map["timestampType"] == "int"
    assert "headers" not in schema_map


# ---------------------------------------------------------------------------
# includeHeaders exposes the headers column
# ---------------------------------------------------------------------------


def test_include_headers(spark, kafka_opts):
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("includeHeaders", "true")
        .options(**kafka_opts)
        .load()
    )
    field_names = {f.name for f in df.schema.fields}
    assert "headers" in field_names
    row = df.collect()[0]
    hdr_keys = {h.key for h in row.headers}
    assert hdr_keys == {"h1", "h2"}


# ---------------------------------------------------------------------------
# JSON-scoped startingOffsets and endingOffsets
# ---------------------------------------------------------------------------


def test_json_offsets(spark, kafka_opts):
    starting = json.dumps({_TOPIC: {"0": 2}})
    ending = json.dumps({_TOPIC: {"0": 7}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingOffsets", starting)
        .option("endingOffsets", ending)
        .options(**kafka_opts)
        .load()
    )
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == [2, 3, 4, 5, 6]  # end exclusive


# ---------------------------------------------------------------------------
# Spark-style sentinel offsets (-1 latest, -2 earliest)
# ---------------------------------------------------------------------------


def test_sentinel_offsets(spark, kafka_opts):
    starting = json.dumps({_TOPIC: {"0": -2}})
    ending = json.dumps({_TOPIC: {"0": -1}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingOffsets", starting)
        .option("endingOffsets", ending)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == _TOPIC_MSG_COUNT


# ---------------------------------------------------------------------------
# Empty range returns zero rows without hanging
# ---------------------------------------------------------------------------


def test_empty_range(spark, kafka_opts):
    starting = json.dumps({_TOPIC: {"0": 3}})
    ending = json.dumps({_TOPIC: {"0": 3}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingOffsets", starting)
        .option("endingOffsets", ending)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == 0


# ---------------------------------------------------------------------------
# Multi-topic subscribe unions both topics
# ---------------------------------------------------------------------------


def test_multi_topic(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", f"{_TOPIC},{_TOPIC_ALT}").options(**kafka_opts).load()
    topics = {r.topic for r in df.collect()}
    assert topics == {_TOPIC, _TOPIC_ALT}
    assert df.count() == _TOPIC_MSG_COUNT + _TOPIC_ALT_MSG_COUNT


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_missing_bootstrap_raises(spark):
    with pytest.raises(Exception, match=r"bootstrap\.servers"):
        spark.read.format("kafka").option("subscribe", _TOPIC).load().collect()


def test_missing_subscribe_raises(spark, kafka_opts):
    with pytest.raises(Exception, match=r"subscribe"):
        spark.read.format("kafka").options(**kafka_opts).load().collect()


def test_unknown_topic_raises(spark, kafka_opts):
    with pytest.raises(Exception, match=r"not found"):
        spark.read.format("kafka").option("subscribe", "does-not-exist").options(**kafka_opts).load().collect()


# ---------------------------------------------------------------------------
# Timestamp-based reads (offsets_for_times)
# ---------------------------------------------------------------------------


def test_starting_timestamp(spark, kafka_opts):
    """Global startingTimestamp resolves via offsets_for_times."""
    # Grab the timestamp of message at offset 5, then start from there.
    baseline = (
        spark.read.format("kafka").option("subscribe", _TOPIC).options(**kafka_opts).load().orderBy("offset").collect()
    )
    pivot_ts_ms = int(baseline[5].timestamp.timestamp() * 1000)

    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingTimestamp", str(pivot_ts_ms))
        .options(**kafka_opts)
        .load()
    )
    rows = sorted(df.collect(), key=lambda r: r.offset)
    assert rows, "startingTimestamp should return at least the pivot message"
    assert all(int(r.timestamp.timestamp() * 1000) >= pivot_ts_ms for r in rows)
    assert rows[-1].offset == _TOPIC_MSG_COUNT - 1


def test_offsets_by_timestamp_json(spark, kafka_opts):
    """Per-partition startingOffsetsByTimestamp with a far-past timestamp
    behaves like earliest."""
    starting = json.dumps({_TOPIC: {"0": 0}})  # epoch = before any message
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingOffsetsByTimestamp", starting)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == _TOPIC_MSG_COUNT


def test_starting_timestamp_future_reads_nothing(spark, kafka_opts):
    """A startingTimestamp far in the future resolves to the high watermark,
    so no rows are returned — but the query does not fail. This is the
    'never miss a message' semantic: rerun later and any new messages will
    be picked up."""
    far_future_ms = 4102444800000  # year 2100
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingTimestamp", str(far_future_ms))
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == 0


def test_conflicting_starting_options_raise(spark, kafka_opts):
    with pytest.raises(Exception, match=r"cannot be combined|mutually exclusive"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("startingOffsets", "earliest")
            .option("startingTimestamp", "1000")
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_ending_timestamp(spark, kafka_opts):
    """endingTimestamp resolves via offsets_for_times and bounds the read."""
    baseline = (
        spark.read.format("kafka").option("subscribe", _TOPIC).options(**kafka_opts).load().orderBy("offset").collect()
    )
    pivot_ts_ms = int(baseline[5].timestamp.timestamp() * 1000)

    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("endingTimestamp", str(pivot_ts_ms))
        .options(**kafka_opts)
        .load()
    )
    rows = sorted(df.collect(), key=lambda r: r.offset)
    assert rows, "endingTimestamp should return at least the earliest messages"
    # Every returned row has timestamp strictly less than the pivot — the
    # ending offset is the first offset with ts >= pivot, and the read range
    # is end-exclusive.
    assert all(int(r.timestamp.timestamp() * 1000) < pivot_ts_ms for r in rows)


def test_ending_offsets_by_timestamp_json(spark, kafka_opts):
    """Per-partition endingOffsetsByTimestamp with a far-future timestamp
    reads everything currently in the log."""
    far_future_ms = 4102444800000  # year 2100
    ending = json.dumps({_TOPIC: {"0": far_future_ms}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("endingOffsetsByTimestamp", ending)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == _TOPIC_MSG_COUNT


def test_conflicting_ending_options_raise(spark, kafka_opts):
    with pytest.raises(Exception, match=r"cannot be combined|mutually exclusive"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("endingOffsets", "latest")
            .option("endingTimestamp", "1000")
            .options(**kafka_opts)
            .load()
            .collect()
        )
