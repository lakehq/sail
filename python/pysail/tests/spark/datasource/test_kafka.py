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


def _flush(producer) -> None:
    """Flush and assert the queue drained.

    ``flush()`` returns the number of messages still queued when it gives up.
    Ignoring that lets the suite seed fewer messages than expected and fail
    later in an unrelated assertion, which is a confusing way to learn the
    broker was slow.
    """
    remaining = producer.flush(timeout=15)
    assert remaining == 0, f"producer failed to drain within 15s; {remaining} message(s) still queued"


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
        _flush(producer)
        time.sleep(0.01)
    for i in range(_TOPIC_ALT_MSG_COUNT):
        producer.produce(
            _TOPIC_ALT,
            key=f"alt-{i}".encode(),
            value=f"alt-value-{i}".encode(),
            partition=0,
        )
    _flush(producer)


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


def test_inverted_range_raises(spark, kafka_opts):
    """An explicit start-past-end range must fail rather than silently
    returning a partial (here empty) result, matching Spark's treatment of an
    inverted range as data loss."""
    starting = json.dumps({_TOPIC: {"0": 7}})
    ending = json.dumps({_TOPIC: {"0": 3}})
    with pytest.raises(Exception, match=r"Inverted offset range"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("startingOffsets", starting)
            .option("endingOffsets", ending)
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_offsets_for_unsubscribed_topic_raises(spark, kafka_opts):
    """A typo'd topic in the offsets JSON must not be silently ignored."""
    with pytest.raises(Exception, match=r"must specify all assigned TopicPartitions"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("startingOffsets", json.dumps({"not-subscribed": {"0": 2}}))
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_offsets_missing_a_partition_raises(spark, kafka_opts):
    """Spark asserts the specific-offsets map covers every assigned partition.

    An omitted partition would otherwise fall back to a watermark, so the read
    silently starts somewhere the user did not ask for.
    """
    with pytest.raises(Exception, match=r"must specify all assigned TopicPartitions"):
        (
            spark.read.format("kafka")
            .option("subscribe", f"{_TOPIC},{_TOPIC_ALT}")
            .option("startingOffsets", json.dumps({_TOPIC: {"0": 2}}))
            .options(**kafka_opts)
            .load()
            .collect()
        )


# ---------------------------------------------------------------------------
# Multi-topic subscribe unions both topics
# ---------------------------------------------------------------------------


def test_multi_topic(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", f"{_TOPIC},{_TOPIC_ALT}").options(**kafka_opts).load()
    topics = {r.topic for r in df.collect()}
    assert topics == {_TOPIC, _TOPIC_ALT}
    assert df.count() == _TOPIC_MSG_COUNT + _TOPIC_ALT_MSG_COUNT


def test_duplicate_topic_not_double_counted(spark, kafka_opts):
    """A repeated topic in `subscribe` must be deduped.

    Without dedup each occurrence is enumerated separately, producing duplicate
    input partitions over the same offset range and emitting every row twice.
    """
    df = spark.read.format("kafka").option("subscribe", f"{_TOPIC},{_TOPIC}").options(**kafka_opts).load()
    assert df.count() == _TOPIC_MSG_COUNT
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == list(range(_TOPIC_MSG_COUNT))


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


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("startingOffsets", "latest"),
        ("endingOffsets", "earliest"),
        ("startingOffsets", json.dumps({_TOPIC: {"0": -1}})),
        ("endingOffsets", json.dumps({_TOPIC: {"0": -2}})),
    ],
)
def test_wrong_endpoint_sentinel_raises(spark, kafka_opts, option, value):
    """Spark's batch source rejects latest-at-start and earliest-at-end.

    Both always resolve to an empty range, so they must fail loudly rather than
    silently returning no rows.
    """
    with pytest.raises(Exception, match=r"does not support"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option(option, value)
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_out_of_range_offset_sentinel_raises(spark, kafka_opts):
    """-1000 is librdkafka's OFFSET_STORED; passed through it would silently
    read from committed consumer-group offsets."""
    with pytest.raises(Exception, match=r"must be >= 0"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("startingOffsets", json.dumps({_TOPIC: {"0": -1000}}))
            .options(**kafka_opts)
            .load()
            .collect()
        )


# ---------------------------------------------------------------------------
# Timestamp-based reads (list_offsets by timestamp)
# ---------------------------------------------------------------------------


def test_starting_timestamp(spark, kafka_opts):
    """Global startingTimestamp resolves via a timestamp list_offsets lookup."""
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


def test_starting_timestamp_future_raises_by_default(spark, kafka_opts):
    """Spark defaults startingOffsetsByTimestampStrategy to `error`.

    A future or mistaken starting timestamp must fail rather than silently
    return an empty result.
    """
    far_future_ms = 4102444800000  # year 2100
    with pytest.raises(Exception, match=r"No offset matches the requested timestamp"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("startingTimestamp", str(far_future_ms))
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_starting_timestamp_future_with_latest_strategy_reads_nothing(spark, kafka_opts):
    """Only an explicit `latest` strategy falls back to the high watermark."""
    far_future_ms = 4102444800000  # year 2100
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingTimestamp", str(far_future_ms))
        .option("startingOffsetsByTimestampStrategy", "latest")
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == 0


def test_starting_timestamp_takes_precedence_over_offsets(spark, kafka_opts):
    """Spark applies precedence rather than rejecting the combination.

    The global timestamp wins and the offsets value is never parsed, so a job
    supplying a fallback keeps working.
    """
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("startingOffsets", "earliest")
        .option("startingTimestamp", "0")  # epoch: before every message
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == _TOPIC_MSG_COUNT


def test_ending_timestamp(spark, kafka_opts):
    """endingTimestamp resolves via a timestamp lookup and bounds the read."""
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


def test_ending_timestamp_takes_precedence_over_offsets(spark, kafka_opts):
    far_future_ms = 4102444800000  # year 2100
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("endingOffsets", "latest")
        .option("endingTimestamp", str(far_future_ms))
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == _TOPIC_MSG_COUNT


# ---------------------------------------------------------------------------
# Subscription strategies — assign / subscribePattern
# ---------------------------------------------------------------------------


def test_assign(spark, kafka_opts):
    df = spark.read.format("kafka").option("assign", json.dumps({_TOPIC: [0]})).options(**kafka_opts).load()
    assert df.count() == _TOPIC_MSG_COUNT
    assert {r.topic for r in df.collect()} == {_TOPIC}


def test_assign_unknown_partition_raises(spark, kafka_opts):
    with pytest.raises(Exception, match=r"partitions that do not exist"):
        (spark.read.format("kafka").option("assign", json.dumps({_TOPIC: [7]})).options(**kafka_opts).load().collect())


def test_subscribe_pattern(spark, kafka_opts):
    """The pattern is matched against whole topic names, as Spark does."""
    df = spark.read.format("kafka").option("subscribePattern", "sail-test-topic(-alt)?").options(**kafka_opts).load()
    assert {r.topic for r in df.collect()} == {_TOPIC, _TOPIC_ALT}
    assert df.count() == _TOPIC_MSG_COUNT + _TOPIC_ALT_MSG_COUNT


def test_subscribe_pattern_narrow(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribePattern", ".*-alt").options(**kafka_opts).load()
    assert {r.topic for r in df.collect()} == {_TOPIC_ALT}


def test_multiple_subscription_strategies_raise(spark, kafka_opts):
    with pytest.raises(Exception, match=r"Only one of the following options"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("assign", json.dumps({_TOPIC: [0]}))
            .options(**kafka_opts)
            .load()
            .collect()
        )


# ---------------------------------------------------------------------------
# minPartitions / maxRecordsPerPartition
#
# Spark splits Kafka offset ranges into additional input partitions for these.
# The observable guarantee through the DataFrame API is that splitting changes
# neither the rows nor their offsets.
# ---------------------------------------------------------------------------


def test_min_partitions_preserves_rows(spark, kafka_opts):
    df = (
        spark.read.format("kafka").option("subscribe", _TOPIC).option("minPartitions", "4").options(**kafka_opts).load()
    )
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == list(range(_TOPIC_MSG_COUNT))


def test_max_records_per_partition_preserves_rows(spark, kafka_opts):
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("maxRecordsPerPartition", "3")
        .options(**kafka_opts)
        .load()
    )
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == list(range(_TOPIC_MSG_COUNT))


# ---------------------------------------------------------------------------
# failOnDataLoss
# ---------------------------------------------------------------------------


def test_ending_offset_past_end_of_log_raises(spark, kafka_opts):
    """An end offset the log cannot reach is data loss under the default.

    The read stops at end-of-log below the planned end, and with
    failOnDataLoss=true (Spark's default) that must fail rather than return a
    partial result that looks like a successful read.
    """
    ending = json.dumps({_TOPIC: {"0": _TOPIC_MSG_COUNT + 1000}})
    with pytest.raises(Exception, match=r"Data loss"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("endingOffsets", ending)
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_fail_on_data_loss_false_returns_partial(spark, kafka_opts):
    ending = json.dumps({_TOPIC: {"0": _TOPIC_MSG_COUNT + 1000}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("endingOffsets", ending)
        .option("failOnDataLoss", "false")
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == _TOPIC_MSG_COUNT


def test_invalid_fail_on_data_loss_raises(spark, kafka_opts):
    with pytest.raises(Exception, match=r"must be 'true' or 'false'"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("failOnDataLoss", "maybe")
            .options(**kafka_opts)
            .load()
            .collect()
        )


# ---------------------------------------------------------------------------
# Option validation that mirrors Spark's
# ---------------------------------------------------------------------------


def test_invalid_include_headers_raises(spark, kafka_opts):
    """Spark rejects a non-boolean; silently treating it as false would drop
    the headers column the user asked for."""
    with pytest.raises(Exception, match=r"must be 'true' or 'false'"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("includeHeaders", "yes")
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_protected_consumer_option_raises(spark, kafka_opts):
    """`auto.offset.reset` is owned by the source: the planner already resolved
    a concrete start, so a reset policy could only move it silently."""
    with pytest.raises(Exception, match=r"is not supported"):
        (
            spark.read.format("kafka")
            .option("subscribe", _TOPIC)
            .option("kafka.auto.offset.reset", "earliest")
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_user_specified_schema_rejected(spark, kafka_opts):
    """Spark's Kafka source has a fixed schema and rejects a custom one."""
    with pytest.raises(Exception, match=r"fixed schema"):
        (
            spark.read.format("kafka")
            .schema("value STRING")
            .option("subscribe", _TOPIC)
            .options(**kafka_opts)
            .load()
            .collect()
        )


# ---------------------------------------------------------------------------
# Late binding of earliest/latest
#
# These drive the data source API directly rather than going through a
# DataFrame. Spark Connect plans lazily at `collect()`, so the DataFrame API
# gives no seam between planning and reading — and the seam is exactly what
# these assert. Everything below still runs against the real broker: real
# AdminClient, real consumer, real watermarks. A dedicated topic per test keeps
# them from perturbing the row counts the rest of the module asserts.
# ---------------------------------------------------------------------------


def _plan_and_read(bootstrap_servers, topic, before_read, **options):
    """Plan a read, run ``before_read`` against the broker, then read.

    Returns the offsets the read produced. The callback runs after
    ``partitions()`` and before ``read()``, which is the window in which a
    late-bound endpoint is supposed to notice the broker has moved on.
    """
    from pysail.spark.datasource.kafka import KafkaDataSource

    reader = KafkaDataSource(
        options={"kafka.bootstrap.servers": bootstrap_servers, "subscribe": topic, **options}
    ).reader(None)
    partitions = reader.partitions()
    before_read()
    offsets = []
    for partition in partitions:
        for batch in reader.read(partition):
            offsets.extend(batch.to_pylist()[i]["offset"] for i in range(batch.num_rows))
    return sorted(offsets)


def test_late_binding_picks_up_records_produced_after_planning(bootstrap_servers):
    """`latest` means the end of the log when the task runs, as in Spark.

    Spark carries the -1 sentinel to the executor and binds it in
    `KafkaSourceRDD.resolveRange`. Resolving on the driver instead would stop at
    the watermark captured during planning and silently drop this tail.
    """
    topic = "sail-test-late-bind-tail"
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for i in range(4):
        producer.produce(topic, value=f"before-{i}".encode(), partition=0)
    _flush(producer)

    def produce_more():
        for i in range(3):
            producer.produce(topic, value=f"after-{i}".encode(), partition=0)
        _flush(producer)

    # Planned while the log held 4 records; 3 more land before the read runs.
    assert _plan_and_read(bootstrap_servers, topic, produce_more) == [0, 1, 2, 3, 4, 5, 6]


def test_late_binding_survives_retention_after_planning(bootstrap_servers):
    """An `earliest` start stays valid when the log ages on mid-query.

    A start frozen at planning would now sit below the low watermark, and with
    the default `failOnDataLoss=true` that fails a query Spark completes by
    simply re-resolving `earliest` at task start.
    """
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient

    topic = "sail-test-late-bind-retention"
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for i in range(10):
        producer.produce(topic, value=f"value-{i}".encode(), partition=0)
    _flush(producer)

    def truncate():
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        futures = admin.delete_records([TopicPartition(topic, 0, 6)])
        for future in futures.values():
            future.result(timeout=30)

    # Offsets 0-5 are deleted between planning and reading. The read must bind
    # `earliest` to the new low watermark rather than fail on the stale one.
    assert _plan_and_read(bootstrap_servers, topic, truncate) == [6, 7, 8, 9]


def test_schema_fields_are_nullable(spark, kafka_opts):
    """Spark declares every Kafka column nullable; nullability is user-visible
    in printSchema and in write-compatibility checks."""
    df = (
        spark.read.format("kafka")
        .option("subscribe", _TOPIC)
        .option("includeHeaders", "true")
        .options(**kafka_opts)
        .load()
    )
    assert all(f.nullable for f in df.schema.fields)
