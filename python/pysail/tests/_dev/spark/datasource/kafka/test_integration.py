# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the Kafka data source using testcontainers.

A single-node Kafka container is started once per module and torn down at
the end. Messages are seeded via a synchronous producer, then read back
through Sail's Spark Connect server via ``spark.read.format("kafka")``.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

pytestmark = [
    pytest.mark.integration("kafka"),
    pytest.mark.spark(version=">=4.0", reason="Uses the Python DataSource API"),
    # Allow time for a cold Kafka image pull and container startup in CI.
    pytest.mark.timeout(300),
]


TOPIC = "sail-test-topic"
TOPIC_ALT = "sail-test-topic-alt"
TOPIC_MSG_COUNT = 10
TOPIC_ALT_MSG_COUNT = 5


def make_producer(bootstrap_servers: str, **extra: str):
    confluent_kafka = pytest.importorskip("confluent_kafka")
    return confluent_kafka.Producer({"bootstrap.servers": bootstrap_servers, **extra})


@pytest.fixture(scope="module")
def kafka_container():
    pytest.importorskip("confluent_kafka")
    try:
        from testcontainers.community.kafka import KafkaContainer
    except ImportError:
        KafkaContainer = pytest.importorskip("testcontainers.kafka").KafkaContainer  # noqa: N806

    container = KafkaContainer(image="confluentinc/cp-kafka:7.6.1")
    container.with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    container.with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    with container:
        yield container


@pytest.fixture(scope="module")
def bootstrap_servers(kafka_container):
    return kafka_container.get_bootstrap_server()


def flush_producer(producer) -> None:
    """Flush and fail fast if the producer queue did not drain."""
    remaining = producer.flush(timeout=15)
    assert remaining == 0, f"producer failed to drain within 15s; {remaining} message(s) still queued"


@pytest.fixture(scope="module", autouse=True)
def seed_messages(bootstrap_servers):
    """Produce a fixed set of messages to two topics, single partition each."""
    producer = make_producer(bootstrap_servers)
    for i in range(TOPIC_MSG_COUNT):
        producer.produce(
            TOPIC,
            key=f"key-{i}".encode(),
            value=f"value-{i}".encode(),
            partition=0,
            headers=[("h1", b"v1"), ("h2", b"v2")],
        )
        flush_producer(producer)
        time.sleep(0.01)
    for i in range(TOPIC_ALT_MSG_COUNT):
        producer.produce(
            TOPIC_ALT,
            key=f"alt-{i}".encode(),
            value=f"alt-value-{i}".encode(),
            partition=0,
        )
    flush_producer(producer)


@pytest.fixture(scope="module", autouse=True)
def register_kafka(spark):
    from pysail.spark.datasource.kafka import KafkaDataSource

    spark.dataSource.register(KafkaDataSource)


@pytest.fixture
def kafka_opts(bootstrap_servers):
    return {"kafka.bootstrap.servers": bootstrap_servers}


def test_basic_read(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", TOPIC).options(**kafka_opts).load()
    rows = df.collect()
    assert len(rows) == TOPIC_MSG_COUNT
    assert all(r.topic == TOPIC for r in rows)
    values = sorted(bytes(r.value).decode() for r in rows)
    assert values == [f"value-{i}" for i in range(TOPIC_MSG_COUNT)]
    # timestampType must use Spark's enum values (0=CreateTime, 1=LogAppendTime),
    # not librdkafka's (1=CreateTime, 2=LogAppendTime). Default topic config is
    # CreateTime, so every row should report 0.
    assert all(r.timestampType == 0 for r in rows)


def test_schema(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", TOPIC).options(**kafka_opts).load()
    schema_map = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert schema_map["key"] == "binary"
    assert schema_map["value"] == "binary"
    assert schema_map["topic"] == "string"
    assert schema_map["partition"] == "int"
    assert schema_map["offset"] == "bigint"
    assert schema_map["timestampType"] == "int"
    assert "headers" not in schema_map
    assert all(field.nullable for field in df.schema.fields)


def test_include_headers(spark, kafka_opts):
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("includeHeaders", "true")
        .options(**kafka_opts)
        .load()
    )
    field_names = {f.name for f in df.schema.fields}
    assert "headers" in field_names
    row = df.collect()[0]
    hdr_keys = {h.key for h in row.headers}
    assert hdr_keys == {"h1", "h2"}


def test_tombstones_and_nullable_duplicate_headers(spark, bootstrap_servers, kafka_opts):
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = "sail-nullable-records"
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(timeout=30)
    producer = make_producer(bootstrap_servers)
    records = [
        (None, None),
        (b"", []),
        (b"nullable", [("h", None)]),
        (b"duplicate", [("h", b"first"), ("h", b"second")]),
    ]
    for value, headers in records:
        producer.produce(topic, key=b"key", value=value, headers=headers, partition=0)
    flush_producer(producer)

    df = (
        spark.read.format("kafka")
        .option("subscribe", topic)
        .option("includeHeaders", "true")
        .options(**kafka_opts)
        .load()
    )
    rows = sorted(df.collect(), key=lambda row: row.offset)
    actual = [(row.value, [(h.key, h.value) for h in row.headers] if row.headers is not None else None) for row in rows]
    assert actual == [(value, headers or None) for value, headers in records]
    assert all(field.nullable for field in df.schema.fields)
    headers_type = df.schema["headers"].dataType
    assert headers_type.containsNull
    assert all(field.nullable for field in headers_type.elementType.fields)


def test_json_offsets(spark, kafka_opts):
    starting = json.dumps({TOPIC: {"0": 2}})
    ending = json.dumps({TOPIC: {"0": 7}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting)
        .option("endingOffsets", ending)
        .options(**kafka_opts)
        .load()
    )
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == [2, 3, 4, 5, 6]  # end exclusive


def test_sentinel_offsets(spark, kafka_opts):
    starting = json.dumps({TOPIC: {"0": -2}})
    ending = json.dumps({TOPIC: {"0": -1}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting)
        .option("endingOffsets", ending)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == TOPIC_MSG_COUNT


def test_empty_range(spark, kafka_opts):
    starting = json.dumps({TOPIC: {"0": 3}})
    ending = json.dumps({TOPIC: {"0": 3}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting)
        .option("endingOffsets", ending)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == 0


def test_multi_topic(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", f"{TOPIC},{TOPIC_ALT}").options(**kafka_opts).load()
    topics = {r.topic for r in df.collect()}
    assert topics == {TOPIC, TOPIC_ALT}
    assert df.count() == TOPIC_MSG_COUNT + TOPIC_ALT_MSG_COUNT


def test_duplicate_topic_not_double_counted(spark, kafka_opts):
    """Repeated topics in `subscribe` are deduped rather than scanned twice."""
    df = spark.read.format("kafka").option("subscribe", f"{TOPIC},{TOPIC}").options(**kafka_opts).load()
    assert df.count() == TOPIC_MSG_COUNT
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == list(range(TOPIC_MSG_COUNT))


def test_unknown_topic_raises(spark, kafka_opts):
    with pytest.raises(Exception, match=r"not found"):
        spark.read.format("kafka").option("subscribe", "does-not-exist").options(**kafka_opts).load().collect()


def test_starting_timestamp(spark, kafka_opts):
    """Global startingTimestamp resolves via a timestamp list_offsets lookup."""
    # Grab the timestamp of message at offset 5, then start from there.
    baseline = (
        spark.read.format("kafka").option("subscribe", TOPIC).options(**kafka_opts).load().orderBy("offset").collect()
    )
    pivot_ts_ms = int(baseline[5].timestamp.timestamp() * 1000)

    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("startingTimestamp", str(pivot_ts_ms))
        .options(**kafka_opts)
        .load()
    )
    rows = sorted(df.collect(), key=lambda r: r.offset)
    assert rows, "startingTimestamp should return at least the pivot message"
    assert all(int(r.timestamp.timestamp() * 1000) >= pivot_ts_ms for r in rows)
    assert rows[-1].offset == TOPIC_MSG_COUNT - 1


def test_offsets_by_timestamp_json(spark, kafka_opts):
    """Per-partition startingOffsetsByTimestamp with a far-past timestamp
    behaves like earliest."""
    starting = json.dumps({TOPIC: {"0": 0}})  # epoch = before any message
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("startingOffsetsByTimestamp", starting)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == TOPIC_MSG_COUNT


def test_starting_timestamp_future_raises_by_default(spark, kafka_opts):
    """Unmatched starting timestamps fail by default (strategy='error')."""
    far_future_ms = 4102444800000  # year 2100
    with pytest.raises(Exception, match=r"No offset matches the requested timestamp"):
        (
            spark.read.format("kafka")
            .option("subscribe", TOPIC)
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
        .option("subscribe", TOPIC)
        .option("startingTimestamp", str(far_future_ms))
        .option("startingOffsetsByTimestampStrategy", "latest")
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == 0


def test_starting_timestamp_takes_precedence_over_offsets(spark, kafka_opts):
    """Global timestamp wins over startingOffsets without raising."""
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("startingTimestamp", "0")  # epoch: before every message
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == TOPIC_MSG_COUNT


def test_ending_timestamp(spark, kafka_opts):
    """endingTimestamp resolves via a timestamp lookup and bounds the read."""
    baseline = (
        spark.read.format("kafka").option("subscribe", TOPIC).options(**kafka_opts).load().orderBy("offset").collect()
    )
    pivot_ts_ms = int(baseline[5].timestamp.timestamp() * 1000)

    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
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
    ending = json.dumps({TOPIC: {"0": far_future_ms}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("endingOffsetsByTimestamp", ending)
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == TOPIC_MSG_COUNT


def test_ending_timestamp_takes_precedence_over_offsets(spark, kafka_opts):
    far_future_ms = 4102444800000  # year 2100
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("endingOffsets", "latest")
        .option("endingTimestamp", str(far_future_ms))
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == TOPIC_MSG_COUNT


def test_assign(spark, kafka_opts):
    df = spark.read.format("kafka").option("assign", json.dumps({TOPIC: [0]})).options(**kafka_opts).load()
    assert df.count() == TOPIC_MSG_COUNT
    assert {r.topic for r in df.collect()} == {TOPIC}


@pytest.mark.parametrize("assignment", [{}, {TOPIC: []}, {TOPIC: [7]}])
def test_assign_skips_partitions_that_do_not_exist(spark, kafka_opts, assignment):
    """Empty or nonexistent partitions in `assign` are skipped rather than rejected."""
    df = spark.read.format("kafka").option("assign", json.dumps(assignment)).options(**kafka_opts).load()
    assert df.count() == 0


def test_assign_keeps_the_partitions_that_do_exist(spark, kafka_opts):
    df = spark.read.format("kafka").option("assign", json.dumps({TOPIC: [0, 7]})).options(**kafka_opts).load()
    assert df.count() == TOPIC_MSG_COUNT


def test_subscribe_pattern(spark, kafka_opts):
    """The pattern is searched within topic names, as Spark's `findFirstIn` is."""
    df = spark.read.format("kafka").option("subscribePattern", "sail-test-topic(-alt)?").options(**kafka_opts).load()
    assert {r.topic for r in df.collect()} == {TOPIC, TOPIC_ALT}
    assert df.count() == TOPIC_MSG_COUNT + TOPIC_ALT_MSG_COUNT


def test_subscribe_pattern_is_not_anchored(spark, kafka_opts):
    """Unanchored prefix matches all topics containing it, matching Spark."""
    df = spark.read.format("kafka").option("subscribePattern", "sail-test-topic").options(**kafka_opts).load()
    assert {r.topic for r in df.collect()} == {TOPIC, TOPIC_ALT}


def test_subscribe_pattern_narrow(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribePattern", ".*-alt").options(**kafka_opts).load()
    assert {r.topic for r in df.collect()} == {TOPIC_ALT}


def test_min_partitions_preserves_rows(spark, kafka_opts):
    df = spark.read.format("kafka").option("subscribe", TOPIC).option("minPartitions", "4").options(**kafka_opts).load()
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == list(range(TOPIC_MSG_COUNT))


def test_max_records_per_partition_preserves_rows(spark, kafka_opts):
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("maxRecordsPerPartition", "3")
        .options(**kafka_opts)
        .load()
    )
    offsets = sorted(r.offset for r in df.collect())
    assert offsets == list(range(TOPIC_MSG_COUNT))


def test_ending_offset_past_end_of_log_raises(spark, kafka_opts):
    """An end offset beyond the log is data loss under default failOnDataLoss=true."""
    ending = json.dumps({TOPIC: {"0": TOPIC_MSG_COUNT + 1000}})
    with pytest.raises(Exception, match=r"Data loss"):
        (
            spark.read.format("kafka")
            .option("subscribe", TOPIC)
            .option("endingOffsets", ending)
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_fail_on_data_loss_false_returns_partial(spark, kafka_opts):
    ending = json.dumps({TOPIC: {"0": TOPIC_MSG_COUNT + 1000}})
    df = (
        spark.read.format("kafka")
        .option("subscribe", TOPIC)
        .option("endingOffsets", ending)
        .option("failOnDataLoss", "false")
        .options(**kafka_opts)
        .load()
    )
    assert df.count() == TOPIC_MSG_COUNT


def test_protected_consumer_option_raises(spark, kafka_opts):
    """Verify option-validation errors surface through Spark Connect."""
    with pytest.raises(Exception, match=r"is not supported"):
        (
            spark.read.format("kafka")
            .option("subscribe", TOPIC)
            .option("kafka.auto.offset.reset", "earliest")
            .options(**kafka_opts)
            .load()
            .collect()
        )


def test_user_specified_schema_rejected(spark, kafka_opts):
    """Verify custom .schema(...) reaches KafkaDataSource.reader and is rejected."""
    with pytest.raises(Exception, match=r"fixed schema"):
        (
            spark.read.format("kafka")
            .schema("value STRING")
            .option("subscribe", TOPIC)
            .options(**kafka_opts)
            .load()
            .collect()
        )


def plan_and_read(bootstrap_servers, topic, before_read, **options):
    """Plan, run ``before_read`` against the broker between planning and reading, then read."""
    from pysail.spark.datasource.kafka import KafkaDataSource

    reader = KafkaDataSource(
        options={"kafka.bootstrap.servers": bootstrap_servers, "subscribe": topic, **options}
    ).reader(None)
    partitions = reader.partitions()
    before_read()
    offsets = []
    for partition in partitions:
        for batch in reader.read(partition):
            offsets.extend(batch.column("offset").to_pylist())
    return sorted(offsets)


def test_late_binding_picks_up_records_produced_after_planning(bootstrap_servers):
    """`latest` binds at task start, picking up records produced after planning."""
    topic = "sail-test-late-bind-tail"
    producer = make_producer(bootstrap_servers)
    for i in range(4):
        producer.produce(topic, value=f"before-{i}".encode(), partition=0)
    flush_producer(producer)

    def produce_more():
        for i in range(3):
            producer.produce(topic, value=f"after-{i}".encode(), partition=0)
        flush_producer(producer)

    # Planned while the log held 4 records; 3 more land before the read runs.
    assert plan_and_read(bootstrap_servers, topic, produce_more) == [0, 1, 2, 3, 4, 5, 6]


def seed_topic(bootstrap_servers, topic, count):
    producer = make_producer(bootstrap_servers)
    for i in range(count):
        producer.produce(topic, value=f"value-{i}".encode(), partition=0)
    flush_producer(producer)


def truncate_before(bootstrap_servers, topic, offset):
    """Delete every record below ``offset``, as retention eventually would."""
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient

    def run():
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        futures = admin.delete_records([TopicPartition(topic, 0, offset)])
        for future in futures.values():
            future.result(timeout=30)

    return run


def test_late_binding_survives_retention_after_planning(bootstrap_servers):
    """`earliest` binds at task start, surviving retention between planning and reading."""
    topic = "sail-test-late-bind-retention"
    seed_topic(bootstrap_servers, topic, 10)

    # Offsets 0-5 are deleted between planning and reading. The read must bind
    # `earliest` to the new low watermark rather than fail on the stale one.
    truncate = truncate_before(bootstrap_servers, topic, 6)
    assert plan_and_read(bootstrap_servers, topic, truncate) == [6, 7, 8, 9]


def test_explicit_start_below_the_log_start_is_data_loss(bootstrap_servers):
    """Explicit start offset removed by retention fails under default failOnDataLoss=true."""
    topic = "sail-test-truncated-start-fails"
    seed_topic(bootstrap_servers, topic, 10)
    truncate = truncate_before(bootstrap_servers, topic, 6)

    with pytest.raises(ValueError, match=r"no longer available on the broker"):
        plan_and_read(
            bootstrap_servers,
            topic,
            truncate,
            startingOffsets=json.dumps({topic: {"0": 0}}),
            endingOffsets=json.dumps({topic: {"0": 10}}),
        )


def test_explicit_start_below_the_log_start_recovers_when_allowed(bootstrap_servers):
    """With failOnDataLoss=false, out-of-range recovery resumes within the planned range."""
    topic = "sail-test-truncated-start-recovers"
    seed_topic(bootstrap_servers, topic, 10)
    truncate = truncate_before(bootstrap_servers, topic, 6)

    assert plan_and_read(
        bootstrap_servers,
        topic,
        truncate,
        startingOffsets=json.dumps({topic: {"0": 0}}),
        endingOffsets=json.dumps({topic: {"0": 10}}),
        failOnDataLoss="false",
    ) == [6, 7, 8, 9]


def test_partition_eof_reports_the_log_end_offset(bootstrap_servers):
    """Pin librdkafka's contract that _PARTITION_EOF carries the reached log-end offset."""
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    topic = "sail-test-eof-offset"
    record_count = 3
    seed_topic(bootstrap_servers, topic, record_count)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "sail-test-eof-offset",
            "enable.auto.commit": False,
            "auto.offset.reset": "error",
            "enable.partition.eof": True,
        }
    )
    try:
        consumer.assign([TopicPartition(topic, 0, 0)])
        deadline = time.monotonic() + 30
        eof_offset = None
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            error = message.error()
            if error is None:
                continue
            assert error.code() == KafkaError._PARTITION_EOF, str(error)  # noqa: SLF001
            eof_offset = message.offset()
            break
    finally:
        consumer.close()

    assert eof_offset == record_count, "librdkafka no longer stamps the EOF event with the log end offset"


@pytest.mark.parametrize(
    ("isolation", "expected"),
    [
        ("read_committed", [b"committed"]),
        ("read_uncommitted", [b"committed", b"aborted", b"open"]),
    ],
)
def test_transaction_visibility_in_planning_and_reading(spark, bootstrap_servers, isolation, expected):
    from confluent_kafka.admin import AdminClient, NewTopic

    topic = f"sail-transactions-{isolation}"
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(timeout=30)
    producer = make_producer(bootstrap_servers, **{"transactional.id": topic})
    producer.init_transactions(30)
    for value, commit in [(b"committed", True), (b"aborted", False)]:
        producer.begin_transaction()
        producer.produce(topic, value=value, partition=0)
        flush_producer(producer)
        if commit:
            producer.commit_transaction(30)
        else:
            producer.abort_transaction(30)
    producer.begin_transaction()
    try:
        producer.produce(topic, value=b"open", partition=0)
        flush_producer(producer)
        rows = (
            spark.read.format("kafka")
            .options(
                **{
                    "kafka.bootstrap.servers": bootstrap_servers,
                    "subscribe": topic,
                    "kafka.isolation.level": isolation,
                    "minPartitions": "2",
                    "pollTimeoutMs": "10000",
                }
            )
            .load()
            .collect()
        )
        assert [bytes(r.value) for r in sorted(rows, key=lambda r: r.offset)] == expected
    finally:
        producer.abort_transaction(30)


def test_concurrent_queries_share_a_group_without_committing(spark, bootstrap_servers):
    from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition

    group = "sail-kafka-concurrent-read"

    def read():
        return (
            spark.read.format("kafka")
            .options(
                **{
                    "kafka.bootstrap.servers": bootstrap_servers,
                    "subscribe": TOPIC,
                    "kafka.group.id": group,
                    "minPartitions": "3",
                    "maxBatchRows": "2",
                }
            )
            .load()
            .collect()
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: read(), range(2)))
    for rows in results:
        assert sorted(r.offset for r in rows) == list(range(TOPIC_MSG_COUNT))
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group, "enable.auto.commit": False})
    try:
        assert consumer.committed([TopicPartition(TOPIC, 0)], timeout=10)[0].offset == OFFSET_INVALID
    finally:
        consumer.close()
