# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Kafka planning: range splitting, and discovery and offset resolution against a fake broker."""

from __future__ import annotations

import itertools

import pytest

from pysail.tests._dev.spark.datasource.kafka.fakes import (
    EARLIEST,
    LATEST,
    TOPIC,
    FakeAdminClient,
    FakeConsumer,
    FakeTopicMeta,
    bound_ranges,
    make_reader,
    planned_ranges,
    read_all,
)

pytestmark = pytest.mark.spark(version=">=4.0", reason="Uses the Python DataSource API")


def test_divide_range_single_part(kafka_planning, range_of_t):
    assert kafka_planning.divide_range(range_of_t(0, 0, 10), 1) == [range_of_t(0, 0, 10)]


def test_divide_range_even_split(kafka_planning, range_of_t):
    assert kafka_planning.divide_range(range_of_t(0, 0, 10), 2) == [
        range_of_t(0, 0, 5),
        range_of_t(0, 5, 10),
    ]


def test_divide_range_remainder_goes_to_latest_chunks(kafka_planning, range_of_t):
    # Spark sizes each chunk against what is left (`remaining / (parts - i)`),
    # so ten records over three chunks are 3/3/4 rather than 4/3/3.
    assert kafka_planning.divide_range(range_of_t(0, 0, 10), 3) == [
        range_of_t(0, 0, 3),
        range_of_t(0, 3, 6),
        range_of_t(0, 6, 10),
    ]
    assert kafka_planning.divide_range(range_of_t(0, 0, 10), 4) == [
        range_of_t(0, 0, 2),
        range_of_t(0, 2, 4),
        range_of_t(0, 4, 7),
        range_of_t(0, 7, 10),
    ]


def test_divide_range_more_parts_than_records(kafka_planning, range_of_t):
    assert kafka_planning.divide_range(range_of_t(0, 0, 2), 5) == [
        range_of_t(0, 0, 1),
        range_of_t(0, 1, 2),
    ]


def test_divide_range_large_partition_hint_caps_at_range_size(kafka_planning, range_of_t):
    # Caps `n_parts` to `offset_range.size` rather than iterating empty chunks.
    assert kafka_planning.divide_range(range_of_t(0, 9, 10), 2**31 - 1) == [range_of_t(0, 9, 10)]


def test_divide_range_chunks_are_contiguous_and_complete(kafka_planning, range_of_t):
    chunks = kafka_planning.divide_range(range_of_t(0, 100, 137), 4)
    assert chunks[0].start == 100
    assert chunks[-1].end == 137
    assert all(left.end == right.start for left, right in itertools.pairwise(chunks))


def test_split_ranges_no_options_is_identity(kafka_planning, range_of_t):
    ranges = [range_of_t(0, 0, 100)]
    assert kafka_planning.split_offset_ranges(ranges, min_partitions=None, max_records_per_partition=None) == ranges


def test_split_ranges_max_records_caps_each_range(kafka_planning, range_of_t):
    out = kafka_planning.split_offset_ranges([range_of_t(0, 0, 10)], min_partitions=None, max_records_per_partition=4)
    assert out == [
        range_of_t(0, 0, 3),
        range_of_t(0, 3, 6),
        range_of_t(0, 6, 10),
    ]
    assert all(r.size <= 4 for r in out)


def test_split_ranges_min_partitions_below_count_is_identity(kafka_planning, range_of_t):
    ranges = [range_of_t(0, 0, 10), range_of_t(1, 0, 10)]
    assert kafka_planning.split_offset_ranges(ranges, min_partitions=2, max_records_per_partition=None) == ranges


def test_split_ranges_min_partitions_splits_proportionally(kafka_planning, range_of_t):
    # One partition, 100 records, minPartitions=4 -> four equal chunks.
    out = kafka_planning.split_offset_ranges([range_of_t(0, 0, 100)], min_partitions=4, max_records_per_partition=None)
    assert out == [
        range_of_t(0, 0, 25),
        range_of_t(0, 25, 50),
        range_of_t(0, 50, 75),
        range_of_t(0, 75, 100),
    ]


def test_split_ranges_min_partitions_weights_by_size(kafka_planning, range_of_t):
    # 90 vs 10 records with minPartitions=10 -> 9 chunks and 1 chunk.
    out = kafka_planning.split_offset_ranges(
        [range_of_t(0, 0, 90), range_of_t(1, 0, 10)],
        min_partitions=10,
        max_records_per_partition=None,
    )
    assert len([r for r in out if r.topic_partition.partition == 0]) == 9
    assert len([r for r in out if r.topic_partition.partition == 1]) == 1


def test_split_ranges_both_options_apply_in_order(kafka_planning, range_of_t):
    out = kafka_planning.split_offset_ranges([range_of_t(0, 0, 100)], min_partitions=8, max_records_per_partition=50)
    # maxRecords gives 2 ranges, then minPartitions splits those up to 8.
    assert len(out) == 8
    assert out[0].start == 0
    assert out[-1].end == 100


def test_split_ranges_records_are_never_lost(kafka_planning, range_of_t):
    out = kafka_planning.split_offset_ranges([range_of_t(0, 5, 47)], min_partitions=7, max_records_per_partition=9)
    assert sum(r.size for r in out) == 42
    assert out[0].start == 5
    assert out[-1].end == 47


def test_split_ranges_unsplit_keys_match_spark_when_both_hints_apply(kafka_planning, range_of_t):
    # Spark exempts the entire topic-partition when one of its chunks is
    # too small to split. minPartitions is a hint, so two chunks is valid.
    out = kafka_planning.split_offset_ranges([range_of_t(0, 0, 7)], min_partitions=3, max_records_per_partition=4)
    assert out == [range_of_t(0, 0, 3), range_of_t(0, 3, 7)]


def test_split_ranges_small_ranges_do_not_inflate_the_partition_count(kafka_planning, range_of_t):
    # Spark sets aside ranges too small to split before dividing the
    # `minPartitions` budget, so the big range takes only the budget the
    # small ones left behind. A single proportional pass would give the big
    # range 4 chunks and still add one per small range, overshooting to 6.
    out = kafka_planning.split_offset_ranges(
        [
            range_of_t(0, 0, 100),
            range_of_t(1, 0, 1),
            range_of_t(2, 0, 1),
        ],
        min_partitions=4,
        max_records_per_partition=None,
    )
    assert out == [
        range_of_t(0, 0, 50),
        range_of_t(0, 50, 100),
        range_of_t(1, 0, 1),
        range_of_t(2, 0, 1),
    ]


def test_split_ranges_skewed_split_matches_spark_budget(kafka_planning, range_of_t):
    out = kafka_planning.split_offset_ranges(
        [range_of_t(0, 0, 1000), range_of_t(1, 0, 5)],
        min_partitions=8,
        max_records_per_partition=None,
    )
    assert len(out) == 8
    assert [r for r in out if r.topic_partition.partition == 1] == [range_of_t(1, 0, 5)]
    assert out[0].start == 0
    assert out[-2].end == 1000


def test_planning_partition_repr_omits_client_credentials(consumer_factory):
    reader = make_reader(
        consumer_factory,
        FakeConsumer(),
        **{"kafka.sasl.password": "test-only-secret", "kafka.ssl.key.password": "test-only-key"},
    )
    partition = reader.partitions()[0]
    assert "test-only-secret" not in repr(partition)
    assert "test-only-key" not in repr(partition)
    assert TOPIC in repr(partition)
    assert not hasattr(partition, "client_config")
    assert reader.options.client_config["sasl.password"] == "test-only-secret"


def test_planning_default_range_is_carried_as_sentinels(consumer_factory):
    from pysail.spark.datasource.kafka.models import OffsetRange, TopicPartitionKey

    # Spark's `fetchPartitionOffsets` returns -2/-1 for earliest/latest and
    # binds them per task, so planning must not freeze them here.
    consumer = FakeConsumer()
    reader = make_reader(consumer_factory, consumer)
    partitions = reader.partitions()
    assert planned_ranges(partitions) == [(TOPIC, 0, EARLIEST, LATEST)]
    assert bound_ranges(reader, consumer, partitions) == [OffsetRange(TopicPartitionKey(TOPIC, 0), 0, 10)]


def test_planning_concrete_inverted_range_raises_before_planning(consumer_factory):
    consumer = FakeConsumer()
    with pytest.raises(ValueError, match="Inverted offset range"):
        make_reader(
            consumer_factory,
            consumer,
            startingOffsets='{"orders": {"0": 8}}',
            endingOffsets='{"orders": {"0": 3}}',
        )


def test_planning_late_bound_inverted_range_raises_at_task_time(consumer_factory):
    # A sentinel endpoint has no size until the task binds it, so this one
    # can only be caught where Spark catches it.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)})
    reader = make_reader(consumer_factory, consumer, startingOffsets='{"orders": {"0": 8}}')
    partition = reader.partitions()[0]
    assert planned_ranges([partition]) == [(TOPIC, 0, 8, LATEST)]
    with pytest.raises(ValueError, match="Inverted offset range"):
        read_all(reader, partition)


def test_planning_empty_concrete_range_still_plans_a_partition(consumer_factory):
    # Spark plans an input partition for an empty range too, and returns no
    # rows from it; only the splitting path drops empty ranges.
    consumer = FakeConsumer()
    reader = make_reader(
        consumer_factory,
        consumer,
        startingOffsets='{"orders": {"0": 3}}',
        endingOffsets='{"orders": {"0": 3}}',
    )
    partitions = reader.partitions()
    assert planned_ranges(partitions) == [(TOPIC, 0, 3, 3)]
    assert read_all(reader, partitions[0]) == []


def test_planning_concrete_inverted_range_with_splitting_raises_before_planning(consumer_factory):
    with pytest.raises(ValueError, match="Inverted offset range"):
        make_reader(
            consumer_factory,
            FakeConsumer(watermarks={(TOPIC, 0): (0, 3)}),
            startingOffsets='{"orders": {"0": 8}}',
            endingOffsets='{"orders": {"0": 3}}',
            minPartitions="4",
        )


@pytest.mark.parametrize(
    ("watermarks", "starting", "ending", "split_hint"),
    [
        ((0, 3), '{"orders": {"0": 8}}', "latest", {"minPartitions": "4"}),
        ((8, 12), "earliest", '{"orders": {"0": 3}}', {"minPartitions": "4"}),
        ((8, 12), "earliest", '{"orders": {"0": 3}}', {"maxRecordsPerPartition": "2"}),
        ((0, 3), '{"orders": {"0": 3}}', "latest", {"minPartitions": "4"}),
    ],
)
def test_planning_splitting_drops_non_positive_resolved_ranges(
    consumer_factory, watermarks, starting, ending, split_hint
):
    # Verified with Spark 4.1.1's KafkaOffsetReaderAdmin: its split calculator
    # discards these ranges, although the unsplit task would reject an inversion.
    reader = make_reader(
        consumer_factory,
        FakeConsumer(watermarks={(TOPIC, 0): watermarks}),
        startingOffsets=starting,
        endingOffsets=ending,
        **split_hint,
    )
    assert reader.partitions() == []


def test_planning_splitting_keeps_valid_partitions_when_another_range_inverts(consumer_factory):
    consumer = FakeConsumer(
        topics={TOPIC: FakeTopicMeta([0, 1])},
        watermarks={(TOPIC, 0): (0, 3), (TOPIC, 1): (0, 6)},
    )
    reader = make_reader(
        consumer_factory,
        consumer,
        startingOffsets='{"orders": {"0": 8, "1": 0}}',
        endingOffsets="latest",
        minPartitions="4",
    )
    assert planned_ranges(reader.partitions()) == [
        (TOPIC, 1, 0, 1),
        (TOPIC, 1, 1, 2),
        (TOPIC, 1, 2, 4),
        (TOPIC, 1, 4, LATEST),
    ]


@pytest.mark.parametrize(("initial_parts", "updated_parts"), [([0], [0, 1]), ([0, 1], [0])])
def test_planning_partition_set_change_between_endpoints_raises(
    monkeypatch, initial_parts, updated_parts, kafka_client
):
    from pysail.spark.datasource.kafka import KafkaDataSource

    state = FakeConsumer(topics={TOPIC: FakeTopicMeta(initial_parts)})

    class ChangingAdmin(FakeAdminClient):
        def describe_topics(self, topic_collection, request_timeout=None):
            result = super().describe_topics(topic_collection, request_timeout)
            state.topics[TOPIC] = FakeTopicMeta(updated_parts)
            return result

    admin = ChangingAdmin(state)
    monkeypatch.setattr(kafka_client, "AdminClient", lambda _: admin)
    reader = KafkaDataSource(options={"kafka.bootstrap.servers": "unused:9092", "subscribe": TOPIC}).reader(None)
    with pytest.raises(ValueError, match="different topic partitions"):
        reader.partitions()
    assert admin.list_offsets_calls == 0


def test_planning_topic_error_is_relayed(consumer_factory):
    from confluent_kafka import KafkaError

    consumer = FakeConsumer(topics={TOPIC: FakeTopicMeta([0], error=KafkaError(29, "authorization failed"))})
    reader = make_reader(consumer_factory, consumer)
    with pytest.raises(ValueError, match=r"is not available: .*authorization failed"):
        reader.partitions()


def test_planning_json_sentinels_are_late_bound_too(consumer_factory):
    from pysail.spark.datasource.kafka.models import OffsetRange, TopicPartitionKey

    # The JSON -2/-1 form is the same limit as the string form, so it gets
    # the same late binding.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (2, 9)})
    reader = make_reader(
        consumer_factory,
        consumer,
        startingOffsets='{"orders": {"0": -2}}',
        endingOffsets='{"orders": {"0": -1}}',
    )
    partitions = reader.partitions()
    assert planned_ranges(partitions) == [(TOPIC, 0, EARLIEST, LATEST)]
    assert bound_ranges(reader, consumer, partitions) == [OffsetRange(TopicPartitionKey(TOPIC, 0), 2, 9)]


def test_planning_explicit_offsets_are_not_late_bound(consumer_factory):
    # Concrete offsets are used as given and need no broker round-trip,
    # either on the driver or in the task.
    consumer = FakeConsumer()
    reader = make_reader(
        consumer_factory,
        consumer,
        startingOffsets='{"orders": {"0": 2}}',
        endingOffsets='{"orders": {"0": 7}}',
    )
    partitions = reader.partitions()
    assert planned_ranges(partitions) == [(TOPIC, 0, 2, 7)]
    assert consumer.admin.list_offsets_calls == 0


def test_planning_unresolved_admin_future_does_not_hang_planning(consumer_factory, monkeypatch, kafka_client):
    monkeypatch.setattr(kafka_client, "FUTURE_TIMEOUT_MARGIN_S", 0)
    # `Future.result()` with no timeout blocks forever. librdkafka normally
    # completes the future itself via `request_timeout`, but if it does not,
    # planning must still give up rather than wedge the query.
    consumer = FakeConsumer()
    reader = make_reader(consumer_factory, consumer, adminTimeoutMs="1")
    consumer.admin.hang = True
    with pytest.raises(TimeoutError, match=r"Timed out .* waiting for the broker to describe topic"):
        reader.partitions()


def test_planning_does_not_open_a_consumer(consumer_factory):
    # Planning runs entirely on the AdminClient, as Spark's
    # KafkaOffsetReaderAdmin does; only executors open consumers.
    consumer = FakeConsumer()
    reader = make_reader(consumer_factory, consumer)
    reader.partitions()
    assert consumer.assigned is None


def planning_make_wide_consumer():
    return FakeConsumer(
        topics={TOPIC: FakeTopicMeta([0, 1, 2, 3])},
        watermarks={(TOPIC, p): (0, 10) for p in range(4)},
    )


def test_planning_late_bound_planning_makes_no_offset_request(consumer_factory):
    # Nothing to resolve on the driver when both endpoints are sentinels.
    consumer = planning_make_wide_consumer()
    reader = make_reader(consumer_factory, consumer)
    assert len(reader.partitions()) == 4
    assert consumer.admin.list_offsets_calls == 0


def test_planning_defaults_to_read_uncommitted(consumer_factory):
    from confluent_kafka import IsolationLevel

    consumer = planning_make_wide_consumer()
    reader = make_reader(consumer_factory, consumer, minPartitions="8")
    reader.partitions()
    assert set(consumer.admin.isolation_levels) == {IsolationLevel.READ_UNCOMMITTED}


@pytest.mark.parametrize("raw", ["read_committed", "READ_COMMITTED"])
def test_planning_follows_the_configured_isolation_level(consumer_factory, raw):
    from confluent_kafka import IsolationLevel

    from pysail.spark.datasource.kafka import KafkaDataSource

    # Spark builds `ListOffsetsOptions` from the driver Kafka params. Under
    # `read_committed` a "latest" lookup answers with the last stable
    # offset, which is what the reader will be able to see; resolving
    # against the high watermark instead would place split boundaries past
    # it.
    consumer = planning_make_wide_consumer()
    consumer_factory(consumer)
    reader = KafkaDataSource(
        options={
            "kafka.bootstrap.servers": "localhost:9092",
            "subscribe": TOPIC,
            # Spark's planning upper-cases the value, so any letter case plans the same.
            "kafka.isolation.level": raw,
            "minPartitions": "8",
        }
    ).reader(None)
    reader.partitions()
    assert set(consumer.admin.isolation_levels) == {IsolationLevel.READ_COMMITTED}


def test_planning_forced_resolution_is_one_request_per_endpoint(consumer_factory):
    # `minPartitions` forces the sentinels to be resolved. That resolution
    # is batched — one call per endpoint, not one per partition, which is
    # what would turn planning a wide topic into a round-trip each.
    consumer = planning_make_wide_consumer()
    reader = make_reader(consumer_factory, consumer, minPartitions="8")
    assert len(reader.partitions()) == 8
    assert consumer.admin.list_offsets_calls == 2


MULTI_TOPICS = {
    "orders": FakeTopicMeta([0, 1]),
    "orders-eu": FakeTopicMeta([0]),
    "shipments": FakeTopicMeta([0]),
    # Flagged internal by the broker, as `__consumer_offsets` really is.
    "__consumer_offsets": FakeTopicMeta([0], is_internal=True),
    # A *user* topic that merely looks internal. The broker does not flag it,
    # so Spark reads it and so must we — a `__` name-prefix rule would not.
    "__audit": FakeTopicMeta([0]),
}


MULTI_WATERMARKS = {
    ("orders", 0): (0, 5),
    ("orders", 1): (0, 5),
    ("orders-eu", 0): (0, 5),
    ("shipments", 0): (0, 5),
    ("__consumer_offsets", 0): (0, 5),
    ("__audit", 0): (0, 5),
}


def subscription_discovery_make_consumer():
    return FakeConsumer(topics=MULTI_TOPICS, watermarks=MULTI_WATERMARKS)


def test_subscription_discovery_subscribe_pattern_excludes_broker_internal_topics(consumer_factory):
    from pysail.spark.datasource.kafka import KafkaDataSource

    consumer_factory(subscription_discovery_make_consumer())
    reader = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribePattern": ".*"}).reader(
        None
    )
    topics = {topic for topic, _, _, _ in planned_ranges(reader.partitions())}
    assert "__consumer_offsets" not in topics
    # `__audit` is a user topic the broker does not flag internal. Spark's
    # `retrieveAllPartitions` filters on `isInternal`, not on the name, so
    # excluding it by its `__` prefix would hide data Spark returns.
    assert topics == {"orders", "orders-eu", "shipments", "__audit"}


def test_subscription_discovery_subscribe_pattern_matches_unanchored(consumer_factory):
    from pysail.spark.datasource.kafka import KafkaDataSource

    consumer_factory(subscription_discovery_make_consumer())
    reader = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribePattern": "eu"}).reader(
        None
    )
    topics = {topic for topic, _, _, _ in planned_ranges(reader.partitions())}
    assert topics == {"orders-eu"}


def test_subscription_discovery_list_topics_timeout_and_error_are_wrapped(consumer_factory, monkeypatch):
    from confluent_kafka import KafkaError, KafkaException

    from pysail.spark.datasource.kafka import KafkaDataSource

    consumer = subscription_discovery_make_consumer()
    consumer_factory(consumer)
    reader = KafkaDataSource(
        options={"kafka.bootstrap.servers": "localhost:9092", "subscribePattern": "orders", "adminTimeoutMs": "250"}
    ).reader(None)

    def fail_timeout(timeout=None):  # noqa: ARG001
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT))  # noqa: SLF001

    monkeypatch.setattr(consumer.admin, "list_topics", fail_timeout)
    with pytest.raises(TimeoutError, match=r"list topics.*adminTimeoutMs"):
        reader.partitions()

    def fail_broker(timeout=None):  # noqa: ARG001
        raise KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "denied"))

    monkeypatch.setattr(consumer.admin, "list_topics", fail_broker)
    with pytest.raises(ValueError, match=r"Failed to list topics on broker:.*denied"):
        reader.partitions()


def test_subscription_discovery_internal_topics_are_excluded_from_subscribe_too(consumer_factory):
    from pysail.spark.datasource.kafka import KafkaDataSource

    # Spark applies the isInternal filter in `retrieveAllPartitions`, which
    # every strategy goes through — not just the pattern one.
    consumer_factory(subscription_discovery_make_consumer())
    reader = KafkaDataSource(
        options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "__consumer_offsets"}
    ).reader(None)
    assert reader.partitions() == []


def specific_offsets_make_consumer():
    return FakeConsumer(
        topics={TOPIC: FakeTopicMeta([0, 1])},
        watermarks={(TOPIC, 0): (0, 10), (TOPIC, 1): (0, 10)},
    )


def test_specific_offsets_all_partitions_named_ok(consumer_factory):
    reader = make_reader(
        consumer_factory, specific_offsets_make_consumer(), startingOffsets='{"orders": {"0": 1, "1": 2}}'
    )
    assert planned_ranges(reader.partitions()) == [(TOPIC, 0, 1, LATEST), (TOPIC, 1, 2, LATEST)]


def test_specific_offsets_missing_partition_raises(consumer_factory):
    reader = make_reader(consumer_factory, specific_offsets_make_consumer(), startingOffsets='{"orders": {"0": 1}}')
    with pytest.raises(ValueError, match="must specify all assigned TopicPartitions"):
        reader.partitions()


def test_specific_offsets_extra_partition_raises(consumer_factory):
    reader = make_reader(
        consumer_factory, specific_offsets_make_consumer(), endingOffsets='{"orders": {"0": 1, "1": 2, "9": 3}}'
    )
    with pytest.raises(ValueError, match="not assigned"):
        reader.partitions()


def test_specific_offsets_timestamp_map_is_validated_too(consumer_factory):
    reader = make_reader(
        consumer_factory, specific_offsets_make_consumer(), startingOffsetsByTimestamp='{"orders": {"0": 100}}'
    )
    with pytest.raises(ValueError, match="must specify all assigned TopicPartitions"):
        reader.partitions()


def test_timestamp_lookup_match_resolves_to_offset(consumer_factory):
    consumer = FakeConsumer(times={(TOPIC, 0, 500): (4, None)})
    reader = make_reader(consumer_factory, consumer, startingTimestamp="500")
    assert planned_ranges(reader.partitions()) == [(TOPIC, 0, 4, LATEST)]


def test_timestamp_lookup_no_match_on_start_raises_by_default(consumer_factory):
    # Spark's startingOffsetsByTimestampStrategy defaults to `error`.
    consumer = FakeConsumer(times={})
    reader = make_reader(consumer_factory, consumer, startingTimestamp="99999")
    with pytest.raises(ValueError, match="No offset matches the requested timestamp"):
        reader.partitions()


def test_timestamp_lookup_no_match_on_start_with_latest_strategy(consumer_factory):
    from pysail.spark.datasource.kafka.models import OffsetRange, TopicPartitionKey

    consumer = FakeConsumer(times={})
    reader = make_reader(
        consumer_factory,
        consumer,
        startingTimestamp="99999",
        startingOffsetsByTimestampStrategy="latest",
    )
    # Start collapses onto the high watermark. The end is still late-bound,
    # so the range only turns out to be empty once the task binds it.
    partitions = reader.partitions()
    assert planned_ranges(partitions) == [(TOPIC, 0, 10, LATEST)]
    assert bound_ranges(reader, consumer, partitions) == [OffsetRange(TopicPartitionKey(TOPIC, 0), 10, 10)]
    assert read_all(reader, partitions[0]) == []


def test_timestamp_lookup_no_match_on_end_falls_back_to_latest(consumer_factory):
    # Ending timestamps always fall back, no strategy option involved.
    consumer = FakeConsumer(times={})
    reader = make_reader(consumer_factory, consumer, endingTimestamp="99999")
    assert planned_ranges(reader.partitions()) == [(TOPIC, 0, EARLIEST, 10)]


def test_timestamp_lookup_error_is_surfaced(consumer_factory):
    from confluent_kafka import KafkaError

    # A per-partition error carries an invalid offset; treating it as "no
    # match" would turn a broker failure into a silently empty read.
    consumer = FakeConsumer(times={(TOPIC, 0, 500): (-1001, KafkaError(7, "leader not available"))})
    reader = make_reader(consumer_factory, consumer, startingTimestamp="500")
    with pytest.raises(ValueError, match="Failed to look up offsets for"):
        reader.partitions()


def test_planning_splits_min_partitions_splits(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 100)})
    reader = make_reader(consumer_factory, consumer, minPartitions="4")
    assert planned_ranges(reader.partitions()) == [
        (TOPIC, 0, EARLIEST, 25),
        (TOPIC, 0, 25, 50),
        (TOPIC, 0, 50, 75),
        (TOPIC, 0, 75, LATEST),
    ]


def test_planning_splits_max_records_splits(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 10)})
    reader = make_reader(
        consumer_factory,
        consumer,
        maxRecordsPerPartition="4",
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":10}}',
    )
    # Spark's chunking puts the remainder in the last chunk: 3/3/4, not 4/3/3.
    assert planned_ranges(reader.partitions()) == [(TOPIC, 0, 0, 3), (TOPIC, 0, 3, 6), (TOPIC, 0, 6, 10)]


def test_planning_splits_max_records_does_not_force_default_sentinels_to_resolve(consumer_factory):
    consumer = FakeConsumer()
    reader = make_reader(consumer_factory, consumer, maxRecordsPerPartition="4")
    assert planned_ranges(reader.partitions()) == [(TOPIC, 0, EARLIEST, LATEST)]
    assert consumer.admin.list_offsets_calls == 0


def test_planning_splits_partition_ids_stay_sequential(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 100)})
    reader = make_reader(consumer_factory, consumer, minPartitions="4")
    assert [p.value for p in reader.partitions()] == [0, 1, 2, 3]
