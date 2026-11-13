"""Unit tests for Kafka planning and read logic, against fake broker clients.

The integration tests in ``test_kafka.py`` need Docker and a broker, so they
only run in the integration job. These cover the same Spark-parity rules —
offset resolution, the exact partition-set check, timestamp strategies, and
data-loss detection — with a stand-in consumer, so a regression surfaces in the
ordinary test run.
"""

from __future__ import annotations

import pytest

from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 1):
    pytest.skip("Python data source requires Spark 4.1+", allow_module_level=True)

pytest.importorskip("confluent_kafka")

from concurrent.futures import TimeoutError as FuturesTimeoutError

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin._listoffsets import EarliestSpec, TimestampSpec

from pysail.spark.datasource import kafka as kafka_module
from pysail.spark.datasource.kafka import KafkaDataSource

_TOPIC = "orders"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeError:
    def __init__(self, code, message="fake error"):
        self._code = code
        self._message = message

    def code(self):
        return self._code

    def __str__(self):
        return self._message


class _FakeFuture:
    """Stand-in for the ``concurrent.futures.Future`` the admin calls return.

    ``never_resolves`` models a broker that leaves the future pending, which is
    what a bare ``Future.result()`` would wait on indefinitely.
    """

    def __init__(self, result=None, exception=None, *, never_resolves=False):
        self._result = result
        self._exception = exception
        self._never_resolves = never_resolves

    def result(self, timeout=None):  # noqa: ARG002
        if self._never_resolves:
            raise FuturesTimeoutError
        if self._exception is not None:
            raise self._exception
        return self._result


class _FakePartitionInfo:
    def __init__(self, partition_id):
        self.id = partition_id


class _FakeTopicDescription:
    """Stand-in for ``TopicDescription``, the result of ``describe_topics``."""

    def __init__(self, partitions, *, is_internal=False):
        self.partitions = [_FakePartitionInfo(p) for p in partitions]
        self.is_internal = is_internal


class _FakeListOffsetsResult:
    def __init__(self, offset):
        self.offset = offset


class _FakeTopicMeta:
    def __init__(self, partitions, error=None, *, is_internal=False):
        self.partitions = dict.fromkeys(partitions)
        self.error = error
        self.is_internal = is_internal


class _FakeMetadata:
    def __init__(self, topics):
        self.topics = topics


class _FakeMessage:
    """Stand-in for confluent_kafka.Message."""

    def __init__(self, offset, *, topic=_TOPIC, partition=0, ts=(1, 1700000000000), error=None):
        self._offset = offset
        self._topic = topic
        self._partition = partition
        self._ts = ts
        self._error = error

    def error(self):
        return self._error

    def offset(self):
        return self._offset

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def key(self):
        return f"k{self._offset}".encode()

    def value(self):
        return f"v{self._offset}".encode()

    def timestamp(self):
        return self._ts

    def headers(self):
        return None


def _eof_message():
    return _FakeMessage(-1, error=_FakeError(KafkaError._PARTITION_EOF))  # noqa: SLF001


class _FakeConsumer:
    """Broker state, plus the slice of the Consumer API that ``read()`` uses.

    Planning goes through :class:`_FakeAdminClient`, which reads the same state,
    so a single instance describes one broker to both clients.
    """

    def __init__(
        self,
        *,
        topics=None,
        watermarks=None,
        times=None,
        messages=None,
        watermarks_after_assign=None,
    ):
        # `is None` rather than a falsy check: an empty topic map is a valid
        # broker state (it is how the unknown-topic case is set up).
        self.topics = {_TOPIC: _FakeTopicMeta([0])} if topics is None else topics
        self.watermarks = {(_TOPIC, 0): (0, 10)} if watermarks is None else watermarks
        self.times = {} if times is None else times
        self.messages = list(messages or [])
        # Watermarks observed once reading has started, for truncation cases.
        self.watermarks_after_assign = watermarks_after_assign
        self.assigned = None
        self.closed = False

    # -- reading -------------------------------------------------------
    def get_watermark_offsets(self, tp, timeout=None, *, cached=False):  # noqa: ARG002
        source = (
            self.watermarks_after_assign
            if self.assigned is not None and self.watermarks_after_assign is not None
            else self.watermarks
        )
        return source[(tp.topic, tp.partition)]

    def assign(self, partitions):
        self.assigned = partitions

    def poll(self, timeout=None):  # noqa: ARG002
        return self.messages.pop(0) if self.messages else None

    def close(self):
        self.closed = True


class _FakeAdminClient:
    """Implements the slice of the AdminClient API that planning uses.

    Mirrors the real contract closely: results arrive as futures, per-topic
    failures surface when the future is resolved, and `list_offsets` answers
    every partition in a single call.
    """

    def __init__(self, state):
        self.state = state
        self.list_offsets_calls = 0
        self.hang = False

    def list_topics(self, timeout=None):  # noqa: ARG002
        return _FakeMetadata(self.state.topics)

    def describe_topics(self, topic_collection, request_timeout=None):  # noqa: ARG002
        out = {}
        for name in topic_collection.topic_names:
            if self.hang:
                out[name] = _FakeFuture(never_resolves=True)
                continue
            meta = self.state.topics.get(name)
            if meta is None:
                error = _FakeError(KafkaError.UNKNOWN_TOPIC_OR_PART, "unknown topic")
                out[name] = _FakeFuture(exception=KafkaException(error))
            elif meta.error is not None:
                out[name] = _FakeFuture(exception=KafkaException(meta.error))
            else:
                out[name] = _FakeFuture(_FakeTopicDescription(sorted(meta.partitions), is_internal=meta.is_internal))
        return out

    def list_offsets(self, specs, isolation_level=None, request_timeout=None):  # noqa: ARG002
        self.list_offsets_calls += 1
        out = {}
        for tp, spec in specs.items():
            low, high = self.state.watermarks[(tp.topic, tp.partition)]
            if isinstance(spec, TimestampSpec):
                offset, error = self.state.times.get((tp.topic, tp.partition, spec.timestamp), (-1, None))
                if error is not None:
                    out[tp] = _FakeFuture(exception=KafkaException(error))
                    continue
            elif isinstance(spec, EarliestSpec):
                offset = low
            else:
                offset = high
            out[tp] = _FakeFuture(_FakeListOffsetsResult(offset))
        return out


@pytest.fixture
def consumer_factory(monkeypatch):
    """Install a fake Consumer and AdminClient backed by one broker state."""
    holder = {}

    def _install(consumer):
        holder["consumer"] = consumer
        holder["admin"] = _FakeAdminClient(consumer)
        monkeypatch.setattr(kafka_module, "Consumer", lambda _config: holder["consumer"])
        monkeypatch.setattr(kafka_module, "AdminClient", lambda _config: holder["admin"])
        consumer.admin = holder["admin"]
        return consumer

    return _install


def _reader(consumer_factory, consumer, **options):
    consumer_factory(consumer)
    opts = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": _TOPIC, **options}
    return KafkaDataSource(options=opts).reader(None)


def _ranges(partitions):
    """Ranges as planned, which may still carry the -2/-1 late-binding sentinels."""
    return [(p.topic, p.kafka_partition, p.start_offset, p.end_offset) for p in partitions]


def _bound_ranges(consumer, partitions):
    """Ranges as each task will see them, after binding sentinels at task start."""
    bind = kafka_module._bind_partition_range  # noqa: SLF001
    return [(p.topic, p.kafka_partition, *bind(consumer, p)) for p in partitions]


# Spark's late-binding sentinels, carried from planning into the task.
_EARLIEST = kafka_module._SPARK_OFFSET_EARLIEST  # noqa: SLF001
_LATEST = kafka_module._SPARK_OFFSET_LATEST  # noqa: SLF001


# ---------------------------------------------------------------------------
# Partition planning
# ---------------------------------------------------------------------------


class TestPlanning:
    def test_default_range_is_carried_as_sentinels(self, consumer_factory):
        # Spark's `fetchPartitionOffsets` returns -2/-1 for earliest/latest and
        # binds them per task, so planning must not freeze them here.
        consumer = _FakeConsumer()
        reader = _reader(consumer_factory, consumer)
        partitions = reader.partitions()
        assert _ranges(partitions) == [(_TOPIC, 0, _EARLIEST, _LATEST)]
        assert _bound_ranges(consumer, partitions) == [(_TOPIC, 0, 0, 10)]

    def test_late_binding_picks_up_records_produced_after_planning(self, consumer_factory):
        # The whole point of late binding: `latest` means the end of the log when
        # the task runs, not when the query was planned. Freezing it on the
        # driver would silently drop this tail.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer)
        partitions = reader.partitions()
        consumer.watermarks = {(_TOPIC, 0): (0, 7)}  # four more records arrive
        assert _bound_ranges(consumer, partitions) == [(_TOPIC, 0, 0, 7)]

    def test_late_binding_survives_retention_after_planning(self, consumer_factory):
        # An `earliest` start frozen at planning can age out before the task
        # runs, failing a query Spark completes by simply re-resolving.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 10)})
        reader = _reader(consumer_factory, consumer)
        partitions = reader.partitions()
        consumer.watermarks = {(_TOPIC, 0): (6, 10)}  # offsets 0-5 aged out
        assert _bound_ranges(consumer, partitions) == [(_TOPIC, 0, 6, 10)]

    def test_empty_range_yields_no_rows(self, consumer_factory):
        # A late-bound range has no size until it is read, so the empty case is
        # detected per task, as `KafkaSourceRDD.compute` does.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (4, 4)})
        reader = _reader(consumer_factory, consumer)
        partition = reader.partitions()[0]
        assert _bound_ranges(consumer, [partition]) == [(_TOPIC, 0, 4, 4)]
        assert _read_all(reader, partition) == []

    def test_inverted_range_raises_at_task_time(self, consumer_factory):
        # Both endpoints are concrete, but with no splitting to force an early
        # resolve the check lands in the task, where Spark's
        # `KafkaSourceRDD.compute` asserts `fromOffset <= untilOffset`.
        consumer = _FakeConsumer()
        reader = _reader(
            consumer_factory,
            consumer,
            startingOffsets='{"orders": {"0": 8}}',
            endingOffsets='{"orders": {"0": 3}}',
        )
        partition = reader.partitions()[0]
        with pytest.raises(ValueError, match="Inverted offset range"):
            _read_all(reader, partition)

    def test_inverted_range_raises_during_planning_when_splitting(self, consumer_factory):
        # `minPartitions` forces an early resolve, so the same range is caught
        # on the driver instead.
        reader = _reader(
            consumer_factory,
            _FakeConsumer(),
            startingOffsets='{"orders": {"0": 8}}',
            endingOffsets='{"orders": {"0": 3}}',
            minPartitions="4",
        )
        with pytest.raises(ValueError, match="Inverted offset range"):
            reader.partitions()

    def test_unknown_topic_raises(self, consumer_factory):
        consumer = _FakeConsumer(topics={})
        reader = _reader(consumer_factory, consumer)
        with pytest.raises(ValueError, match="not found on broker"):
            reader.partitions()

    def test_topic_error_is_relayed(self, consumer_factory):
        consumer = _FakeConsumer(topics={_TOPIC: _FakeTopicMeta([0], error=_FakeError(29, "authorization failed"))})
        reader = _reader(consumer_factory, consumer)
        with pytest.raises(ValueError, match="is not available: authorization failed"):
            reader.partitions()

    def test_json_sentinels_are_late_bound_too(self, consumer_factory):
        # The JSON -2/-1 form is the same limit as the string form, so it gets
        # the same late binding.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (2, 9)})
        reader = _reader(
            consumer_factory,
            consumer,
            startingOffsets='{"orders": {"0": -2}}',
            endingOffsets='{"orders": {"0": -1}}',
        )
        partitions = reader.partitions()
        assert _ranges(partitions) == [(_TOPIC, 0, _EARLIEST, _LATEST)]
        assert _bound_ranges(consumer, partitions) == [(_TOPIC, 0, 2, 9)]

    def test_explicit_offsets_are_not_late_bound(self, consumer_factory):
        # Concrete offsets are used as given and need no broker round-trip,
        # either on the driver or in the task.
        consumer = _FakeConsumer()
        reader = _reader(
            consumer_factory,
            consumer,
            startingOffsets='{"orders": {"0": 2}}',
            endingOffsets='{"orders": {"0": 7}}',
        )
        partitions = reader.partitions()
        assert _ranges(partitions) == [(_TOPIC, 0, 2, 7)]
        assert consumer.admin.list_offsets_calls == 0

    def test_unresolved_admin_future_does_not_hang_planning(self, consumer_factory):
        # `Future.result()` with no timeout blocks forever. librdkafka normally
        # completes the future itself via `request_timeout`, but if it does not,
        # planning must still give up rather than wedge the query.
        consumer = _FakeConsumer()
        reader = _reader(consumer_factory, consumer, adminTimeoutMs="1")
        consumer.admin.hang = True
        with pytest.raises(TimeoutError, match=r"Timed out .* waiting for the broker to describe topic"):
            reader.partitions()

    def test_planning_does_not_open_a_consumer(self, consumer_factory):
        # Planning runs entirely on the AdminClient, as Spark's
        # KafkaOffsetReaderAdmin does; only executors open consumers.
        consumer = _FakeConsumer()
        reader = _reader(consumer_factory, consumer)
        reader.partitions()
        assert consumer.assigned is None

    def _wide_consumer(self):
        return _FakeConsumer(
            topics={_TOPIC: _FakeTopicMeta([0, 1, 2, 3])},
            watermarks={(_TOPIC, p): (0, 10) for p in range(4)},
        )

    def test_late_bound_planning_makes_no_offset_request(self, consumer_factory):
        # Nothing to resolve on the driver when both endpoints are sentinels.
        consumer = self._wide_consumer()
        reader = _reader(consumer_factory, consumer)
        assert len(reader.partitions()) == 4  # noqa: PLR2004
        assert consumer.admin.list_offsets_calls == 0

    def test_forced_resolution_is_one_request_per_endpoint(self, consumer_factory):
        # `minPartitions` forces the sentinels to be resolved. That resolution
        # is batched — one call per endpoint, not one per partition, which is
        # what would turn planning a wide topic into a round-trip each.
        consumer = self._wide_consumer()
        reader = _reader(consumer_factory, consumer, minPartitions="8")
        assert len(reader.partitions()) == 8  # noqa: PLR2004
        assert consumer.admin.list_offsets_calls == 2  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Subscription strategies
# ---------------------------------------------------------------------------


_MULTI_TOPICS = {
    "orders": _FakeTopicMeta([0, 1]),
    "orders-eu": _FakeTopicMeta([0]),
    "shipments": _FakeTopicMeta([0]),
    # Flagged internal by the broker, as `__consumer_offsets` really is.
    "__consumer_offsets": _FakeTopicMeta([0], is_internal=True),
    # A *user* topic that merely looks internal. The broker does not flag it,
    # so Spark reads it and so must we — a `__` name-prefix rule would not.
    "__audit": _FakeTopicMeta([0]),
}
_MULTI_WATERMARKS = {
    ("orders", 0): (0, 5),
    ("orders", 1): (0, 5),
    ("orders-eu", 0): (0, 5),
    ("shipments", 0): (0, 5),
    ("__consumer_offsets", 0): (0, 5),
    ("__audit", 0): (0, 5),
}


class TestSubscriptionStrategies:
    def _consumer(self):
        return _FakeConsumer(topics=_MULTI_TOPICS, watermarks=_MULTI_WATERMARKS)

    def test_subscribe_enumerates_all_partitions(self, consumer_factory):
        reader = _reader(consumer_factory, self._consumer(), subscribe="orders")
        assert _ranges(reader.partitions()) == [("orders", 0, _EARLIEST, _LATEST), ("orders", 1, _EARLIEST, _LATEST)]

    def test_assign_selects_named_partitions_only(self, consumer_factory):
        consumer_factory(self._consumer())
        reader = KafkaDataSource(
            options={"kafka.bootstrap.servers": "localhost:9092", "assign": '{"orders": [1]}'}
        ).reader(None)
        assert _ranges(reader.partitions()) == [("orders", 1, _EARLIEST, _LATEST)]

    def test_assign_unknown_partition_raises(self, consumer_factory):
        consumer_factory(self._consumer())
        reader = KafkaDataSource(
            options={"kafka.bootstrap.servers": "localhost:9092", "assign": '{"orders": [4]}'}
        ).reader(None)
        with pytest.raises(ValueError, match="partitions that do not exist"):
            reader.partitions()

    def test_subscribe_pattern_matches_whole_name(self, consumer_factory):
        consumer_factory(self._consumer())
        reader = KafkaDataSource(
            options={"kafka.bootstrap.servers": "localhost:9092", "subscribePattern": "orders"}
        ).reader(None)
        # `orders-eu` must not match: Spark anchors the pattern to the whole name.
        assert _ranges(reader.partitions()) == [("orders", 0, _EARLIEST, _LATEST), ("orders", 1, _EARLIEST, _LATEST)]

    def test_subscribe_pattern_wildcard(self, consumer_factory):
        consumer_factory(self._consumer())
        reader = KafkaDataSource(
            options={"kafka.bootstrap.servers": "localhost:9092", "subscribePattern": "orders.*"}
        ).reader(None)
        assert {topic for topic, _, _, _ in _ranges(reader.partitions())} == {"orders", "orders-eu"}

    def test_subscribe_pattern_excludes_broker_internal_topics(self, consumer_factory):
        consumer_factory(self._consumer())
        reader = KafkaDataSource(
            options={"kafka.bootstrap.servers": "localhost:9092", "subscribePattern": ".*"}
        ).reader(None)
        topics = {topic for topic, _, _, _ in _ranges(reader.partitions())}
        assert "__consumer_offsets" not in topics
        # `__audit` is a user topic the broker does not flag internal. Spark's
        # `retrieveAllPartitions` filters on `isInternal`, not on the name, so
        # excluding it by its `__` prefix would hide data Spark returns.
        assert topics == {"orders", "orders-eu", "shipments", "__audit"}

    def test_internal_topics_are_excluded_from_subscribe_too(self, consumer_factory):
        # Spark applies the isInternal filter in `retrieveAllPartitions`, which
        # every strategy goes through — not just the pattern one.
        consumer_factory(self._consumer())
        reader = KafkaDataSource(
            options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "__consumer_offsets"}
        ).reader(None)
        assert reader.partitions() == []


# ---------------------------------------------------------------------------
# Specific offsets must name every assigned partition (Spark's assertion)
# ---------------------------------------------------------------------------


class TestSpecificOffsetValidation:
    def _consumer(self):
        return _FakeConsumer(
            topics={_TOPIC: _FakeTopicMeta([0, 1])},
            watermarks={(_TOPIC, 0): (0, 10), (_TOPIC, 1): (0, 10)},
        )

    def test_all_partitions_named_ok(self, consumer_factory):
        reader = _reader(consumer_factory, self._consumer(), startingOffsets='{"orders": {"0": 1, "1": 2}}')
        assert _ranges(reader.partitions()) == [(_TOPIC, 0, 1, _LATEST), (_TOPIC, 1, 2, _LATEST)]

    def test_missing_partition_raises(self, consumer_factory):
        reader = _reader(consumer_factory, self._consumer(), startingOffsets='{"orders": {"0": 1}}')
        with pytest.raises(ValueError, match="must specify all assigned TopicPartitions"):
            reader.partitions()

    def test_extra_partition_raises(self, consumer_factory):
        reader = _reader(consumer_factory, self._consumer(), endingOffsets='{"orders": {"0": 1, "1": 2, "9": 3}}')
        with pytest.raises(ValueError, match="not assigned"):
            reader.partitions()

    def test_timestamp_map_is_validated_too(self, consumer_factory):
        reader = _reader(consumer_factory, self._consumer(), startingOffsetsByTimestamp='{"orders": {"0": 100}}')
        with pytest.raises(ValueError, match="must specify all assigned TopicPartitions"):
            reader.partitions()


# ---------------------------------------------------------------------------
# Timestamp resolution
# ---------------------------------------------------------------------------


class TestTimestampResolution:
    def test_match_resolves_to_offset(self, consumer_factory):
        consumer = _FakeConsumer(times={(_TOPIC, 0, 500): (4, None)})
        reader = _reader(consumer_factory, consumer, startingTimestamp="500")
        assert _ranges(reader.partitions()) == [(_TOPIC, 0, 4, _LATEST)]

    def test_no_match_on_start_raises_by_default(self, consumer_factory):
        # Spark's startingOffsetsByTimestampStrategy defaults to `error`.
        consumer = _FakeConsumer(times={})
        reader = _reader(consumer_factory, consumer, startingTimestamp="99999")
        with pytest.raises(ValueError, match="No offset matches the requested timestamp"):
            reader.partitions()

    def test_no_match_on_start_with_latest_strategy(self, consumer_factory):
        consumer = _FakeConsumer(times={})
        reader = _reader(
            consumer_factory,
            consumer,
            startingTimestamp="99999",
            startingOffsetsByTimestampStrategy="latest",
        )
        # Start collapses onto the high watermark. The end is still late-bound,
        # so the range only turns out to be empty once the task binds it.
        partitions = reader.partitions()
        assert _ranges(partitions) == [(_TOPIC, 0, 10, _LATEST)]
        assert _bound_ranges(consumer, partitions) == [(_TOPIC, 0, 10, 10)]
        assert _read_all(reader, partitions[0]) == []

    def test_no_match_on_end_falls_back_to_latest(self, consumer_factory):
        # Ending timestamps always fall back, no strategy option involved.
        consumer = _FakeConsumer(times={})
        reader = _reader(consumer_factory, consumer, endingTimestamp="99999")
        assert _ranges(reader.partitions()) == [(_TOPIC, 0, _EARLIEST, 10)]

    def test_lookup_error_is_surfaced(self, consumer_factory):
        # A per-partition error carries an invalid offset; treating it as "no
        # match" would turn a broker failure into a silently empty read.
        consumer = _FakeConsumer(times={(_TOPIC, 0, 500): (-1001, _FakeError(7, "leader not available"))})
        reader = _reader(consumer_factory, consumer, startingTimestamp="500")
        with pytest.raises(ValueError, match="Failed to look up offsets for"):
            reader.partitions()


# ---------------------------------------------------------------------------
# minPartitions / maxRecordsPerPartition through planning
# ---------------------------------------------------------------------------


class TestPlanningSplits:
    def test_min_partitions_splits(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 100)})
        reader = _reader(consumer_factory, consumer, minPartitions="4")
        assert _ranges(reader.partitions()) == [
            (_TOPIC, 0, _EARLIEST, 25),
            (_TOPIC, 0, 25, 50),
            (_TOPIC, 0, 50, 75),
            (_TOPIC, 0, 75, _LATEST),
        ]

    def test_max_records_splits(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 10)})
        reader = _reader(consumer_factory, consumer, maxRecordsPerPartition="4")
        # Spark's chunking puts the remainder in the last chunk: 3/3/4, not 4/3/3.
        assert _ranges(reader.partitions()) == [(_TOPIC, 0, _EARLIEST, 3), (_TOPIC, 0, 3, 6), (_TOPIC, 0, 6, _LATEST)]

    def test_partition_ids_stay_sequential(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 100)})
        reader = _reader(consumer_factory, consumer, minPartitions="4")
        assert [p.value for p in reader.partitions()] == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# read() — data loss and row conversion
# ---------------------------------------------------------------------------


def _read_all(reader, partition):
    rows = []
    for batch in reader.read(partition):
        rows.extend(batch.to_pylist())
    return rows


class TestRead:
    def _partition(self, reader, consumer, messages, *, watermarks_after=None):
        consumer.messages = messages
        consumer.watermarks_after_assign = watermarks_after
        return reader.partitions()[0]

    def test_reads_the_planned_range(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_FakeMessage(i) for i in range(3)])
        rows = _read_all(reader, partition)
        assert [r["offset"] for r in rows] == [0, 1, 2]
        assert [r["value"] for r in rows] == [b"v0", b"v1", b"v2"]

    def test_eof_within_available_range_stops_cleanly(self, consumer_factory):
        # Compacted or aborted-transaction offsets are unreadable but not lost:
        # the high watermark still covers the planned end, so Spark skips them.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_FakeMessage(0), _eof_message()])
        rows = _read_all(reader, partition)
        assert [r["offset"] for r in rows] == [0]

    def test_eof_below_shrunken_watermark_is_data_loss(self, consumer_factory):
        # The log no longer reaches the planned end: records that existed at
        # planning time are gone, which Spark reports as data loss.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(
            reader,
            consumer,
            [_FakeMessage(0), _eof_message()],
            watermarks_after={(_TOPIC, 0): (0, 1)},
        )
        with pytest.raises(ValueError, match="Data loss"):
            _read_all(reader, partition)

    def test_fail_on_data_loss_false_returns_partial(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer, failOnDataLoss="false")
        partition = self._partition(
            reader,
            consumer,
            [_FakeMessage(0), _eof_message()],
            watermarks_after={(_TOPIC, 0): (0, 1)},
        )
        assert [r["offset"] for r in _read_all(reader, partition)] == [0]

    def test_records_below_the_planned_start_are_dropped(self, consumer_factory):
        # Under failOnDataLoss=false the consumer runs with
        # `auto.offset.reset=earliest`, which librdkafka applies to an
        # out-of-range start in both directions: a start past the end of the log
        # rewinds to the beginning and delivers the whole partition. Spark reads
        # nothing there, so anything below the planned start must be dropped
        # rather than emitted.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(
            consumer_factory,
            consumer,
            failOnDataLoss="false",
            startingOffsets=f'{{"{_TOPIC}": {{"0": 100}}}}',
            endingOffsets=f'{{"{_TOPIC}": {{"0": 110}}}}',
        )
        partition = self._partition(
            reader,
            consumer,
            [_FakeMessage(0), _FakeMessage(1), _FakeMessage(2), _eof_message()],
        )
        assert _read_all(reader, partition) == []

    def test_stops_at_the_exclusive_end(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 2)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_FakeMessage(i) for i in range(4)])
        assert [r["offset"] for r in _read_all(reader, partition)] == [0, 1]

    def test_batches_are_capped_at_max_batch_rows(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer, maxBatchRows="2")
        partition = self._partition(reader, consumer, [_FakeMessage(i) for i in range(5)])
        assert [batch.num_rows for batch in reader.read(partition)] == [2, 2, 1]

    def test_stall_guard_fires(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer, stallTimeoutMs="1", pollTimeoutMs="1")
        partition = self._partition(reader, consumer, [])  # poll() always returns None
        with pytest.raises(TimeoutError, match="Kafka read stalled"):
            _read_all(reader, partition)

    def test_timestamp_type_is_translated_to_spark_values(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 2)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(
            reader,
            consumer,
            [
                _FakeMessage(0, ts=(1, 1700000000000)),  # librdkafka CREATE_TIME
                _FakeMessage(1, ts=(2, 1700000000000)),  # librdkafka LOG_APPEND_TIME
            ],
        )
        assert [r["timestampType"] for r in _read_all(reader, partition)] == [0, 1]

    def test_missing_timestamp_keeps_sparks_minus_one_ms(self, consumer_factory):
        # Spark passes Kafka's -1ms through millisToMicros rather than emitting
        # null, surfacing 1969-12-31 23:59:59.999.
        import datetime

        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 1)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_FakeMessage(0, ts=(0, -1))])
        row = _read_all(reader, partition)[0]
        assert row["timestampType"] == -1
        assert row["timestamp"] == datetime.datetime(1969, 12, 31, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc)

    def test_reader_consumer_is_closed(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 1)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_FakeMessage(0)])
        _read_all(reader, partition)
        assert consumer.closed
