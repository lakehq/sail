"""Unit tests for Kafka planning and read logic, against fake broker clients.

The integration tests in ``test_kafka.py`` need Docker and a broker, so they
only run in the integration job. These cover the same Spark-parity rules —
offset resolution, the exact partition-set check, timestamp strategies, and
data-loss detection — with a stand-in consumer, so a regression surfaces in the
ordinary test run.
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest
from pyspark import cloudpickle

import pysail
from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 0):
    pytest.skip("Python data source requires Spark 4.0+", allow_module_level=True)

pytest.importorskip("confluent_kafka")

from concurrent.futures import TimeoutError as FuturesTimeoutError

from confluent_kafka import IsolationLevel, KafkaError, KafkaException
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
    """An EOF event with no usable offset, exercising the fallback path."""
    return _FakeMessage(-1, error=_FakeError(KafkaError._PARTITION_EOF))  # noqa: SLF001


def _eof_message_at(offset):
    """An EOF event as librdkafka reports one, carrying the position it reached."""
    return _FakeMessage(offset, error=_FakeError(KafkaError._PARTITION_EOF))  # noqa: SLF001


def _offset_out_of_range_message():
    """The error `auto.offset.reset=error` produces for an unserviceable offset."""
    return _FakeMessage(-1, error=_FakeError(KafkaError._AUTO_OFFSET_RESET))  # noqa: SLF001


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
        self.seeks = []
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

    def seek(self, tp):
        self.seeks.append(tp)

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
        self.isolation_levels = []
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
        self.isolation_levels.append(isolation_level)
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
    def test_partition_repr_omits_client_credentials(self, consumer_factory):
        reader = _reader(
            consumer_factory,
            _FakeConsumer(),
            **{"kafka.sasl.password": "test-only-secret", "kafka.ssl.key.password": "test-only-key"},
        )
        partition = reader.partitions()[0]
        assert "test-only-secret" not in repr(partition)
        assert "test-only-key" not in repr(partition)
        assert _TOPIC in repr(partition)
        # Credentials still have to reach the worker; repr redaction must not
        # alter the connection options carried by the task.
        assert partition.client_config["sasl.password"] == "test-only-secret"

    def test_default_range_is_carried_as_sentinels(self, consumer_factory):
        # Spark's `fetchPartitionOffsets` returns -2/-1 for earliest/latest and
        # binds them per task, so planning must not freeze them here.
        consumer = _FakeConsumer()
        reader = _reader(consumer_factory, consumer)
        partitions = reader.partitions()
        assert _ranges(partitions) == [(_TOPIC, 0, _EARLIEST, _LATEST)]
        assert _bound_ranges(consumer, partitions) == [(_TOPIC, 0, 0, 10)]

    def test_concrete_inverted_range_raises_during_planning(self, consumer_factory):
        # Spark asserts `fromOffset <= untilOffset` in `KafkaSourceRDD.compute`,
        # per task. With both endpoints concrete the verdict cannot change
        # between the driver and the task, so the query fails either way and
        # failing here names the partition before any task is launched.
        consumer = _FakeConsumer()
        reader = _reader(
            consumer_factory,
            consumer,
            startingOffsets='{"orders": {"0": 8}}',
            endingOffsets='{"orders": {"0": 3}}',
        )
        with pytest.raises(ValueError, match="Inverted offset range"):
            reader.partitions()

    def test_late_bound_inverted_range_raises_at_task_time(self, consumer_factory):
        # A sentinel endpoint has no size until the task binds it, so this one
        # can only be caught where Spark catches it.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer, startingOffsets='{"orders": {"0": 8}}')
        partition = reader.partitions()[0]
        assert _ranges([partition]) == [(_TOPIC, 0, 8, _LATEST)]
        with pytest.raises(ValueError, match="Inverted offset range"):
            _read_all(reader, partition)

    def test_empty_concrete_range_still_plans_a_partition(self, consumer_factory):
        # Spark plans an input partition for an empty range too, and returns no
        # rows from it; only the splitting path drops empty ranges.
        consumer = _FakeConsumer()
        reader = _reader(
            consumer_factory,
            consumer,
            startingOffsets='{"orders": {"0": 3}}',
            endingOffsets='{"orders": {"0": 3}}',
        )
        partitions = reader.partitions()
        assert _ranges(partitions) == [(_TOPIC, 0, 3, 3)]
        assert _read_all(reader, partitions[0]) == []

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

    def test_planning_defaults_to_read_uncommitted(self, consumer_factory):
        consumer = self._wide_consumer()
        reader = _reader(consumer_factory, consumer, minPartitions="8")
        reader.partitions()
        assert set(consumer.admin.isolation_levels) == {IsolationLevel.READ_UNCOMMITTED}

    def test_planning_follows_the_configured_isolation_level(self, consumer_factory):
        # Spark builds `ListOffsetsOptions` from the driver Kafka params. Under
        # `read_committed` a "latest" lookup answers with the last stable
        # offset, which is what the reader will be able to see; resolving
        # against the high watermark instead would place split boundaries past
        # it.
        consumer = self._wide_consumer()
        consumer_factory(consumer)
        reader = KafkaDataSource(
            options={
                "kafka.bootstrap.servers": "localhost:9092",
                "subscribe": _TOPIC,
                "kafka.isolation.level": "read_committed",
                "minPartitions": "8",
            }
        ).reader(None)
        reader.partitions()
        assert set(consumer.admin.isolation_levels) == {IsolationLevel.READ_COMMITTED}

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
        reader = _reader(
            consumer_factory,
            consumer,
            maxRecordsPerPartition="4",
            startingOffsets='{"orders":{"0":0}}',
            endingOffsets='{"orders":{"0":10}}',
        )
        # Spark's chunking puts the remainder in the last chunk: 3/3/4, not 4/3/3.
        assert _ranges(reader.partitions()) == [(_TOPIC, 0, 0, 3), (_TOPIC, 0, 3, 6), (_TOPIC, 0, 6, 10)]

    def test_max_records_does_not_force_default_sentinels_to_resolve(self, consumer_factory):
        consumer = _FakeConsumer()
        reader = _reader(consumer_factory, consumer, maxRecordsPerPartition="4")
        assert _ranges(reader.partitions()) == [(_TOPIC, 0, _EARLIEST, _LATEST)]
        assert consumer.admin.list_offsets_calls == 0

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

    def test_out_of_range_offset_is_data_loss_by_default(self, consumer_factory):
        # `auto.offset.reset=error` surfaces the offset the broker will not
        # serve instead of moving the position, which is what lets
        # `failOnDataLoss=true` report it.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_offset_out_of_range_message()])
        with pytest.raises(ValueError, match="no longer available on the broker"):
            _read_all(reader, partition)

    def test_fail_on_data_loss_false_recovers_within_the_planned_range(self, consumer_factory):
        # Spark's `getEarliestAvailableOffsetBetween`: resume at the earliest
        # offset that is both still in the log and inside the planned range.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer, failOnDataLoss="false")
        partition = self._partition(
            reader,
            consumer,
            [_offset_out_of_range_message(), _FakeMessage(2), _FakeMessage(3), _FakeMessage(4)],
            watermarks_after={(_TOPIC, 0): (2, 5)},
        )
        assert [r["offset"] for r in _read_all(reader, partition)] == [2, 3, 4]
        assert [(tp.topic, tp.partition, tp.offset) for tp in consumer.seeks] == [(_TOPIC, 0, 2)]

    def test_fail_on_data_loss_false_stops_when_nothing_overlaps(self, consumer_factory):
        # The planned range and what is left of the log have no overlap, so
        # there is nothing to rewind to. Under `auto.offset.reset=earliest` this
        # is where librdkafka would instead replay the whole partition.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer, failOnDataLoss="false")
        partition = self._partition(
            reader,
            consumer,
            [_offset_out_of_range_message()],
            watermarks_after={(_TOPIC, 0): (7, 9)},
        )
        assert _read_all(reader, partition) == []
        assert consumer.seeks == []

    def test_forward_gap_within_the_log_is_skipped(self, consumer_factory):
        # Compacted offsets, transaction control markers and aborted records are
        # unreadable but not lost: the log still starts at or below the
        # requested offset, so Spark skips them.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_FakeMessage(3), _FakeMessage(4)])
        assert [r["offset"] for r in _read_all(reader, partition)] == [3, 4]

    def test_forward_gap_below_the_log_start_is_data_loss(self, consumer_factory):
        # Same shape of gap, different cause: the log now starts above the
        # requested offset, so the records in between are gone rather than
        # merely invisible. Accepting this silently is what a loop that only
        # tracks the last record delivered would do.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(
            reader,
            consumer,
            [_FakeMessage(3), _FakeMessage(4)],
            watermarks_after={(_TOPIC, 0): (3, 5)},
        )
        with pytest.raises(ValueError, match="no longer in the log"):
            _read_all(reader, partition)

    def test_forward_gap_below_the_log_start_is_skipped_when_allowed(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer, failOnDataLoss="false")
        partition = self._partition(
            reader,
            consumer,
            [_FakeMessage(3), _FakeMessage(4)],
            watermarks_after={(_TOPIC, 0): (3, 5)},
        )
        assert [r["offset"] for r in _read_all(reader, partition)] == [3, 4]

    def test_stale_eof_does_not_truncate_the_batch(self, consumer_factory):
        # librdkafka stamps the EOF event with the position it had reached when
        # it queued the event, not when the application polls it. Records queued
        # behind it are still to come, so stopping on it drops them.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(
            reader,
            consumer,
            [_FakeMessage(0), _eof_message_at(1), _FakeMessage(1), _FakeMessage(2)],
        )
        assert [r["offset"] for r in _read_all(reader, partition)] == [0, 1, 2]

    def test_eof_at_the_planned_end_completes_the_read(self, consumer_factory):
        # A terminal EOF needs no watermark lookup at all.
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(reader, consumer, [_FakeMessage(0), _eof_message_at(3)])
        assert [r["offset"] for r in _read_all(reader, partition)] == [0]

    def test_eof_below_the_planned_end_with_a_shrunken_log_is_data_loss(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 5)})
        reader = _reader(consumer_factory, consumer)
        partition = self._partition(
            reader,
            consumer,
            [_FakeMessage(0), _eof_message_at(1)],
            watermarks_after={(_TOPIC, 0): (0, 1)},
        )
        with pytest.raises(ValueError, match="Data loss"):
            _read_all(reader, partition)

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


class TestReadLifecycle:
    def test_reader_and_partitions_deserialize_in_a_fresh_process(self, consumer_factory):
        consumer = _FakeConsumer()
        reader = _reader(
            consumer_factory,
            consumer,
            includeHeaders="true",
            maxBatchBytes="1024",
            **{"kafka.sasl.password": "test-only-secret"},
        )
        partitions = reader.partitions()
        payload = cloudpickle.dumps((reader, partitions))
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    # Reuse the package search path, including editable/native
                    # build locations, without carrying any live reader state.
                    "import json, sys, pysail; from pyspark import cloudpickle; "
                    "pysail.__path__[:] = json.loads(sys.argv[1]); "
                    "reader, parts = cloudpickle.loads(sys.stdin.buffer.read()); "
                    "assert reader._cancel_check is None; "
                    "assert reader.include_headers and parts[0].include_headers; "
                    "assert reader.max_batch_bytes == parts[0].max_batch_bytes == 1024; "
                    "assert parts[0].client_config['sasl.password'] == 'test-only-secret'; "
                    "assert 'test-only-secret' not in repr(parts[0]); "
                    "assert (parts[0].topic, parts[0].start_offset, parts[0].end_offset) == ('orders', -2, -1); "
                    "print('deserialized')"
                ),
                json.dumps(list(pysail.__path__)),
            ],
            input=payload,
            capture_output=True,
            check=False,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr.decode()
        assert result.stdout.strip() == b"deserialized"

    def test_consumer_is_closed_on_arrow_conversion_failure(self, consumer_factory):
        class InvalidTimestamp(_FakeMessage):
            def timestamp(self):
                return (1, 2**63 - 1)

        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 1)}, messages=[InvalidTimestamp(0)])
        reader = _reader(consumer_factory, consumer)
        with pytest.raises(OverflowError):
            _read_all(reader, reader.partitions()[0])
        assert consumer.closed

    def test_refreshes_the_log_start_after_an_earlier_harmless_gap(self, consumer_factory):
        class MovingLog(_FakeConsumer):
            def poll(self, timeout=None):
                record = super().poll(timeout)
                if record.offset() == 5:  # noqa: PLR2004
                    self.watermarks[(_TOPIC, 0)] = (5, 7)
                return record

        consumer = MovingLog(watermarks={(_TOPIC, 0): (0, 7)}, messages=[_FakeMessage(i) for i in [1, 5, 6]])
        reader = _reader(consumer_factory, consumer)
        with pytest.raises(ValueError, match="no longer in the log"):
            _read_all(reader, reader.partitions()[0])
        assert consumer.closed

    def test_poll_uses_only_the_remaining_idle_budget(self, consumer_factory, monkeypatch):
        clock = [0.0]
        timeouts = []

        class IdleConsumer(_FakeConsumer):
            def poll(self, timeout=None):
                timeouts.append(timeout)
                clock[0] += timeout

        monkeypatch.setattr(kafka_module.time, "monotonic", lambda: clock[0])
        consumer = IdleConsumer()
        reader = _reader(consumer_factory, consumer, stallTimeoutMs="10", pollTimeoutMs="80")
        with pytest.raises(TimeoutError, match="Kafka read stalled"):
            _read_all(reader, reader.partitions()[0])
        assert sum(timeouts) == pytest.approx(0.01)

    def test_out_of_range_recovery_without_progress_is_bounded(self, consumer_factory, monkeypatch):
        clock = [0.0]

        class StuckConsumer(_FakeConsumer):
            def poll(self, timeout=None):  # noqa: ARG002
                clock[0] += 0.01
                if clock[0] > 0.1:  # noqa: PLR2004
                    pytest.fail("recovery ignored the idle deadline")
                return _offset_out_of_range_message()

        monkeypatch.setattr(kafka_module.time, "monotonic", lambda: clock[0])
        consumer = StuckConsumer()
        reader = _reader(consumer_factory, consumer, failOnDataLoss="false", stallTimeoutMs="20")
        with pytest.raises(TimeoutError, match="Kafka read stalled"):
            _read_all(reader, reader.partitions()[0])
        assert consumer.closed

    def test_byte_budget_flushes_and_preserves_oversized_records(self, consumer_factory):
        class Payload(_FakeMessage):
            def value(self):
                return b"x" * (1000 if self.offset() == 1 else 200)

        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 3)}, messages=[Payload(i) for i in range(3)])
        reader = _reader(consumer_factory, consumer, maxBatchBytes="300")
        batches = list(reader.read(reader.partitions()[0]))
        assert [b.num_rows for b in batches] == [1, 1, 1]
        assert [len(r["value"]) for b in batches for r in b.to_pylist()] == [200, 1000, 200]

    def test_yield_does_not_retain_the_python_payload_buffer(self, consumer_factory):
        consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 2)}, messages=[_FakeMessage(i) for i in range(2)])
        reader = _reader(consumer_factory, consumer, maxBatchRows="1")
        generator = reader.read(reader.partitions()[0])
        try:
            assert next(generator).num_rows == 1
            assert len(generator.gi_frame.f_locals["buf"]) == 0
        finally:
            generator.close()
        assert consumer.closed

    def test_cancellation_closes_a_consumer_with_no_records(self, consumer_factory):
        cancelled = [False]

        class CancelledConsumer(_FakeConsumer):
            def poll(self, timeout=None):  # noqa: ARG002
                cancelled[0] = True

        consumer = CancelledConsumer()
        reader = _reader(consumer_factory, consumer)
        reader._set_cancel_callback(lambda: cancelled[0])  # noqa: SLF001
        assert _read_all(reader, reader.partitions()[0]) == []
        assert consumer.closed

    def test_cleanup_error_does_not_replace_the_read_failure(self, consumer_factory, caplog):
        class BrokenClose(_FakeConsumer):
            def close(self):
                msg = "cleanup failed with test-only-secret"
                raise RuntimeError(msg)

        consumer = BrokenClose(messages=[_offset_out_of_range_message()])
        reader = _reader(consumer_factory, consumer)
        with pytest.raises(ValueError, match="Data loss"):
            _read_all(reader, reader.partitions()[0])
        assert "close" in caplog.text.lower()
        assert "test-only-secret" not in caplog.text

    def test_skipped_ranges_are_reported(self, consumer_factory, caplog):
        consumer = _FakeConsumer(
            messages=[_offset_out_of_range_message(), _FakeMessage(8), _FakeMessage(9)],
            watermarks_after_assign={(_TOPIC, 0): (8, 10)},
        )
        reader = _reader(consumer_factory, consumer, failOnDataLoss="false")
        assert [r["offset"] for r in _read_all(reader, reader.partitions()[0])] == [8, 9]
        assert "[0, 8)" in caplog.text

    def test_partial_batch_is_flushed_before_the_idle_deadline(self, consumer_factory, monkeypatch):
        clock = [0.0]

        class SlowConsumer(_FakeConsumer):
            def poll(self, timeout=None):
                if self.messages:
                    return self.messages.pop(0)
                clock[0] += timeout
                return None

        monkeypatch.setattr(kafka_module.time, "monotonic", lambda: clock[0])
        consumer = SlowConsumer(messages=[_FakeMessage(0)])
        reader = _reader(consumer_factory, consumer, maxBatchWaitMs="10", stallTimeoutMs="100")
        generator = reader.read(reader.partitions()[0])
        try:
            assert next(generator).column("offset").to_pylist() == [0]
            assert clock[0] == pytest.approx(0.01)
        finally:
            generator.close()

    def test_backpressure_does_not_use_the_broker_idle_budget(self, consumer_factory, monkeypatch):
        clock = [0.0]

        class PausedConsumer(_FakeConsumer):
            def poll(self, timeout=None):
                clock[0] += 0.001
                return super().poll(timeout)

        monkeypatch.setattr(kafka_module.time, "monotonic", lambda: clock[0])
        consumer = PausedConsumer(
            watermarks={(_TOPIC, 0): (0, 2)},
            messages=[_FakeMessage(0), None, _FakeMessage(1)],
        )
        reader = _reader(consumer_factory, consumer, maxBatchRows="1", stallTimeoutMs="20")
        generator = reader.read(reader.partitions()[0])
        assert next(generator).column("offset").to_pylist() == [0]
        clock[0] += 60
        assert next(generator).column("offset").to_pylist() == [1]
        generator.close()


@pytest.fixture(scope="module", autouse=True)
def spark_doctest():
    """Override the parent fixture: these unit tests need no Spark server."""


@pytest.mark.parametrize("path", ["bind", "gap", "eof", "recover"])
def test_watermark_timeout_has_context_and_closes_consumer(consumer_factory, monkeypatch, path):
    def timeout(*_args, **_kwargs):
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT))  # noqa: SLF001

    messages = {
        "bind": [],
        "gap": [_FakeMessage(3)],
        "eof": [_eof_message_at(1)],
        "recover": [_offset_out_of_range_message()],
    }
    consumer = _FakeConsumer(messages=messages[path])
    options = (
        {}
        if path == "bind"
        else {
            "startingOffsets": '{"orders":{"0":0}}',
            "endingOffsets": '{"orders":{"0":5}}',
        }
    )
    if path == "recover":
        options["failOnDataLoss"] = "false"
    reader = _reader(consumer_factory, consumer, **options)
    partition = reader.partitions()[0]
    monkeypatch.setattr(consumer, "get_watermark_offsets", timeout)
    with pytest.raises(TimeoutError, match=r"orders\[0\].*adminTimeoutMs"):
        _read_all(reader, partition)
    assert consumer.closed


def test_watermark_broker_error_is_preserved(consumer_factory, monkeypatch):
    error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)

    def denied(*_args, **_kwargs):
        raise KafkaException(error)

    consumer = _FakeConsumer()
    reader = _reader(consumer_factory, consumer)
    partition = reader.partitions()[0]
    monkeypatch.setattr(consumer, "get_watermark_offsets", denied)
    with pytest.raises(KafkaException) as exc:
        _read_all(reader, partition)
    assert exc.value.args[0] is error
    assert consumer.closed


def test_gap_error_is_limited_to_requested_offsets(consumer_factory):
    consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (3, 11)}, messages=[_FakeMessage(10)])
    reader = _reader(
        consumer_factory, consumer, startingOffsets='{"orders":{"0":0}}', endingOffsets='{"orders":{"0":5}}'
    )
    with pytest.raises(ValueError, match="offsets 0 to 4 were requested"):
        _read_all(reader, reader.partitions()[0])


def test_permissive_gaps_use_one_lookup_per_batch(consumer_factory, monkeypatch, caplog):
    consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (0, 199)}, messages=[_FakeMessage(i) for i in range(0, 199, 2)])
    reader = _reader(
        consumer_factory,
        consumer,
        failOnDataLoss="false",
        maxBatchRows="25",
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":199}}',
    )
    calls = []
    original = consumer.get_watermark_offsets

    def query(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(consumer, "get_watermark_offsets", query)
    rows = _read_all(reader, reader.partitions()[0])
    assert [r["offset"] for r in rows] == list(range(0, 199, 2))
    assert len(calls) == 4  # noqa: PLR2004
    assert "partial read" not in caplog.text


def test_permissive_gaps_report_retention_at_batch_delivery(consumer_factory, caplog):
    consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (3, 5)}, messages=[_FakeMessage(3), _FakeMessage(4)])
    reader = _reader(
        consumer_factory,
        consumer,
        failOnDataLoss="false",
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":5}}',
    )
    assert [r["offset"] for r in _read_all(reader, reader.partitions()[0])] == [3, 4]
    assert "skipped 1 offset interval(s), spanning [0, 3)" in caplog.text


@pytest.mark.skipif(sys.platform == "win32", reason="time.tzset is unavailable on Windows")
def test_reference_normalizes_aware_and_python_local_timestamps(monkeypatch):
    import datetime
    import time

    from pyspark.sql import Row

    from pysail.testing.spark.kafka_reference import canonical_rows

    # Spark collect returns naive timestamps in the Python process timezone,
    # irrespective of spark.sql.session.timeZone.
    try:
        with monkeypatch.context() as context:
            context.setenv("TZ", "Europe/Amsterdam")
            time.tzset()
            instant = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
            values = [
                instant.astimezone(datetime.timezone(datetime.timedelta(hours=2))),
                instant.astimezone().replace(tzinfo=None),
            ]
            rows = [Row(topic="t", partition=0, offset=i, timestamp=value) for i, value in enumerate(values)]
            assert [r["timestamp"] for r in canonical_rows(rows)] == ["2024-01-01T00:00:00+00:00"] * 2
    finally:
        time.tzset()


def test_deferred_gap_and_eof_warning_cover_the_whole_skipped_span(consumer_factory, caplog):
    consumer = _FakeConsumer(watermarks={(_TOPIC, 0): (2, 3)}, messages=[_FakeMessage(2), _eof_message_at(3)])
    reader = _reader(
        consumer_factory,
        consumer,
        failOnDataLoss="false",
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":5}}',
    )
    assert [r["offset"] for r in _read_all(reader, reader.partitions()[0])] == [2]
    assert "skipped 2 offset interval(s), spanning [0, 5)" in caplog.text
