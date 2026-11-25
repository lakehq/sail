# SPDX-License-Identifier: Apache-2.0

"""Fake confluent-kafka clients describing one broker, for planning and read tests."""

from __future__ import annotations

from concurrent.futures import Future

TOPIC = "orders"
# Spark's late-binding sentinels, carried from planning into the task.
EARLIEST = -2
LATEST = -1


def make_future(result=None, exception=None):
    future = Future()
    if exception is None:
        future.set_result(result)
    else:
        future.set_exception(exception)
    return future


class FakeTopicMeta:
    def __init__(self, partitions, error=None, *, is_internal=False):
        self.partitions = dict.fromkeys(partitions)
        self.error = error
        self.is_internal = is_internal


class FakeMessage:
    """Stand-in for confluent_kafka.Message."""

    def __init__(self, offset, *, topic=TOPIC, partition=0, ts=(1, 1700000000000), error=None):
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


def eof_message():
    """An EOF event with no usable offset, exercising the fallback path."""
    from confluent_kafka import KafkaError

    return FakeMessage(-1, error=KafkaError(KafkaError._PARTITION_EOF))  # noqa: SLF001


def eof_message_at(offset):
    """An EOF event as librdkafka reports one, carrying the position it reached."""
    from confluent_kafka import KafkaError

    return FakeMessage(offset, error=KafkaError(KafkaError._PARTITION_EOF))  # noqa: SLF001


def offset_out_of_range_message():
    """The error `auto.offset.reset=error` produces for an unserviceable offset."""
    from confluent_kafka import KafkaError

    return FakeMessage(-1, error=KafkaError(KafkaError._AUTO_OFFSET_RESET))  # noqa: SLF001


class FakeConsumer:
    """Broker state, plus the slice of the Consumer API that ``read()`` uses.

    Planning goes through :class:`FakeAdminClient`, which reads the same state,
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
        positions=None,
    ):
        # `is None` rather than a falsy check: an empty topic map is a valid
        # broker state (it is how the unknown-topic case is set up).
        self.topics = {TOPIC: FakeTopicMeta([0])} if topics is None else topics
        self.watermarks = {(TOPIC, 0): (0, 10)} if watermarks is None else watermarks
        self.times = {} if times is None else times
        self.messages = list(messages or [])
        # Watermarks observed once reading has started, for truncation cases.
        self.watermarks_after_assign = watermarks_after_assign
        self.positions = list(positions or [])
        self.assigned = None
        self.assignments = []
        self.closed = False

    # -- reading -------------------------------------------------------
    def get_watermark_offsets(self, topic_partition, timeout=None, *, cached=False):  # noqa: ARG002
        source = (
            self.watermarks_after_assign
            if self.assigned is not None and self.watermarks_after_assign is not None
            else self.watermarks
        )
        return source[(topic_partition.topic, topic_partition.partition)]

    def assign(self, partitions):
        self.assigned = partitions
        self.assignments.append([(item.topic, item.partition, item.offset) for item in partitions])

    def poll(self, timeout=None):  # noqa: ARG002
        return self.messages.pop(0) if self.messages else None

    def position(self, partitions):
        from confluent_kafka import TopicPartition

        topic_partition = partitions[0]
        offset = self.positions.pop(0) if self.positions else topic_partition.offset
        return [TopicPartition(topic_partition.topic, topic_partition.partition, offset)]

    def close(self):
        self.closed = True


class FakeAdminClient:
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
        from confluent_kafka.admin import ClusterMetadata

        metadata = ClusterMetadata()
        metadata.topics = self.state.topics
        return metadata

    def describe_topics(self, topic_collection, request_timeout=None):  # noqa: ARG002
        from confluent_kafka import KafkaError, KafkaException, Node, TopicPartitionInfo, Uuid
        from confluent_kafka.admin import TopicDescription

        leader = Node(1, "localhost", 9092)
        out = {}
        for name in topic_collection.topic_names:
            if self.hang:
                out[name] = Future()
                continue
            meta = self.state.topics.get(name)
            if meta is None:
                error = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, "unknown topic")
                out[name] = make_future(exception=KafkaException(error))
            elif meta.error is not None:
                out[name] = make_future(exception=KafkaException(meta.error))
            else:
                out[name] = make_future(
                    TopicDescription(
                        name=name,
                        topic_id=Uuid(0, 0),
                        is_internal=meta.is_internal,
                        partitions=[
                            TopicPartitionInfo(id=p, leader=leader, replicas=[leader], isr=[leader])
                            for p in sorted(meta.partitions)
                        ],
                    )
                )
        return out

    def list_offsets(self, specs, isolation_level=None, request_timeout=None):  # noqa: ARG002
        from confluent_kafka import KafkaException
        from confluent_kafka.admin import ListOffsetsResultInfo, OffsetSpec

        self.list_offsets_calls += 1
        self.isolation_levels.append(isolation_level)
        out = {}
        earliest_spec = OffsetSpec.earliest()
        latest_spec = OffsetSpec.latest()
        for topic_partition, spec in specs.items():
            low, high = self.state.watermarks[(topic_partition.topic, topic_partition.partition)]
            if spec == earliest_spec:
                offset = low
            elif spec == latest_spec:
                offset = high
            else:
                offset, error = self.state.times.get(
                    (topic_partition.topic, topic_partition.partition, spec.timestamp), (-1, None)
                )
                if error is not None:
                    out[topic_partition] = make_future(exception=KafkaException(error))
                    continue
            out[topic_partition] = make_future(ListOffsetsResultInfo(offset=offset, timestamp=-1, leader_epoch=-1))
        return out


def make_reader(consumer_factory, consumer, **options):
    """Install ``consumer`` as the broker and return a reader for topic ``TOPIC``."""
    from pysail.spark.datasource.kafka import KafkaDataSource

    consumer_factory(consumer)
    opts = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": TOPIC, **options}
    return KafkaDataSource(options=opts).reader(None)


def planned_ranges(partitions):
    """Ranges as planned, which may still carry the -2/-1 late-binding sentinels."""
    return [(*p.offset_range.topic_partition, p.offset_range.start, p.offset_range.end) for p in partitions]


def bound_ranges(reader, consumer, partitions):
    """Ranges as each task will see them, after binding sentinels at task start."""
    from pysail.spark.datasource.kafka.cursor import bind_offset_range

    timeout_s = reader.options.admin_timeout_s
    return [bind_offset_range(consumer, p.offset_range, timeout_s=timeout_s) for p in partitions]


def read_all(reader, partition):
    """Read one input partition to completion and return its rows as dicts."""
    rows = []
    for batch in reader.read(partition):
        rows.extend(batch.to_pylist())
    return rows
