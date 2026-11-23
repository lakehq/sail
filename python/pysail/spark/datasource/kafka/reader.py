"""Kafka batch reader, input partition, fixed Spark-compatible schema, and Arrow buffering."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, NamedTuple, TypedDict

import pyarrow as pa
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient
from pyspark.sql.datasource import DataSourceReader, InputPartition

from pysail.spark.datasource.kafka.options import (
    SpecialOffset,
    TimestampStrategy,
    TopicPartitionKey,
    build_admin_config,
    build_consumer_config,
)
from pysail.spark.datasource.kafka.planning import (
    OffsetRange,
    list_topic_partitions,
    resolve_endpoint_offsets,
    resolve_sentinels,
    restore_outer_bounds,
    split_offset_ranges,
    validate_limit_partitions,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Literal, TypeAlias

    from confluent_kafka import IsolationLevel, Message

    from pysail.spark.datasource.kafka.options import RangeLimit, Subscription

    EofOutcome: TypeAlias = Literal["complete", "partial", "stale"]

LOGGER = logging.getLogger(__name__)


# ============================================================================
# Fixed Spark-compatible schema
# ============================================================================

# Matches the column names, order, types, and nullability of
# ``org.apache.spark.sql.kafka010.KafkaRecordToRowConverter``'s output schema.
# ``headers`` is only appended when the ``includeHeaders`` option is true.
#
# Every field is nullable because Spark builds these with plain ``StructField``s,
# whose ``nullable`` defaults to true. Declaring the always-populated columns
# non-nullable would be a user-visible schema difference: nullability shows up in
# ``printSchema`` output and in the compatibility checks Spark runs when a
# DataFrame is written into an existing table.

BASE_FIELDS: list[pa.Field] = [
    pa.field("key", pa.binary(), nullable=True),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("topic", pa.string(), nullable=True),
    pa.field("partition", pa.int32(), nullable=True),
    pa.field("offset", pa.int64(), nullable=True),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("timestampType", pa.int32(), nullable=True),
]

HEADER_STRUCT = pa.struct([pa.field("key", pa.string(), nullable=True), pa.field("value", pa.binary(), nullable=True)])


class SparkTimestampType(IntEnum):
    """``org.apache.kafka.common.record.TimestampType`` ids, as Spark reports them."""

    NO_TIMESTAMP_TYPE = -1
    CREATE_TIME = 0
    LOG_APPEND_TIME = 1


# Translate librdkafka's timestamp type ids to Spark's enum values so downstream
# code keying off ``timestampType`` behaves the same as against the JVM source.
LIBRDKAFKA_TO_SPARK_TS_TYPE = {
    TIMESTAMP_NOT_AVAILABLE: SparkTimestampType.NO_TIMESTAMP_TYPE,
    TIMESTAMP_CREATE_TIME: SparkTimestampType.CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME: SparkTimestampType.LOG_APPEND_TIME,
}

# Kafka reports no timestamp as -1 ms, and Spark passes that straight through
# ``millisToMicros``, surfacing 1969-12-31 23:59:59.999 rather than null. Only
# ``timestampType`` marks the record as untimed.
NO_TIMESTAMP_MS = -1

# An offset the broker will not serve arrives two ways: as the broker's own
# ``OFFSET_OUT_OF_RANGE``, and as the local ``_AUTO_OFFSET_RESET`` librdkafka
# raises when ``auto.offset.reset=error`` declines to move the position itself.
OUT_OF_RANGE_CODES = (
    KafkaError.OFFSET_OUT_OF_RANGE,
    KafkaError._AUTO_OFFSET_RESET,  # noqa: SLF001
)


def build_schema(*, include_headers: bool) -> pa.Schema:
    fields = list(BASE_FIELDS)
    if include_headers:
        fields.append(pa.field("headers", pa.list_(HEADER_STRUCT), nullable=True))
    return pa.schema(fields)


# ============================================================================
# Row buffer — accumulates messages, flushes to a RecordBatch
# ============================================================================


def record_size(record: Message, *, include_headers: bool) -> int:
    """Conservative Arrow payload estimate, not a process-memory measurement."""
    size = 64 + len(record.key() or b"") + len(record.value() or b"") + len(record.topic().encode("utf-8"))
    if include_headers:
        size += sum(16 + len(key.encode("utf-8")) + len(value or b"") for key, value in record.headers() or [])
    return size


class KafkaHeader(TypedDict):
    """An Arrow header struct, including nullable Kafka header values."""

    key: str
    value: bytes | None


class Watermarks(NamedTuple):
    """The broker's low offset and exclusive high offset."""

    low: int
    high: int


class RecordErrorResult(NamedTuple):
    """Read-loop action and updated position after a Kafka error event."""

    stop: bool
    partial_read: bool
    next_offset: int
    stalled_recoveries: int


class _RowBuffer:
    """Column-major accumulator that builds an Arrow RecordBatch on flush."""

    include_headers: bool
    size_bytes: int
    key: list[bytes | None]
    value: list[bytes | None]
    topic: list[str]
    partition: list[int]
    offset: list[int]
    timestamp_us: list[int]
    timestamp_type: list[int]
    headers: list[list[KafkaHeader] | None]

    def __init__(self, *, include_headers: bool) -> None:
        self.include_headers = include_headers
        self.clear()

    def clear(self) -> None:
        self.size_bytes = 0
        self.key = []
        self.value = []
        self.topic = []
        self.partition = []
        self.offset = []
        self.timestamp_us = []
        self.timestamp_type = []
        self.headers = []

    def __len__(self) -> int:
        # Row count; every column list has the same length.
        return len(self.offset)

    def append(self, record: Message, size_bytes: int) -> None:
        self.size_bytes += size_bytes
        ts_type, ts_ms = record.timestamp()
        self.key.append(record.key())
        self.value.append(record.value())
        self.topic.append(record.topic())
        self.partition.append(record.partition())
        self.offset.append(record.offset())
        # Spark surfaces -1ms (1969-12-31 23:59:59.999) for untimed records rather than null.
        if ts_type == TIMESTAMP_NOT_AVAILABLE:
            ts_ms = NO_TIMESTAMP_MS
        self.timestamp_us.append(ts_ms * 1000)
        # Deliberately map any unknown/future librdkafka timestamp type to NO_TIMESTAMP_TYPE.
        self.timestamp_type.append(LIBRDKAFKA_TO_SPARK_TS_TYPE.get(ts_type, SparkTimestampType.NO_TIMESTAMP_TYPE))
        if self.include_headers:
            headers = record.headers()
            self.headers.append([{"key": k, "value": v} for k, v in headers] if headers else None)

    def to_batch(self, schema: pa.Schema) -> pa.RecordBatch:
        arrays = [
            pa.array(self.key, type=pa.binary()),
            pa.array(self.value, type=pa.binary()),
            pa.array(self.topic, type=pa.string()),
            pa.array(self.partition, type=pa.int32()),
            pa.array(self.offset, type=pa.int64()),
            pa.array(self.timestamp_us, type=pa.timestamp("us", tz="UTC")),
            pa.array(self.timestamp_type, type=pa.int32()),
        ]
        if self.include_headers:
            arrays.append(pa.array(self.headers, type=pa.list_(HEADER_STRUCT)))
        batch = pa.record_batch(arrays, schema=schema)
        # Release Python payloads before yielding across backpressure boundaries.
        self.clear()
        return batch


# ============================================================================
# InputPartition — one per (topic, partition, offset-range)
# ============================================================================


class KafkaInputPartition(InputPartition):
    def __init__(
        self,
        partition_id: int,
        *,
        topic: str,
        partition: int,
        start: int,
        end: int,
    ) -> None:
        super().__init__(partition_id)
        self.topic = topic
        self.partition = partition
        self.start = start
        self.end = end  # exclusive

    def __repr__(self) -> str:
        return (
            f"KafkaInputPartition(topic={self.topic!r}, partition={self.partition}, start={self.start}, end={self.end})"
        )


# ============================================================================
# DataSourceReader
# ============================================================================


MAX_STALLED_RECOVERIES = 3


@contextmanager
def open_consumer(
    client_config: dict[str, str], isolation_level: IsolationLevel, *, topic: str, partition: int
) -> Iterator[Consumer]:
    """Open a consumer and close it without leaking credentials or masking read errors."""
    consumer = Consumer(build_consumer_config(client_config, isolation_level))
    try:
        yield consumer
    except BaseException:
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            # Omit client exception text (may contain security config) and do not mask read errors.
            LOGGER.warning("Failed to close Kafka consumer for %s[%s]", topic, partition)
        raise
    else:
        try:
            consumer.close()
        except Exception:
            LOGGER.warning("Failed to close Kafka consumer for %s[%s]", topic, partition)
            raise


@dataclass(kw_only=True, repr=False, eq=False)
class KafkaDataSourceReader(DataSourceReader):
    subscription: Subscription
    starting: RangeLimit
    ending: RangeLimit
    starting_ts_strategy: TimestampStrategy
    client_config: dict[str, str]
    isolation_level: IsolationLevel
    include_headers: bool
    poll_timeout_s: float
    max_batch_rows: int
    max_batch_bytes: int
    admin_timeout_s: float
    fail_on_data_loss: bool
    min_partitions: int | None
    max_records_per_partition: int | None

    # ------------------------------------------------------------------
    # Partition planning — talks to the broker once on the driver
    # ------------------------------------------------------------------

    def partitions(self) -> list[InputPartition]:
        # Short-lived AdminClient matching Spark's KafkaOffsetReaderAdmin (exposes no close()).
        # earliest/latest remain -2/-1 sentinels until each task binds them in read().
        admin = AdminClient(build_admin_config(self.client_config))

        topic_partitions = list_topic_partitions(admin, self.subscription, timeout_s=self.admin_timeout_s)
        validate_limit_partitions(self.starting, topic_partitions, boundary="starting")
        validate_limit_partitions(self.ending, topic_partitions, boundary="ending")

        starts = resolve_endpoint_offsets(
            admin,
            topic_partitions,
            self.starting,
            timeout_s=self.admin_timeout_s,
            strategy=self.starting_ts_strategy,
            isolation_level=self.isolation_level,
        )
        ends = resolve_endpoint_offsets(
            admin,
            topic_partitions,
            self.ending,
            timeout_s=self.admin_timeout_s,
            strategy=TimestampStrategy.LATEST,
            isolation_level=self.isolation_level,
        )

        offset_ranges = [OffsetRange(topic_partition=tp, start=starts[tp], end=ends[tp]) for tp in topic_partitions]

        # Match Spark 4.1 shouldDivvyUpLargePartitions (tests unresolved sizes), then
        # resolve only to place interior split points and restore outer sentinels.
        needs_split = (
            self.max_records_per_partition is not None
            and any(offset_range.size > self.max_records_per_partition for offset_range in offset_ranges)
        ) or (self.min_partitions is not None and self.min_partitions > len(offset_ranges))
        if needs_split:
            original_ranges = {offset_range.topic_partition: offset_range for offset_range in offset_ranges}
            offset_ranges = resolve_sentinels(
                admin, offset_ranges, timeout_s=self.admin_timeout_s, isolation_level=self.isolation_level
            )
            offset_ranges = split_offset_ranges(
                [offset_range for offset_range in offset_ranges if offset_range.size > 0],
                min_partitions=self.min_partitions,
                max_records_per_partition=self.max_records_per_partition,
            )
            offset_ranges = restore_outer_bounds(offset_ranges, original_ranges)

        return [
            KafkaInputPartition(
                pid,
                topic=offset_range.topic_partition.topic,
                partition=offset_range.topic_partition.partition,
                start=offset_range.start,
                end=offset_range.end,
            )
            for pid, offset_range in enumerate(offset_ranges)
        ]

    # ------------------------------------------------------------------
    # Per-partition read — runs on executors
    # ------------------------------------------------------------------

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        if not isinstance(partition, KafkaInputPartition):
            msg = f"Expected KafkaInputPartition, got {type(partition)}"
            raise TypeError(msg)

        schema = build_schema(include_headers=self.include_headers)
        partial_read = False
        with open_consumer(
            self.client_config, self.isolation_level, topic=partition.topic, partition=partition.partition
        ) as consumer:
            bound = self._bind_partition_range(consumer, partition)
            if bound.size == 0:
                return

            topic_partition = TopicPartition(bound.topic_partition.topic, bound.topic_partition.partition, bound.start)
            consumer.assign([topic_partition])

            row_buffer = _RowBuffer(include_headers=self.include_headers)
            next_offset = bound.start
            stalled_recoveries = 0

            try:
                while next_offset < bound.end:
                    record = consumer.poll(self.poll_timeout_s)
                    if record is None:
                        position = consumer.position([topic_partition])[0].offset
                        if position > next_offset:
                            # Kafka may advance past invisible/control records without returning a visible message.
                            next_offset = position
                            stalled_recoveries = 0
                            continue
                        msg = (
                            f"Cannot fetch record for offset {next_offset} on "
                            f"{topic_partition.topic}[{topic_partition.partition}] in "
                            f"{self.poll_timeout_s * 1000:g} milliseconds"
                        )
                        raise TimeoutError(msg)

                    if record.error() is not None:
                        result = self._handle_record_error(
                            consumer,
                            topic_partition,
                            record,
                            next_offset=next_offset,
                            end=bound.end,
                            stalled_recoveries=stalled_recoveries,
                        )
                        partial_read |= result.partial_read
                        next_offset = result.next_offset
                        stalled_recoveries = result.stalled_recoveries
                        if result.stop:
                            break
                        continue

                    stalled_recoveries = 0
                    record_offset = record.offset()
                    if record_offset < next_offset:
                        msg = (
                            f"Kafka returned offset {record_offset} for {topic_partition.topic}"
                            f"[{topic_partition.partition}] while offset {next_offset} was requested"
                        )
                        raise ValueError(msg)
                    if record_offset > next_offset and self.fail_on_data_loss:
                        self._check_offset_gap(
                            consumer,
                            topic_partition,
                            next_offset=next_offset,
                            record_offset=record_offset,
                            end_offset=bound.end,
                        )
                    if record_offset >= bound.end:
                        break

                    record_bytes = record_size(record, include_headers=self.include_headers)
                    if len(row_buffer) and row_buffer.size_bytes + record_bytes > self.max_batch_bytes:
                        yield row_buffer.to_batch(schema)
                    row_buffer.append(record, record_bytes)
                    next_offset = record_offset + 1

                    if len(row_buffer) >= self.max_batch_rows or row_buffer.size_bytes >= self.max_batch_bytes:
                        yield row_buffer.to_batch(schema)

                if len(row_buffer) > 0:
                    yield row_buffer.to_batch(schema)
            finally:
                if partial_read:
                    LOGGER.warning(
                        "Kafka partial read on %s[%s]: skipped unavailable offsets; failOnDataLoss=false",
                        topic_partition.topic,
                        topic_partition.partition,
                    )

    def _handle_record_error(
        self,
        consumer: Consumer,
        topic_partition: TopicPartition,
        record: Message,
        *,
        next_offset: int,
        end: int,
        stalled_recoveries: int,
    ) -> RecordErrorResult:
        """Handle a non-data poll event (EOF, out-of-range, or broker error).

        Returns the stop decision, partial-read flag, and updated recovery position.
        """
        error = record.error()
        code = error.code()
        if code == KafkaError._PARTITION_EOF:  # noqa: SLF001
            outcome = self._classify_eof(
                consumer, topic_partition, message_offset=record.offset(), next_offset=next_offset, end_offset=end
            )
            if outcome != "stale":
                return RecordErrorResult(
                    stop=True,
                    partial_read=outcome == "partial",
                    next_offset=next_offset,
                    stalled_recoveries=stalled_recoveries,
                )
            # Stale EOF the log has grown past; it can still advance past control records.
            if record.offset() > next_offset:
                return RecordErrorResult(
                    stop=False, partial_read=False, next_offset=record.offset(), stalled_recoveries=0
                )
            return RecordErrorResult(
                stop=False, partial_read=False, next_offset=next_offset, stalled_recoveries=stalled_recoveries
            )
        if code in OUT_OF_RANGE_CODES:
            recovered = self._recover_out_of_range(consumer, topic_partition, next_offset=next_offset, end_offset=end)
            if recovered is None:
                return RecordErrorResult(
                    stop=True, partial_read=True, next_offset=next_offset, stalled_recoveries=stalled_recoveries
                )
            if recovered > next_offset:
                return RecordErrorResult(stop=False, partial_read=True, next_offset=recovered, stalled_recoveries=0)
            stalled_recoveries += 1
            if stalled_recoveries >= MAX_STALLED_RECOVERIES:
                msg = f"Kafka out-of-range recovery made no progress on {topic_partition.topic}[{topic_partition.partition}] at offset {next_offset}"
                raise TimeoutError(msg)
            return RecordErrorResult(
                stop=False, partial_read=False, next_offset=next_offset, stalled_recoveries=stalled_recoveries
            )
        raise KafkaException(error)

    def _query_watermarks(self, consumer: Consumer, topic_partition: TopicPartition) -> Watermarks:
        """Query fresh watermarks, adding task context to client timeouts."""
        try:
            return Watermarks(
                *consumer.get_watermark_offsets(topic_partition, timeout=self.admin_timeout_s, cached=False)
            )
        except KafkaException as e:
            if e.args[0].code() != KafkaError._TIMED_OUT:  # noqa: SLF001
                raise
            msg = (
                f"Timed out querying offsets for {topic_partition.topic}[{topic_partition.partition}] after "
                f"{self.admin_timeout_s:g}s; check broker connectivity or raise 'adminTimeoutMs'."
            )
            raise TimeoutError(msg) from e

    def _bind_partition_range(self, consumer: Consumer, partition: KafkaInputPartition) -> OffsetRange:
        """Late-bind ``earliest``/``latest`` sentinels at task start, mirroring ``KafkaSourceRDD.resolveRange``."""
        start, end = partition.start, partition.end
        if start >= 0 and end >= 0:
            return OffsetRange(TopicPartitionKey(partition.topic, partition.partition), start, end)
        watermarks = self._query_watermarks(consumer, TopicPartition(partition.topic, partition.partition))
        if start == SpecialOffset.EARLIEST:
            start = watermarks.low
        if end == SpecialOffset.LATEST:
            end = watermarks.high
        return OffsetRange(TopicPartitionKey(partition.topic, partition.partition), start, end)

    def _recover_out_of_range(
        self, consumer: Consumer, topic_partition: TopicPartition, *, next_offset: int, end_offset: int
    ) -> int | None:
        """Handle an out-of-range fetch: fail under ``failOnDataLoss``, else clamp to the available overlap."""
        if self.fail_on_data_loss:
            msg = (
                f"Data loss on {topic_partition.topic}[{topic_partition.partition}]: offset {next_offset} is no "
                f"longer available on the broker. Records were removed after planning (retention or "
                f"truncation), or the offset is past the end of the log. "
                f"Set 'failOnDataLoss' to false to skip missing records."
            )
            raise ValueError(msg)
        watermarks = self._query_watermarks(consumer, topic_partition)
        if next_offset >= watermarks.high or watermarks.low >= end_offset:
            return None
        recovered = max(next_offset, watermarks.low)
        consumer.seek(TopicPartition(topic_partition.topic, topic_partition.partition, recovered))
        return recovered

    def _check_offset_gap(
        self, consumer: Consumer, tp: TopicPartition, *, next_offset: int, record_offset: int, end_offset: int
    ) -> None:
        """Distinguish unreadable in-log gaps (compaction/control markers) from retention data loss.

        Always queries fresh watermarks because the log start can advance between gaps.
        """
        watermarks = self._query_watermarks(consumer, tp)
        if watermarks.low <= next_offset:
            return
        msg = (
            f"Data loss on {tp.topic}[{tp.partition}]: offsets {next_offset} to "
            f"{min(record_offset, end_offset) - 1} were requested but are no longer in the log, which now starts at "
            f"{watermarks.low}. Records were removed after planning (retention or truncation). "
            f"Set 'failOnDataLoss' to false to skip missing records."
        )
        raise ValueError(msg)

    def _classify_eof(
        self,
        consumer: Consumer,
        topic_partition: TopicPartition,
        *,
        message_offset: int | None,
        next_offset: int,
        end_offset: int,
    ) -> EofOutcome:
        """Classify a ``_PARTITION_EOF`` event as complete, partial (permissive), stale, or data loss."""
        eof_offset = message_offset if message_offset is not None and message_offset >= 0 else None
        position = next_offset if eof_offset is None else eof_offset
        if position >= end_offset:
            return "complete"
        watermarks = self._query_watermarks(consumer, topic_partition)
        if eof_offset is not None and watermarks.high > eof_offset:
            return "stale"
        if watermarks.high >= end_offset:
            return "complete"
        if not self.fail_on_data_loss:
            return "partial"
        msg = (
            f"Data loss on {topic_partition.topic}[{topic_partition.partition}]: reached end of log at offset "
            f"{next_offset}, but the read was planned up to {end_offset} and the high watermark "
            f"is now {watermarks.high}. Records were removed after planning (retention or truncation), or "
            f"'endingOffsets' points past the end of the log. "
            f"Set 'failOnDataLoss' to false to return the records that remain."
        )
        raise ValueError(msg)
