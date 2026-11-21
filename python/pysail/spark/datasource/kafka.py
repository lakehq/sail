"""Kafka batch data source for Sail, backed by confluent-kafka (librdkafka).

Exposes ``spark.read.format("kafka")`` with a Spark-compatible schema and
option surface. Every read resolves to a bounded ``[start_offset, end_offset)``
range per partition, with ``earliest``/``latest`` bound per task rather than on
the driver, as Spark's batch Kafka source does.

Install the optional dependency before use::

    pip install pysail[kafka]
"""

from __future__ import annotations

import json
import logging
import math
import re
import sys
import time
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING

import pyarrow as pa

try:
    from confluent_kafka import Consumer, IsolationLevel, KafkaError, KafkaException, TopicCollection, TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec
except ImportError as e:
    msg = "confluent-kafka >= 2.5 is required for the kafka data source. Install it with: pip install pysail[kafka]"
    raise ImportError(msg) from e

try:
    from pyspark.sql.datasource import CaseInsensitiveDict, DataSource, DataSourceReader, InputPartition
except ImportError as e:
    msg = "PySpark with the DataSource API is required (PySpark >= 4.0)."
    raise ImportError(msg) from e

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

_LOGGER = logging.getLogger(__name__)
_POLL_SLICE_S = 0.25


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

_BASE_FIELDS: list[pa.Field] = [
    pa.field("key", pa.binary(), nullable=True),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("topic", pa.string(), nullable=True),
    pa.field("partition", pa.int32(), nullable=True),
    pa.field("offset", pa.int64(), nullable=True),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("timestampType", pa.int32(), nullable=True),
]

_HEADER_STRUCT = pa.struct([pa.field("key", pa.string(), nullable=True), pa.field("value", pa.binary(), nullable=True)])

# librdkafka's timestamp types. Spark reports
# ``org.apache.kafka.common.record.TimestampType``'s -1/0/1, so translate on the
# way out: downstream code keying off Spark's enum (e.g. ``timestampType == 0``
# for CreateTime) then behaves the same as against the JVM Kafka source.
_LIBRDKAFKA_TS_NOT_AVAILABLE = 0
_LIBRDKAFKA_TO_SPARK_TS_TYPE = {
    _LIBRDKAFKA_TS_NOT_AVAILABLE: -1,  # NOT_AVAILABLE    -> NO_TIMESTAMP_TYPE
    1: 0,  # CREATE_TIME      -> CREATE_TIME
    2: 1,  # LOG_APPEND_TIME  -> LOG_APPEND_TIME
}

# Kafka reports no timestamp as -1 ms, and Spark passes that straight through
# ``millisToMicros``, surfacing 1969-12-31 23:59:59.999 rather than null. Only
# ``timestampType`` marks the record as untimed.
_NO_TIMESTAMP_MS = -1

# An offset the broker will not serve arrives two ways: as the broker's own
# ``OFFSET_OUT_OF_RANGE``, and as the local ``_AUTO_OFFSET_RESET`` librdkafka
# raises when ``auto.offset.reset=error`` declines to move the position itself.
_OUT_OF_RANGE_CODES = (
    KafkaError.OFFSET_OUT_OF_RANGE,
    KafkaError._AUTO_OFFSET_RESET,  # noqa: SLF001
)


def _build_schema(*, include_headers: bool) -> pa.Schema:
    fields = list(_BASE_FIELDS)
    if include_headers:
        fields.append(pa.field("headers", pa.list_(_HEADER_STRUCT), nullable=True))
    return pa.schema(fields)


# ============================================================================
# Option parsing
# ============================================================================


_SENTINEL_EARLIEST = "earliest"
_SENTINEL_LATEST = "latest"

# Spark's per-partition offset sentinels.
_SPARK_OFFSET_LATEST = -1
_SPARK_OFFSET_EARLIEST = -2

# Scala String.toInt / String.toLong bounds used by Spark's split options.
_INT32_MAX = 2**31 - 1
_INT64_MAX = 2**63 - 1

# ``startingOffsetsByTimestampStrategy`` values.
_TS_STRATEGY_ERROR = "error"
_TS_STRATEGY_LATEST = "latest"

_SUBSCRIPTION_KEYS = ("assign", "subscribe", "subscribePattern")


def _check_integer_width(value: int, *, bits: int, label: str) -> int:
    """Reject values that cannot cross Kafka's signed integer API boundary."""
    limit = 1 << (bits - 1)
    if not -limit <= value < limit:
        msg = f"{label} must fit a signed {bits}-bit integer, got: {value}"
        raise ValueError(msg)
    return value


def _parse_tp_mapping(raw: str, *, option_name: str) -> dict[str, dict[str, int]]:
    """Parse and normalize a ``{topic: {partition: int}}`` JSON mapping.

    Partition keys are canonicalized via ``int()`` (e.g. ``"00"`` -> ``"0"``) to
    match Spark's ``Map[Int, Long]`` deserialization (last spelling wins).
    """
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        msg = f"'{option_name}' JSON must be an object, got: {type(parsed).__name__}"
        raise ValueError(msg)  # noqa: TRY004
    normalized: dict[str, dict[str, int]] = {}
    for topic, partitions in parsed.items():
        if not isinstance(partitions, dict):
            msg = f"'{option_name}' entry for topic {topic!r} must be an object of {{partition: value}}, got: {type(partitions).__name__}"
            raise ValueError(msg)  # noqa: TRY004
        canonical: dict[str, int] = {}
        for partition, value in partitions.items():
            partition_id = _check_integer_width(
                int(partition), bits=32, label=f"'{option_name}' partition id for topic {topic!r}"
            )
            # bool is an int subclass; reject JSON true/false.
            if not isinstance(value, int) or isinstance(value, bool):
                msg = f"'{option_name}' value for topic {topic!r} partition {partition!r} must be an integer, got: {type(value).__name__}"
                raise ValueError(msg)  # noqa: TRY004
            _check_integer_width(
                value, bits=64, label=f"'{option_name}' value for topic {topic!r} partition {partition!r}"
            )
            canonical[str(partition_id)] = value
        normalized[topic] = canonical
    return normalized


def _validate_offset_spec(
    spec: str | dict[str, dict[str, int]] | None,
    *,
    endpoint: str,
    option_name: str,
) -> None:
    """Enforce Spark's batch-mode rules on an offset spec.

    Rejects ``latest``/``-1`` at ``startingOffsets``, ``earliest``/``-2`` at
    ``endingOffsets``, and any offset below ``-2`` (which librdkafka treats as
    special sentinels such as ``-1000`` ``OFFSET_STORED``).
    """
    if spec is None:
        return

    forbidden_sentinel = _SENTINEL_LATEST if endpoint == "starting" else _SENTINEL_EARLIEST
    forbidden_offset = _SPARK_OFFSET_LATEST if endpoint == "starting" else _SPARK_OFFSET_EARLIEST
    allowed_sentinel = _SENTINEL_EARLIEST if endpoint == "starting" else _SENTINEL_LATEST

    if isinstance(spec, str):
        if spec == forbidden_sentinel:
            msg = (
                f"'{option_name}' does not support {forbidden_sentinel!r} for batch reads; "
                f"use {allowed_sentinel!r} or per-partition JSON"
            )
            raise ValueError(msg)
        return

    for topic, partitions in spec.items():
        for partition, value in partitions.items():
            where = f"topic {topic!r} partition {partition!r}"
            if value == forbidden_offset:
                msg = (
                    f"'{option_name}' does not support offset {forbidden_offset} "
                    f"({forbidden_sentinel}) for batch reads, at {where}"
                )
                raise ValueError(msg)
            if value < _SPARK_OFFSET_EARLIEST:
                msg = (
                    f"'{option_name}' offset for {where} must be >= 0, "
                    f"{_SPARK_OFFSET_LATEST} (latest), or {_SPARK_OFFSET_EARLIEST} (earliest), got: {value}"
                )
                raise ValueError(msg)


def _resolve_range_option(
    opts: Mapping[str, str], *, endpoint: str
) -> tuple[str | dict[str, dict[str, int]] | None, int | dict[str, dict[str, int]] | None]:
    """Select one range option per endpoint using Spark's precedence.

    Global timestamp wins over per-partition timestamps, which win over offsets.
    Skipped lower-priority options are not parsed, matching Spark's
    ``KafkaSourceProvider.getKafkaOffsetRangeLimit``.
    """
    global_ts_key = f"{endpoint}Timestamp"
    per_tp_ts_key = f"{endpoint}OffsetsByTimestamp"
    offsets_key = f"{endpoint}Offsets"

    if global_ts_key in opts:
        return None, _check_integer_width(int(opts[global_ts_key]), bits=64, label=f"'{global_ts_key}'")
    if per_tp_ts_key in opts:
        return None, _parse_tp_mapping(opts[per_tp_ts_key], option_name=per_tp_ts_key)
    if offsets_key in opts:
        stripped = opts[offsets_key].strip()
        lowered = stripped.lower()
        spec: str | dict[str, dict[str, int]] = (
            lowered
            if lowered in (_SENTINEL_EARLIEST, _SENTINEL_LATEST)
            else _parse_tp_mapping(stripped, option_name=offsets_key)
        )
        _validate_offset_spec(spec, endpoint=endpoint, option_name=offsets_key)
        return spec, None
    return None, None


def _parse_bool(raw: str, name: str) -> bool:
    """Parse a boolean strictly (case-insensitive ``true``/``false``, matching Scala ``toBoolean``)."""
    value = raw.strip().lower()
    if value in ("true", "false"):
        return value == "true"
    msg = f"'{name}' must be 'true' or 'false', got {raw!r}"
    raise ValueError(msg)


def _parse_positive_int(raw: str, name: str, *, max_value: int | None = None) -> int:
    """Parse a positive integer option, optionally capped to a Scala/Kafka width."""
    value = int(raw)
    if value <= 0:
        msg = f"'{name}' must be positive, got {value}"
        raise ValueError(msg)
    if max_value is not None and value > max_value:
        msg = f"'{name}' must be at most {max_value}, got {value}"
        raise ValueError(msg)
    return value


def _parse_positive_ms(raw: str, name: str, *, max_value: int | None = None) -> float:
    """Parse a positive, finite millisecond option and return seconds."""
    value_ms = float(raw)
    if not math.isfinite(value_ms) or value_ms <= 0:
        msg = f"'{name}' must be a positive, finite number of milliseconds, got {raw!r}"
        raise ValueError(msg)
    if max_value is not None and not 1 <= value_ms <= max_value:
        msg = f"'{name}' must be between 1 and {max_value} milliseconds, got {raw!r}"
        raise ValueError(msg)
    return value_ms / 1000.0


# Source-owned consumer properties rejected to match Spark's validateGeneralOptions:
# overriding them silently moves planned offsets, commits group offsets, or disables EOF stop.
_PROTECTED_KAFKA_CONFIGS = {
    "auto.offset.reset": "startingOffsets/endingOffsets",
    "enable.auto.commit": None,
    "enable.partition.eof": None,
}

# Stable group id: reads use explicit assign() without group membership or commits,
# so concurrent queries do not coordinate or mutate group state. Overridable via kafka.group.id.
_DEFAULT_GROUP_ID = "sail-kafka-reader"

# Consumer-only properties stripped from AdminClient (producer-backed in librdkafka)
# to avoid CONFWARN logs; isolation.level is passed directly to list_offsets instead.
_CONSUMER_ONLY_CONFIGS = (
    "group.id",
    "enable.auto.commit",
    "auto.offset.reset",
    "enable.partition.eof",
    "isolation.level",
    "queued.max.messages.kbytes",
    "queued.min.messages",
    "fetch.max.bytes",
    "max.partition.fetch.bytes",
    "fetch.message.max.bytes",
    "fetch.min.bytes",
    "fetch.wait.max.ms",
    "fetch.error.backoff.ms",
    "check.crcs",
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "max.poll.interval.ms",
    "group.instance.id",
    "auto.commit.interval.ms",
    "partition.assignment.strategy",
    "enable.auto.offset.store",
)


def _build_admin_config(client_config: dict[str, str]) -> dict[str, str]:
    """Return the librdkafka config for the planning-time ``AdminClient``."""
    return {k: v for k, v in client_config.items() if k not in _CONSUMER_ONLY_CONFIGS}


def _planning_isolation_level(client_config: Mapping[str, str]) -> IsolationLevel:
    """Return the isolation level for planning-time ``list_offsets`` (Spark default: ``read_uncommitted``)."""
    value = _canonical_isolation_level(client_config.get("isolation.level", "read_uncommitted"))
    if value == "read_committed":
        return IsolationLevel.READ_COMMITTED
    return IsolationLevel.READ_UNCOMMITTED


def _canonical_isolation_level(raw: str) -> str:
    """Normalize ``kafka.isolation.level`` to librdkafka's required lowercase spelling."""
    value = raw.strip().lower()
    if value in ("read_uncommitted", "read_committed"):
        return value
    msg = f"'kafka.isolation.level' must be 'read_uncommitted' or 'read_committed', got {raw!r}"
    raise ValueError(msg)


def _build_consumer_config(client_config: dict[str, str]) -> dict[str, str | bool]:
    """Return the effective librdkafka config for a batch-read consumer.

    Pins ``isolation.level`` to ``read_uncommitted`` by default (matching the Java
    client Spark inherits rather than librdkafka's ``read_committed``), and applies
    source-owned settings last: ``auto.offset.reset=error`` keeps unserviceable
    offsets visible to the read loop's ``failOnDataLoss`` logic.
    """
    return {
        "group.id": _DEFAULT_GROUP_ID,
        "isolation.level": "read_uncommitted",
        **client_config,
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
        "enable.partition.eof": True,
    }


# ============================================================================
# Subscription strategy — assign / subscribe / subscribePattern
# ============================================================================


def _parse_subscription(opts: Mapping[str, str]) -> tuple[str, object]:
    """Parse Spark's mutually-exclusive ``assign`` / ``subscribe`` / ``subscribePattern`` option."""
    present = [key for key in _SUBSCRIPTION_KEYS if key in opts]
    if len(present) > 1:
        msg = f"Only one of the following options may be specified: {', '.join(_SUBSCRIPTION_KEYS)}. Got: {', '.join(present)}"
        raise ValueError(msg)
    if not present:
        msg = (
            f"One of the following options must be specified for the kafka data source: {', '.join(_SUBSCRIPTION_KEYS)}"
        )
        raise ValueError(msg)

    strategy = present[0]
    raw = opts[strategy]

    if strategy == "subscribe":
        # Dedupe preserving order so repeated topics are not scanned twice.
        topics = list(dict.fromkeys(t.strip() for t in raw.split(",") if t.strip()))
        if not topics:
            msg = "Option 'subscribe' must list at least one topic"
            raise ValueError(msg)
        return strategy, topics

    if strategy == "subscribePattern":
        pattern = raw.strip()
        if not pattern:
            msg = "Option 'subscribePattern' must be a non-empty regular expression"
            raise ValueError(msg)
        try:
            return strategy, re.compile(pattern)
        except re.error as e:
            msg = f"Option 'subscribePattern' is not a valid regular expression: {pattern!r} ({e})"
            raise ValueError(msg) from e

    # Like Spark's JsonUtils.partitions, accept empty maps/lists and nonexistent
    # int32 ids; _list_topic_partitions intersects them with broker metadata.
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        msg = f"'assign' JSON must be an object of {{topic: [partition, ...]}}, got: {raw!r}"
        raise ValueError(msg)  # noqa: TRY004
    for topic, partitions in parsed.items():
        if not isinstance(partitions, list):
            msg = f"'assign' entry for topic {topic!r} must be an array of partition ids"
            raise ValueError(msg)  # noqa: TRY004
        for partition in partitions:
            if not isinstance(partition, int) or isinstance(partition, bool):
                msg = f"'assign' partition id for topic {topic!r} must be an integer, got: {partition!r}"
                raise ValueError(msg)  # noqa: TRY004
            _check_integer_width(partition, bits=32, label=f"'assign' partition id for topic {topic!r}")
    return strategy, parsed


# ============================================================================
# InputPartition — one per (topic, partition, offset-range)
# ============================================================================


class KafkaInputPartition(InputPartition):
    def __init__(
        self,
        partition_id: int,
        *,
        topic: str,
        kafka_partition: int,
        start_offset: int,
        end_offset: int,
    ) -> None:
        super().__init__(partition_id)
        self.topic = topic
        self.kafka_partition = kafka_partition
        self.start_offset = start_offset
        self.end_offset = end_offset  # exclusive

    def __repr__(self) -> str:
        return (
            f"KafkaInputPartition(topic={self.topic!r}, kafka_partition={self.kafka_partition}, "
            f"start_offset={self.start_offset}, end_offset={self.end_offset})"
        )


# ============================================================================
# Offset range splitting — minPartitions / maxRecordsPerPartition
# ============================================================================


def _divide_range(start: int, end: int, parts: int) -> list[tuple[int, int]]:
    """Split ``[start, end)`` into ``parts`` chunks, placing remainders last.

    Mirrors ``KafkaOffsetRangeCalculator.getDividedPartition`` (10 over 3 -> 3/3/4)
    and drops empty chunks.
    """
    remaining = end - start
    # Clamp parts to skip empty leading iterations when parts > remaining.
    parts = min(max(parts, 1), max(remaining, 1))
    out: list[tuple[int, int]] = []
    offset = start
    for i in range(parts):
        length = remaining // (parts - i)
        remaining -= length
        chunk_end = min(offset + length, end)
        if chunk_end > offset:
            out.append((offset, chunk_end))
        offset = chunk_end
    return out


def _check_range(rng: tuple[str, int, int, int]) -> tuple[str, int, int, int] | None:
    """Reject an inverted concrete range and return ``None`` for an empty one."""
    topic, partition, start, end = rng
    if end < start:
        msg = (
            f"Inverted offset range for {topic}[{partition}]: start {start} is past end {end}. "
            f"Check 'startingOffsets'/'endingOffsets'; the log may also have been truncated "
            f"while the range was being resolved."
        )
        raise ValueError(msg)
    return None if end == start else rng


def _resolve_sentinels(
    admin: AdminClient,
    ranges: list[tuple[str, int, int, int]],
    *,
    timeout_s: float,
    isolation_level: IsolationLevel,
) -> list[tuple[str, int, int, int]]:
    """Resolve ``earliest``/``latest`` sentinels with at most one ``list_offsets`` call per endpoint."""
    need_low = {(topic, part) for topic, part, start, _ in ranges if start == _SPARK_OFFSET_EARLIEST}
    need_high = {(topic, part) for topic, part, _, end in ranges if end == _SPARK_OFFSET_LATEST}
    if not need_low and not need_high:
        return ranges

    lows = _list_offsets(
        admin, {k: OffsetSpec.earliest() for k in need_low}, timeout_s=timeout_s, isolation_level=isolation_level
    )
    highs = _list_offsets(
        admin, {k: OffsetSpec.latest() for k in need_high}, timeout_s=timeout_s, isolation_level=isolation_level
    )
    out = []
    for topic, partition, start, end in ranges:
        key = (topic, partition)
        bound_start = lows[key] if start == _SPARK_OFFSET_EARLIEST else start
        bound_end = highs[key] if end == _SPARK_OFFSET_LATEST else end
        out.append((topic, partition, bound_start, bound_end))
    return out


def _restore_outer_bounds(
    ranges: list[tuple[str, int, int, int]], outer: dict[tuple[str, int], tuple[int, int]]
) -> list[tuple[str, int, int, int]]:
    """Restore each partition's original endpoints onto its first and last chunk.

    Mirrors ``getOffsetRangesFromUnresolvedOffsets``: only interior split points
    are fixed; late-bound outer sentinels stay late-bound.
    """
    first: dict[tuple[str, int], int] = {}
    last: dict[tuple[str, int], int] = {}
    for i, (topic, partition, _, _) in enumerate(ranges):
        first.setdefault((topic, partition), i)
        last[(topic, partition)] = i

    out = list(ranges)
    for key, i in first.items():
        topic, partition, _, end = out[i]
        out[i] = (topic, partition, outer[key][0], end)
    for key, i in last.items():
        topic, partition, start, _ = out[i]
        out[i] = (topic, partition, start, outer[key][1])
    return out


def _part_count(size: int, total_size: int, min_parts: int) -> int:
    """Match Scala's ``math.round`` (``floor(x + 0.5)``), not Python's banker's rounding."""
    return max(math.floor(size / total_size * min_parts + 0.5), 1)


def _split_offset_ranges(
    ranges: list[tuple[str, int, int, int]],
    *,
    min_partitions: int | None,
    max_records_per_partition: int | None,
) -> list[tuple[str, int, int, int]]:
    """Apply Spark's ``maxRecordsPerPartition`` then ``minPartitions`` splitting."""
    if max_records_per_partition is not None:
        divided: list[tuple[str, int, int, int]] = []
        for topic, partition, start, end in ranges:
            parts = (end - start + max_records_per_partition - 1) // max_records_per_partition
            divided.extend(
                (topic, partition, chunk_start, chunk_end)
                for chunk_start, chunk_end in _divide_range(start, end, parts)
            )
        ranges = divided

    if min_partitions is None or min_partitions <= len(ranges):
        return ranges

    total_size = sum(end - start for _, _, start, end in ranges)
    if total_size == 0:
        return ranges

    # Two-pass split matching Spark's unsplitRangeTopicPartitions: set aside ranges
    # rounding to 1 chunk (keyed by (topic, partition)), then allocate the remaining
    # minPartitions budget across the rest so small ranges do not inflate the total.
    unsplit = [r for r in ranges if _part_count(r[3] - r[2], total_size, min_partitions) == 1]
    unsplit_tps = {(topic, partition) for topic, partition, _, _ in unsplit}
    split_total_size = total_size - sum(end - start for _, _, start, end in unsplit)
    split_min_partitions = max(min_partitions - len(unsplit), 1)

    out: list[tuple[str, int, int, int]] = []
    for topic, partition, start, end in ranges:
        parts = (
            1 if (topic, partition) in unsplit_tps else _part_count(end - start, split_total_size, split_min_partitions)
        )
        out.extend(
            (topic, partition, chunk_start, chunk_end) for chunk_start, chunk_end in _divide_range(start, end, parts)
        )
    return out


# ============================================================================
# DataSourceReader
# ============================================================================


@dataclass
class _ReadState:
    next_offset: int
    last_progress: float
    skipped_ranges: int = 0
    first_skipped: int | None = None
    last_skipped: int | None = None
    pending_gaps: list[tuple[int, int]] = field(default_factory=list)

    def skip(self, start: int, end: int) -> None:
        if end <= start:
            return
        self.skipped_ranges += 1
        self.first_skipped = start if self.first_skipped is None else min(self.first_skipped, start)
        self.last_skipped = end if self.last_skipped is None else max(self.last_skipped, end)


class KafkaDataSourceReader(DataSourceReader):
    def __init__(
        self,
        *,
        subscription: tuple[str, object],
        starting_offsets: str | dict[str, dict[str, int]] | None,
        ending_offsets: str | dict[str, dict[str, int]] | None,
        starting_timestamps: int | dict[str, dict[str, int]] | None,
        ending_timestamps: int | dict[str, dict[str, int]] | None,
        starting_ts_strategy: str,
        client_config: dict[str, str],
        include_headers: bool,
        poll_timeout_s: float,
        max_batch_rows: int,
        max_batch_bytes: int,
        max_batch_wait_s: float,
        stall_timeout_s: float,
        admin_timeout_s: float,
        fail_on_data_loss: bool,
        min_partitions: int | None,
        max_records_per_partition: int | None,
    ) -> None:
        self.subscription = subscription
        self.starting_offsets = starting_offsets
        self.ending_offsets = ending_offsets
        self.starting_timestamps = starting_timestamps
        self.ending_timestamps = ending_timestamps
        self.starting_ts_strategy = starting_ts_strategy
        self.client_config = client_config
        self.include_headers = include_headers
        self.poll_timeout_s = poll_timeout_s
        self.max_batch_rows = max_batch_rows
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_wait_s = max_batch_wait_s
        self.stall_timeout_s = stall_timeout_s
        self.admin_timeout_s = admin_timeout_s
        self.fail_on_data_loss = fail_on_data_loss
        self.min_partitions = min_partitions
        self.max_records_per_partition = max_records_per_partition

    # ------------------------------------------------------------------
    # Partition planning — talks to the broker once on the driver
    # ------------------------------------------------------------------

    def partitions(self) -> list[InputPartition]:
        # Short-lived AdminClient matching Spark's KafkaOffsetReaderAdmin (exposes no close()).
        # earliest/latest remain -2/-1 sentinels until each task binds them in read().
        admin = AdminClient(_build_admin_config(self.client_config))
        admin_timeout_s = self.admin_timeout_s
        isolation_level = _planning_isolation_level(self.client_config)

        tps = _list_topic_partitions(admin, self.subscription, timeout_s=admin_timeout_s)
        for spec, option_name in (
            (self.starting_offsets, "startingOffsets"),
            (self.ending_offsets, "endingOffsets"),
            (self.starting_timestamps, "startingOffsetsByTimestamp"),
            (self.ending_timestamps, "endingOffsetsByTimestamp"),
        ):
            _validate_spec_partitions(spec, tps, option_name=option_name)

        if self.starting_timestamps is not None:
            starts = _resolve_by_timestamp(
                admin,
                tps,
                self.starting_timestamps,
                endpoint="starting",
                timeout_s=admin_timeout_s,
                strategy=self.starting_ts_strategy,
                isolation_level=isolation_level,
            )
        else:
            starts = _map_offset_spec(tps, self.starting_offsets or _SENTINEL_EARLIEST)
        if self.ending_timestamps is not None:
            ends = _resolve_by_timestamp(
                admin,
                tps,
                self.ending_timestamps,
                endpoint="ending",
                timeout_s=admin_timeout_s,
                strategy=_TS_STRATEGY_LATEST,
                isolation_level=isolation_level,
            )
        else:
            ends = _map_offset_spec(tps, self.ending_offsets or _SENTINEL_LATEST)

        ranges = [
            (tp.topic, tp.partition, starts[(tp.topic, tp.partition)], ends[(tp.topic, tp.partition)]) for tp in tps
        ]

        # Fail fast on concrete inverted ranges; empty ranges stay on the unsplit path.
        for rng in ranges:
            if rng[2] >= 0 and rng[3] >= 0:
                _check_range(rng)

        # Match Spark 4.1 shouldDivvyUpLargePartitions (tests unresolved sizes), then
        # resolve only to place interior split points and restore outer sentinels.
        needs_split = (
            self.max_records_per_partition is not None
            and any(end - start > self.max_records_per_partition for _, _, start, end in ranges)
        ) or (self.min_partitions is not None and self.min_partitions > len(ranges))
        if needs_split:
            outer = {(topic, part): (start, end) for topic, part, start, end in ranges}
            ranges = _resolve_sentinels(admin, ranges, timeout_s=admin_timeout_s, isolation_level=isolation_level)
            ranges = [r for r in (_check_range(r) for r in ranges) if r is not None]
            ranges = _split_offset_ranges(
                ranges,
                min_partitions=self.min_partitions,
                max_records_per_partition=self.max_records_per_partition,
            )
            ranges = _restore_outer_bounds(ranges, outer)

        return [
            KafkaInputPartition(
                pid,
                topic=topic,
                kafka_partition=partition,
                start_offset=start,
                end_offset=end,
            )
            for pid, (topic, partition, start, end) in enumerate(ranges)
        ]

    # ------------------------------------------------------------------
    # Per-partition read — runs on executors
    # ------------------------------------------------------------------

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        if not isinstance(partition, KafkaInputPartition):
            msg = f"Expected KafkaInputPartition, got {type(partition)}"
            raise TypeError(msg)

        schema = _build_schema(include_headers=self.include_headers)
        consumer = Consumer(_build_consumer_config(self.client_config))
        state = None
        try:
            start, end = _bind_partition_range(consumer, partition, timeout_s=self.admin_timeout_s)
            if _check_range((partition.topic, partition.kafka_partition, start, end)) is None:
                return

            tp = TopicPartition(partition.topic, partition.kafka_partition, start)
            consumer.assign([tp])

            buf = _RowBuffer(include_headers=self.include_headers)
            state = _ReadState(next_offset=start, last_progress=time.monotonic())
            batch_started = state.last_progress

            def flush() -> Iterator[pa.RecordBatch]:
                _report_pending_gaps(consumer, tp, state, timeout_s=self.admin_timeout_s)
                batch = buf.to_batch(schema)
                paused = time.monotonic()
                yield batch
                # Downstream backpressure is not broker idle time.
                state.last_progress += time.monotonic() - paused

            while state.next_offset < end:
                now = time.monotonic()
                if len(buf) and now - batch_started >= self.max_batch_wait_s:
                    yield from flush()
                    batch_started = time.monotonic()
                    continue
                _check_stall(
                    tp,
                    state.last_progress,
                    stall_timeout_s=self.stall_timeout_s,
                    next_offset=state.next_offset,
                    end_offset=end,
                )
                remaining = self.stall_timeout_s - (now - state.last_progress)
                timeout = min(self.poll_timeout_s, _POLL_SLICE_S, remaining)
                if len(buf):
                    timeout = min(timeout, self.max_batch_wait_s - (now - batch_started))
                record = consumer.poll(max(timeout, 0.0))
                if record is None:
                    _check_stall(
                        tp,
                        state.last_progress,
                        stall_timeout_s=self.stall_timeout_s,
                        next_offset=state.next_offset,
                        end_offset=end,
                    )
                    continue

                error = record.error()
                if error is not None:
                    code = error.code()
                    if code == KafkaError._PARTITION_EOF:  # noqa: SLF001
                        if _eof_is_terminal(
                            consumer,
                            tp,
                            fail_on_data_loss=self.fail_on_data_loss,
                            admin_timeout_s=self.admin_timeout_s,
                            message_offset=record.offset(),
                            next_offset=state.next_offset,
                            end_offset=end,
                            state=state,
                        ):
                            break
                        # Stale EOF the log has grown past; it can still advance past control records.
                        if record.offset() > state.next_offset:
                            state.next_offset = record.offset()
                            state.last_progress = time.monotonic()
                        _check_stall(
                            tp,
                            state.last_progress,
                            stall_timeout_s=self.stall_timeout_s,
                            next_offset=state.next_offset,
                            end_offset=end,
                        )
                        continue
                    if code in _OUT_OF_RANGE_CODES:
                        recovered = _recover_out_of_range(
                            consumer,
                            tp,
                            fail_on_data_loss=self.fail_on_data_loss,
                            admin_timeout_s=self.admin_timeout_s,
                            next_offset=state.next_offset,
                            end_offset=end,
                        )
                        if recovered is None:
                            state.skip(state.next_offset, end)
                            break
                        state.skip(state.next_offset, recovered)
                        if recovered > state.next_offset:
                            state.last_progress = time.monotonic()
                        state.next_offset = recovered
                        _check_stall(
                            tp,
                            state.last_progress,
                            stall_timeout_s=self.stall_timeout_s,
                            next_offset=state.next_offset,
                            end_offset=end,
                        )
                        continue
                    raise KafkaException(error)

                state.last_progress = time.monotonic()

                record_offset = record.offset()
                if record_offset < state.next_offset:
                    msg = (
                        f"Kafka returned offset {record_offset} for {partition.topic}"
                        f"[{partition.kafka_partition}] while offset {state.next_offset} was requested"
                    )
                    raise ValueError(msg)
                if record_offset > state.next_offset:
                    if self.fail_on_data_loss:
                        _check_offset_gap(
                            consumer,
                            tp,
                            admin_timeout_s=self.admin_timeout_s,
                            next_offset=state.next_offset,
                            record_offset=record_offset,
                            end_offset=end,
                        )
                    else:
                        # Permissive reads only need watermarks for warning diagnostics; defer per batch.
                        state.pending_gaps.append((state.next_offset, min(record_offset, end)))
                if record_offset >= end:
                    break

                record_bytes = _record_size(record, include_headers=self.include_headers)
                if len(buf) and buf.size_bytes + record_bytes > self.max_batch_bytes:
                    yield from flush()
                if not len(buf):
                    batch_started = time.monotonic()
                buf.append(record, record_bytes)
                state.next_offset = record_offset + 1

                if len(buf) >= self.max_batch_rows or buf.size_bytes >= self.max_batch_bytes:
                    yield from flush()
                    batch_started = time.monotonic()

            _report_pending_gaps(consumer, tp, state, timeout_s=self.admin_timeout_s)
            if len(buf) > 0:
                yield buf.to_batch(schema)
        finally:
            if state is not None and state.skipped_ranges:
                _LOGGER.warning(
                    "Kafka partial read on %s[%s]: skipped %s offset interval(s), spanning [%s, %s); "
                    "failOnDataLoss=false (offset spans are not counts of lost records)",
                    partition.topic,
                    partition.kafka_partition,
                    state.skipped_ranges,
                    state.first_skipped,
                    state.last_skipped,
                )
            unwinding = sys.exc_info()[0] is not None
            try:
                consumer.close()
            except Exception:
                # Omit client exception text (may contain security config) and do not mask read errors.
                _LOGGER.warning("Failed to close Kafka consumer for %s[%s]", partition.topic, partition.kafka_partition)
                if not unwinding:
                    raise


def _query_watermarks(consumer: Consumer, tp: TopicPartition, *, timeout_s: float) -> tuple[int, int]:
    """Query fresh watermarks, adding task context to client timeouts."""
    try:
        return consumer.get_watermark_offsets(tp, timeout=timeout_s, cached=False)
    except KafkaException as e:
        if e.args[0].code() != KafkaError._TIMED_OUT:  # noqa: SLF001
            raise
        msg = (
            f"Timed out querying offsets for {tp.topic}[{tp.partition}] after "
            f"{timeout_s:g}s; check broker connectivity or raise 'adminTimeoutMs'."
        )
        raise TimeoutError(msg) from e


def _report_pending_gaps(consumer: Consumer, tp: TopicPartition, state: _ReadState, *, timeout_s: float) -> None:
    """Classify permissive-read gaps once per batch for the final partial-read warning."""
    if not state.pending_gaps:
        return
    low, _ = _query_watermarks(consumer, tp, timeout_s=timeout_s)
    for start, end in state.pending_gaps:
        if low > start:
            state.skip(start, end)
    state.pending_gaps.clear()


def _bind_partition_range(
    consumer: Consumer, partition: KafkaInputPartition, *, timeout_s: float = 10.0
) -> tuple[int, int]:
    """Late-bind ``earliest``/``latest`` sentinels at task start, mirroring ``KafkaSourceRDD.resolveRange``."""
    start, end = partition.start_offset, partition.end_offset
    if start >= 0 and end >= 0:
        return start, end
    low, high = _query_watermarks(
        consumer, TopicPartition(partition.topic, partition.kafka_partition), timeout_s=timeout_s
    )
    if start == _SPARK_OFFSET_EARLIEST:
        start = low
    if end == _SPARK_OFFSET_LATEST:
        end = high
    return start, end


def _check_stall(
    tp: TopicPartition,
    last_progress: float,
    *,
    stall_timeout_s: float,
    next_offset: int,
    end_offset: int,
) -> None:
    """Fail a read that has produced nothing for longer than ``stallTimeoutMs``."""
    idle_s = time.monotonic() - last_progress
    if idle_s < stall_timeout_s:
        return
    msg = (
        f"Kafka read stalled: no messages for {idle_s:.0f}s on "
        f"{tp.topic}[{tp.partition}] at offset "
        f"{next_offset}, expected up to {end_offset}. Broker may be "
        f"unreachable or the partition leader unavailable. "
        f"Raise 'stallTimeoutMs' if the topic is simply idle."
    )
    raise TimeoutError(msg)


def _recover_out_of_range(
    consumer: Consumer,
    tp: TopicPartition,
    *,
    fail_on_data_loss: bool,
    admin_timeout_s: float,
    next_offset: int,
    end_offset: int,
) -> int | None:
    """Handle an out-of-range fetch: fail under ``failOnDataLoss``, else clamp to the available overlap."""
    if fail_on_data_loss:
        msg = (
            f"Data loss on {tp.topic}[{tp.partition}]: offset {next_offset} is no "
            f"longer available on the broker. Records were removed after planning (retention or "
            f"truncation), or the offset is past the end of the log. "
            f"Set 'failOnDataLoss' to false to skip missing records."
        )
        raise ValueError(msg)
    low, high = _query_watermarks(consumer, tp, timeout_s=admin_timeout_s)
    if next_offset >= high or low >= end_offset:
        return None
    recovered = max(next_offset, low)
    consumer.seek(TopicPartition(tp.topic, tp.partition, recovered))
    return recovered


def _check_offset_gap(
    consumer: Consumer,
    tp: TopicPartition,
    *,
    admin_timeout_s: float,
    next_offset: int,
    record_offset: int,
    end_offset: int,
) -> int:
    """Distinguish unreadable in-log gaps (compaction/control markers) from retention data loss.

    Always queries fresh watermarks because the log start can advance between gaps.
    """
    low, _ = _query_watermarks(consumer, tp, timeout_s=admin_timeout_s)
    if low <= next_offset:
        return low
    msg = (
        f"Data loss on {tp.topic}[{tp.partition}]: offsets {next_offset} to "
        f"{min(record_offset, end_offset) - 1} were requested but are no longer in the log, which now starts at "
        f"{low}. Records were removed after planning (retention or truncation). "
        f"Set 'failOnDataLoss' to false to skip missing records."
    )
    raise ValueError(msg)


def _eof_is_terminal(
    consumer: Consumer,
    tp: TopicPartition,
    *,
    fail_on_data_loss: bool,
    admin_timeout_s: float,
    message_offset: int | None,
    next_offset: int,
    end_offset: int,
    state: _ReadState,
) -> bool:
    """Classify a ``_PARTITION_EOF`` event as complete, stale (log grew past it), or data loss."""
    eof_offset = message_offset if message_offset is not None and message_offset >= 0 else None
    position = next_offset if eof_offset is None else eof_offset
    if position >= end_offset:
        return True
    _, high = _query_watermarks(consumer, tp, timeout_s=admin_timeout_s)
    if eof_offset is not None and high > eof_offset:
        return False
    if high >= end_offset:
        return True
    if not fail_on_data_loss:
        state.skip(next_offset, end_offset)
        return True
    msg = (
        f"Data loss on {tp.topic}[{tp.partition}]: reached end of log at offset "
        f"{next_offset}, but the read was planned up to {end_offset} and the high watermark "
        f"is now {high}. Records were removed after planning (retention or truncation), or "
        f"'endingOffsets' points past the end of the log. "
        f"Set 'failOnDataLoss' to false to return the records that remain."
    )
    raise ValueError(msg)


# ============================================================================
# Row buffer — accumulates messages, flushes to a RecordBatch
# ============================================================================


def _record_size(record, *, include_headers: bool) -> int:
    """Conservative Arrow payload estimate, not a process-memory measurement."""
    size = 64 + len(record.key() or b"") + len(record.value() or b"") + len(record.topic().encode("utf-8"))
    if include_headers:
        size += sum(16 + len(key.encode("utf-8")) + len(value or b"") for key, value in record.headers() or [])
    return size


class _RowBuffer:
    """Column-major accumulator sized to ``max_batch_rows``."""

    def __init__(self, *, include_headers: bool) -> None:
        self.include_headers = include_headers
        self.clear()

    def clear(self) -> None:
        self.size_bytes = 0
        self.key: list[bytes | None] = []
        self.value: list[bytes | None] = []
        self.topic: list[str] = []
        self.partition: list[int] = []
        self.offset: list[int] = []
        self.timestamp_us: list[int] = []
        self.timestamp_type: list[int] = []
        self.headers: list[list[dict] | None] = []

    def __len__(self) -> int:
        return len(self.offset)

    def append(self, record, size_bytes: int) -> None:
        self.size_bytes += size_bytes
        ts_type, ts_ms = record.timestamp()  # librdkafka: 0=NotAvailable, 1=Create, 2=LogAppend
        self.key.append(record.key())
        self.value.append(record.value())
        self.topic.append(record.topic())
        self.partition.append(record.partition())
        self.offset.append(record.offset())
        # Spark surfaces -1ms (1969-12-31 23:59:59.999) for untimed records rather than null.
        if ts_type == _LIBRDKAFKA_TS_NOT_AVAILABLE:
            ts_ms = _NO_TIMESTAMP_MS
        self.timestamp_us.append(ts_ms * 1000)
        self.timestamp_type.append(_LIBRDKAFKA_TO_SPARK_TS_TYPE.get(ts_type, -1))
        if self.include_headers:
            hdrs = record.headers()
            self.headers.append([{"key": k, "value": v} for k, v in hdrs] if hdrs else None)

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
            arrays.append(pa.array(self.headers, type=pa.list_(_HEADER_STRUCT)))
        batch = pa.record_batch(arrays, schema=schema)
        # Release Python payloads before yielding across backpressure boundaries.
        self.clear()
        return batch


# ============================================================================
# Broker helpers — enumerate partitions, resolve watermarks
# ============================================================================


# Wall-clock cushion over librdkafka's request_timeout so an unresolved future cannot hang planning.
_FUTURE_TIMEOUT_MARGIN_S = 5.0


def _await(future, *, timeout_s: float, what: str):
    """Resolve an admin-call future with a hard wall-clock bound."""
    try:
        return future.result(timeout=timeout_s + _FUTURE_TIMEOUT_MARGIN_S)
    except FuturesTimeoutError as e:
        msg = (
            f"Timed out after {timeout_s:g}s waiting for the broker to {what}. "
            f"The broker may be unreachable or a partition leader unavailable; "
            f"raise 'adminTimeoutMs' if the cluster is simply slow."
        )
        raise TimeoutError(msg) from e


def _describe_topics(admin: AdminClient, topics: list[str], *, timeout_s: float) -> dict[str, list[int]]:
    """Describe ``topics`` in one request, excluding broker-flagged ``isInternal`` topics."""
    futures = admin.describe_topics(TopicCollection(topics), request_timeout=timeout_s)
    out: dict[str, list[int]] = {}
    for topic, future in futures.items():
        try:
            description = _await(future, timeout_s=timeout_s, what=f"describe topic {topic!r}")
        except KafkaException as e:
            error = e.args[0]
            if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                msg = f"Topic {topic!r} not found on broker"
            else:
                msg = f"Topic {topic!r} is not available: {error}"
            raise ValueError(msg) from e
        if description.is_internal:
            continue
        out[topic] = sorted(p.id for p in description.partitions)
    return out


def _list_topic_partitions(
    admin: AdminClient, subscription: tuple[str, object], *, timeout_s: float
) -> list[TopicPartition]:
    """Enumerate the non-internal (topic, partition) pairs selected by the subscription."""
    strategy, value = subscription

    if strategy == "subscribePattern":
        metadata = admin.list_topics(timeout=timeout_s)
        # Unanchored search matches Spark's SubscribePatternStrategy (findFirstIn).
        names = sorted(topic for topic in metadata.topics if value.search(topic))
        if not names:
            return []
        described = _describe_topics(admin, names, timeout_s=timeout_s)
        return [TopicPartition(topic, p) for topic in sorted(described) for p in described[topic]]

    topics = list(value) if strategy == "subscribe" else [topic for topic, parts in value.items() if parts]
    if not topics:
        return []
    described = _describe_topics(admin, topics, timeout_s=timeout_s)

    out: list[TopicPartition] = []
    for topic in topics:
        if topic not in described:
            continue  # broker-flagged internal topic
        available = described[topic]
        if strategy == "subscribe":
            out.extend(TopicPartition(topic, p) for p in available)
            continue
        # Intersect requested partitions with broker metadata, skipping nonexistent ids as Spark does.
        selected = set(value[topic])
        out.extend(TopicPartition(topic, p) for p in available if p in selected)
    return out


def _validate_spec_partitions(
    spec: str | int | dict[str, dict[str, int]] | None,
    tps: list[TopicPartition],
    *,
    option_name: str,
) -> None:
    """Require a per-partition offset/timestamp map to match the assigned partitions exactly."""
    if not isinstance(spec, dict):
        return
    specified = {(topic, int(partition)) for topic, partitions in spec.items() for partition in partitions}
    assigned = {(tp.topic, tp.partition) for tp in tps}
    if specified == assigned:
        return
    missing = sorted(assigned - specified)
    extra = sorted(specified - assigned)
    details = []
    if missing:
        details.append(f"missing: {missing}")
    if extra:
        details.append(f"not assigned: {extra}")
    msg = (
        f"'{option_name}' must specify all assigned TopicPartitions ({', '.join(details)}). "
        f"Use -1 for latest and -2 for earliest. "
        f"Specified: {sorted(specified)} Assigned: {sorted(assigned)}"
    )
    raise ValueError(msg)


def _list_offsets(
    admin: AdminClient,
    specs: dict[tuple[str, int], object],
    *,
    timeout_s: float,
    isolation_level: IsolationLevel,
) -> dict[tuple[str, int], int]:
    """Resolve ``{(topic, partition): OffsetSpec}`` in a single batched ``list_offsets`` call."""
    if not specs:
        return {}
    futures = admin.list_offsets(
        {TopicPartition(topic, partition): spec for (topic, partition), spec in specs.items()},
        isolation_level=isolation_level,
        request_timeout=timeout_s,
    )
    out: dict[tuple[str, int], int] = {}
    for tp, future in futures.items():
        try:
            result = _await(future, timeout_s=timeout_s, what=f"list offsets for {tp.topic}[{tp.partition}]")
            out[(tp.topic, tp.partition)] = result.offset
        except KafkaException as e:
            msg = f"Failed to look up offsets for {tp.topic}[{tp.partition}]: {e.args[0]}"
            raise ValueError(msg) from e
    return out


def _map_offset_spec(tps: list[TopicPartition], spec: str | dict[str, dict[str, int]]) -> dict[tuple[str, int], int]:
    """Map an offset spec to per-partition offsets, leaving ``-2``/``-1`` sentinels late-bound."""
    if spec == _SENTINEL_EARLIEST:
        return {(tp.topic, tp.partition): _SPARK_OFFSET_EARLIEST for tp in tps}
    if spec == _SENTINEL_LATEST:
        return {(tp.topic, tp.partition): _SPARK_OFFSET_LATEST for tp in tps}
    return {(tp.topic, tp.partition): int(spec[tp.topic][str(tp.partition)]) for tp in tps}


def _resolve_by_timestamp(
    admin: AdminClient,
    tps: list[TopicPartition],
    spec: int | dict[str, dict[str, int]],
    *,
    endpoint: str,
    timeout_s: float,
    strategy: str,
    isolation_level: IsolationLevel,
) -> dict[tuple[str, int], int]:
    """Resolve a global or per-partition timestamp spec via ``list_offsets``.

    Unmatched partitions (offset ``-1``) fall back to ``latest`` for ending
    timestamps, or follow ``startingOffsetsByTimestampStrategy`` for starting ones.
    """
    if isinstance(spec, int):
        wanted = {(tp.topic, tp.partition): OffsetSpec.for_timestamp(spec) for tp in tps}
    else:
        wanted = {
            (tp.topic, tp.partition): OffsetSpec.for_timestamp(int(spec[tp.topic][str(tp.partition)])) for tp in tps
        }

    resolved = _list_offsets(admin, wanted, timeout_s=timeout_s, isolation_level=isolation_level)
    unmatched = [tp for tp in tps if resolved.get((tp.topic, tp.partition), -1) < 0]
    if unmatched and endpoint == "starting" and strategy == _TS_STRATEGY_ERROR:
        tp = unmatched[0]
        msg = (
            f"No offset matches the requested timestamp for {tp.topic}[{tp.partition}]. "
            f"Set 'startingOffsetsByTimestampStrategy' to 'latest' to read from the end of "
            f"the log instead of failing."
        )
        raise ValueError(msg)
    if unmatched:
        fallback = _list_offsets(
            admin,
            {(tp.topic, tp.partition): OffsetSpec.latest() for tp in unmatched},
            timeout_s=timeout_s,
            isolation_level=isolation_level,
        )
        resolved.update(fallback)
    return resolved


# ============================================================================
# DataSource
# ============================================================================


class KafkaDataSource(DataSource):
    """Kafka batch data source backed by confluent-kafka.

    Each task reads a bounded ``[start, end)`` range. Numeric and timestamp
    endpoints are planned on the driver; ``earliest``/``latest`` bind on the
    worker. There is no streaming / continuous mode.

    Register and use::

        from pysail.spark.datasource.kafka import KafkaDataSource

        spark.dataSource.register(KafkaDataSource)

        df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "orders")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

    Options are case-insensitive. See the Kafka data source guide for the
    supported options, precedence, resource limits, and compatibility differences.
    Writes, streaming reads, and filter pushdown are not supported.
    """

    @classmethod
    def name(cls) -> str:
        return "kafka"

    @cached_property
    def _resolved(self) -> dict:
        # Re-wrap to preserve case-insensitivity when constructed directly.
        opts = CaseInsensitiveDict(self.options)

        # CaseInsensitiveDict yields lowercase keys, matching librdkafka property names.
        client_config = {k[len("kafka.") :]: v for k, v in opts.items() if k.startswith("kafka.")}
        if not client_config.get("bootstrap.servers", "").strip():
            msg = "Option 'kafka.bootstrap.servers' is required and must not be blank for the kafka data source"
            raise ValueError(msg)
        for name, alternative in _PROTECTED_KAFKA_CONFIGS.items():
            if name in client_config:
                msg = f"Kafka option 'kafka.{name}' is not supported"
                if alternative:
                    msg += f"; use the source option(s) '{alternative}' instead"
                raise ValueError(msg)
        if "isolation.level" in client_config:
            client_config["isolation.level"] = _canonical_isolation_level(client_config["isolation.level"])

        subscription = _parse_subscription(opts)
        starting_offsets, starting_ts = _resolve_range_option(opts, endpoint="starting")
        ending_offsets, ending_ts = _resolve_range_option(opts, endpoint="ending")

        ts_strategy_raw = opts.get("startingOffsetsByTimestampStrategy", _TS_STRATEGY_ERROR)
        starting_ts_strategy = ts_strategy_raw.strip().lower()
        if starting_ts_strategy not in (_TS_STRATEGY_ERROR, _TS_STRATEGY_LATEST):
            msg = f"'startingOffsetsByTimestampStrategy' must be {_TS_STRATEGY_ERROR!r} or {_TS_STRATEGY_LATEST!r}, got {ts_strategy_raw!r}"
            raise ValueError(msg)

        include_headers = _parse_bool(opts["includeHeaders"], "includeHeaders") if "includeHeaders" in opts else False
        fail_on_data_loss = _parse_bool(opts["failOnDataLoss"], "failOnDataLoss") if "failOnDataLoss" in opts else True
        max_batch_rows = _parse_positive_int(opts["maxBatchRows"], "maxBatchRows") if "maxBatchRows" in opts else 10000
        max_batch_bytes = (
            _parse_positive_int(opts["maxBatchBytes"], "maxBatchBytes", max_value=_INT32_MAX)
            if "maxBatchBytes" in opts
            else 8388608
        )
        max_batch_wait_s = (
            _parse_positive_ms(opts["maxBatchWaitMs"], "maxBatchWaitMs") if "maxBatchWaitMs" in opts else 0.25
        )
        min_partitions = (
            _parse_positive_int(opts["minPartitions"], "minPartitions", max_value=_INT32_MAX)
            if "minPartitions" in opts
            else None
        )
        max_records_per_partition = (
            _parse_positive_int(opts["maxRecordsPerPartition"], "maxRecordsPerPartition", max_value=_INT64_MAX)
            if "maxRecordsPerPartition" in opts
            else None
        )
        # Default matches Spark's fallback to spark.network.timeout (120s).
        poll_key = "kafkaConsumer.pollTimeoutMs" if "kafkaConsumer.pollTimeoutMs" in opts else "pollTimeoutMs"
        poll_timeout_s = _parse_positive_ms(opts[poll_key], poll_key) if poll_key in opts else 120.0
        stall_timeout_s = (
            _parse_positive_ms(opts["stallTimeoutMs"], "stallTimeoutMs") if "stallTimeoutMs" in opts else poll_timeout_s
        )
        # librdkafka admin request timeouts accept 1 ms to 1 hour.
        admin_timeout_s = (
            _parse_positive_ms(opts["adminTimeoutMs"], "adminTimeoutMs", max_value=3600000)
            if "adminTimeoutMs" in opts
            else 10.0
        )

        return {
            "subscription": subscription,
            "starting_offsets": starting_offsets,
            "ending_offsets": ending_offsets,
            "starting_timestamps": starting_ts,
            "ending_timestamps": ending_ts,
            "starting_ts_strategy": starting_ts_strategy,
            "client_config": client_config,
            "include_headers": include_headers,
            "poll_timeout_s": poll_timeout_s,
            "max_batch_rows": max_batch_rows,
            "max_batch_bytes": max_batch_bytes,
            "max_batch_wait_s": max_batch_wait_s,
            "stall_timeout_s": stall_timeout_s,
            "admin_timeout_s": admin_timeout_s,
            "fail_on_data_loss": fail_on_data_loss,
            "min_partitions": min_partitions,
            "max_records_per_partition": max_records_per_partition,
        }

    def schema(self) -> pa.Schema:
        return _build_schema(include_headers=self._resolved["include_headers"])

    def reader(self, schema: pa.Schema) -> KafkaDataSourceReader:
        # Kafka's schema is fixed and every field is nullable; reject any user-supplied
        # schema that changes names, types, or nullability (ignoring metadata).
        expected = _build_schema(include_headers=self._resolved["include_headers"])
        if schema is not None and not schema.equals(expected, check_metadata=False):
            msg = (
                f"The kafka data source has a fixed schema and does not accept a user-specified one.\n"
                f"Expected: {expected}\nGot: {schema}"
            )
            raise ValueError(msg)
        return KafkaDataSourceReader(**self._resolved)
