"""Topic discovery, offset resolution, and range splitting for the Kafka data source."""

from __future__ import annotations

import math
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, TypeVar

from confluent_kafka import KafkaError, KafkaException, TopicCollection, TopicPartition
from confluent_kafka.admin import OffsetSpec

from pysail.spark.datasource.kafka.options import (
    AssignPartitions,
    GlobalTimestamp,
    PartitionOffsets,
    PartitionTimestamps,
    SpecialOffset,
    SubscribePattern,
    SubscribeTopics,
    TimestampStrategy,
    TopicPartitionKey,
)

if TYPE_CHECKING:
    from concurrent.futures import Future

    from confluent_kafka import IsolationLevel
    from confluent_kafka.admin import AdminClient

    from pysail.spark.datasource.kafka.options import RangeBoundary, RangeLimit, Subscription


# ============================================================================
# Offset range splitting — minPartitions / maxRecordsPerPartition
# ============================================================================


@dataclass(frozen=True)
class OffsetRange:
    """A Kafka partition's half-open range, with ordering checked once both offsets are concrete."""

    topic_partition: TopicPartitionKey
    start: int
    end: int

    def __post_init__(self) -> None:
        """Validate ordering when both endpoints are concrete."""
        if self.start >= 0 and self.end >= 0 and self.end < self.start:
            msg = (
                f"Inverted offset range for {self.topic_partition.topic}[{self.topic_partition.partition}]: "
                f"start {self.start} is past end {self.end}. "
                f"Check 'startingOffsets'/'endingOffsets'; the log may also have been truncated "
                f"while the range was being resolved."
            )
            raise ValueError(msg)

    @property
    def size(self) -> int:
        return self.end - self.start


def divide_range(offset_range: OffsetRange, parts: int) -> list[OffsetRange]:
    """Split a concrete range into ``parts`` chunks, placing remainders last.

    Mirrors ``KafkaOffsetRangeCalculator.getDividedPartition`` (10 over 3 -> 3/3/4)
    and drops empty chunks.
    """
    remaining = offset_range.size
    # Clamp parts to skip empty leading iterations when parts > remaining.
    parts = min(max(parts, 1), max(remaining, 1))
    out: list[OffsetRange] = []
    offset = offset_range.start
    for i in range(parts):
        length = remaining // (parts - i)
        remaining -= length
        chunk_end = offset + length
        if chunk_end > offset:
            out.append(replace(offset_range, start=offset, end=chunk_end))
        offset = chunk_end
    return out


def resolve_sentinels(
    admin: AdminClient,
    offset_ranges: list[OffsetRange],
    *,
    timeout_s: float,
    isolation_level: IsolationLevel,
) -> list[OffsetRange]:
    """Resolve ``earliest``/``latest`` sentinels with at most one ``list_offsets`` call per endpoint."""
    need_low = {
        offset_range.topic_partition for offset_range in offset_ranges if offset_range.start == SpecialOffset.EARLIEST
    }
    need_high = {
        offset_range.topic_partition for offset_range in offset_ranges if offset_range.end == SpecialOffset.LATEST
    }
    if not need_low and not need_high:
        return offset_ranges

    lows = get_offsets_per_topic_partition(
        admin, {k: OffsetSpec.earliest() for k in need_low}, timeout_s=timeout_s, isolation_level=isolation_level
    )
    highs = get_offsets_per_topic_partition(
        admin, {k: OffsetSpec.latest() for k in need_high}, timeout_s=timeout_s, isolation_level=isolation_level
    )
    out = []
    for offset_range in offset_ranges:
        bound_start = (
            lows[offset_range.topic_partition] if offset_range.start == SpecialOffset.EARLIEST else offset_range.start
        )
        bound_end = (
            highs[offset_range.topic_partition] if offset_range.end == SpecialOffset.LATEST else offset_range.end
        )
        out.append(replace(offset_range, start=bound_start, end=bound_end))
    return out


def restore_outer_bounds(
    offset_ranges: list[OffsetRange], original_ranges: dict[TopicPartitionKey, OffsetRange]
) -> list[OffsetRange]:
    """Restore each partition's original endpoints onto its first and last chunk.

    Mirrors ``getOffsetRangesFromUnresolvedOffsets``: only interior split points
    are fixed; late-bound outer sentinels stay late-bound.
    """
    first: dict[TopicPartitionKey, int] = {}
    last: dict[TopicPartitionKey, int] = {}
    for i, offset_range in enumerate(offset_ranges):
        first.setdefault(offset_range.topic_partition, i)
        last[offset_range.topic_partition] = i

    out = list(offset_ranges)
    for key, i in first.items():
        out[i] = replace(out[i], start=original_ranges[key].start)
    for key, i in last.items():
        out[i] = replace(out[i], end=original_ranges[key].end)
    return out


def part_count(size: int, total_size: int, min_parts: int) -> int:
    """Match Scala's ``math.round`` (``floor(x + 0.5)``), not Python's banker's rounding."""
    return max(math.floor(size / total_size * min_parts + 0.5), 1)


def split_offset_ranges(
    offset_ranges: list[OffsetRange],
    *,
    min_partitions: int | None,
    max_records_per_partition: int | None,
) -> list[OffsetRange]:
    """Apply Spark's ``maxRecordsPerPartition`` then ``minPartitions`` splitting."""
    if max_records_per_partition is not None:
        divided: list[OffsetRange] = []
        for offset_range in offset_ranges:
            parts = (offset_range.size + max_records_per_partition - 1) // max_records_per_partition
            divided.extend(divide_range(offset_range, parts))
        offset_ranges = divided

    if min_partitions is None or min_partitions <= len(offset_ranges):
        return offset_ranges

    total_size = sum(offset_range.size for offset_range in offset_ranges)
    if total_size == 0:
        return offset_ranges

    # Two-pass split matching Spark's unsplitRangeTopicPartitions: set aside ranges
    # rounding to 1 chunk (keyed by topic/partition), then allocate the remaining
    # minPartitions budget across the rest so small ranges do not inflate the total.
    unsplit = [
        offset_range for offset_range in offset_ranges if part_count(offset_range.size, total_size, min_partitions) == 1
    ]
    unsplit_keys = {offset_range.topic_partition for offset_range in unsplit}
    split_total_size = total_size - sum(offset_range.size for offset_range in unsplit)
    split_min_partitions = max(min_partitions - len(unsplit), 1)

    out: list[OffsetRange] = []
    for offset_range in offset_ranges:
        parts = (
            1
            if offset_range.topic_partition in unsplit_keys
            else part_count(offset_range.size, split_total_size, split_min_partitions)
        )
        out.extend(divide_range(offset_range, parts))
    return out


# ============================================================================
# Broker helpers — enumerate partitions, resolve watermarks
# ============================================================================


# Wall-clock cushion over librdkafka's request_timeout so an unresolved future cannot hang planning.
FUTURE_TIMEOUT_MARGIN_S = 5.0


_Result = TypeVar("_Result")


def await_future(future: Future[_Result], *, timeout_s: float, what: str) -> _Result:
    """Resolve an admin-call future with a hard wall-clock bound."""
    try:
        return future.result(timeout=timeout_s + FUTURE_TIMEOUT_MARGIN_S)
    except FuturesTimeoutError as e:
        msg = (
            f"Timed out after {timeout_s:g}s waiting for the broker to {what}. "
            f"The broker may be unreachable or a partition leader unavailable; "
            f"raise 'adminTimeoutMs' if the cluster is simply slow."
        )
        raise TimeoutError(msg) from e


def describe_topics(admin: AdminClient, topics: list[str], *, timeout_s: float) -> dict[str, list[int]]:
    """Describe ``topics`` in one request, excluding broker-flagged ``isInternal`` topics."""
    futures = admin.describe_topics(TopicCollection(topics), request_timeout=timeout_s)
    out: dict[str, list[int]] = {}
    for topic, future in futures.items():
        try:
            description = await_future(future, timeout_s=timeout_s, what=f"describe topic {topic!r}")
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


def list_topic_partitions(
    admin: AdminClient, subscription: Subscription, *, timeout_s: float
) -> list[TopicPartitionKey]:
    """Enumerate the non-internal (topic, partition) pairs selected by the subscription."""
    match subscription:
        case SubscribeTopics(topics):
            pass
        case SubscribePattern(pattern):
            # Unanchored search matches Spark's SubscribePatternStrategy (findFirstIn).
            topics = sorted(t for t in admin.list_topics(timeout=timeout_s).topics if pattern.search(t))
        case AssignPartitions(partitions):
            topics = sorted({tp.topic for tp in partitions})

    if not topics:
        return []
    described = describe_topics(admin, topics, timeout_s=timeout_s)  # omits broker-flagged internal topics
    keys = [TopicPartitionKey(topic, p) for topic in topics if topic in described for p in described[topic]]
    if isinstance(subscription, AssignPartitions):
        # Skip assigned ids the broker doesn't have, as Spark does.
        keys = [key for key in keys if key in subscription.partitions]
    return keys


def validate_limit_partitions(
    limit: RangeLimit, topic_partitions: list[TopicPartitionKey], *, boundary: RangeBoundary
) -> None:
    """Require a per-partition offset/timestamp map to match the assigned partitions exactly."""
    match limit:
        case PartitionOffsets(offsets):
            specified = set(offsets)
            key = f"{boundary}Offsets"
        case PartitionTimestamps(timestamps):
            specified = set(timestamps)
            key = f"{boundary}OffsetsByTimestamp"
        case _:
            return
    assigned = set(topic_partitions)
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
        f"'{key}' must specify all assigned TopicPartitions ({', '.join(details)}). "
        f"Use -1 for latest and -2 for earliest. "
        f"Specified: {sorted(specified)} Assigned: {sorted(assigned)}"
    )
    raise ValueError(msg)


def get_offsets_per_topic_partition(
    admin: AdminClient,
    specs: dict[TopicPartitionKey, OffsetSpec],
    *,
    timeout_s: float,
    isolation_level: IsolationLevel,
) -> dict[TopicPartitionKey, int]:
    """Resolve ``{TopicPartitionKey: OffsetSpec}`` in a single batched ``list_offsets`` call."""
    if not specs:
        return {}
    futures = admin.list_offsets(
        {TopicPartition(key.topic, key.partition): spec for key, spec in specs.items()},
        isolation_level=isolation_level,
        request_timeout=timeout_s,
    )
    out: dict[TopicPartitionKey, int] = {}
    for tp, future in futures.items():
        try:
            result = await_future(future, timeout_s=timeout_s, what=f"list offsets for {tp.topic}[{tp.partition}]")
            out[TopicPartitionKey(tp.topic, tp.partition)] = result.offset
        except KafkaException as e:
            msg = f"Failed to look up offsets for {tp.topic}[{tp.partition}]: {e.args[0]}"
            raise ValueError(msg) from e
    return out


def resolve_endpoint_offsets(
    admin: AdminClient,
    topic_partitions: list[TopicPartitionKey],
    limit: RangeLimit,
    *,
    timeout_s: float,
    strategy: TimestampStrategy,
    isolation_level: IsolationLevel,
) -> dict[TopicPartitionKey, int]:
    """Resolve an endpoint's range limit to per-partition offsets or ``-2``/``-1`` sentinels."""
    match limit:
        case SpecialOffset():
            return dict.fromkeys(topic_partitions, limit.value)
        case PartitionOffsets(offsets):
            return {tp: offsets[tp] for tp in topic_partitions}
        case PartitionTimestamps(timestamps):
            wanted = {tp: OffsetSpec.for_timestamp(timestamps[tp]) for tp in topic_partitions}
        case GlobalTimestamp(timestamp):
            wanted = {tp: OffsetSpec.for_timestamp(timestamp) for tp in topic_partitions}

    resolved = get_offsets_per_topic_partition(admin, wanted, timeout_s=timeout_s, isolation_level=isolation_level)
    unmatched = [tp for tp in topic_partitions if resolved.get(tp, -1) < 0]
    if not unmatched:
        return resolved
    match strategy:
        case TimestampStrategy.ERROR:
            tp = unmatched[0]
            msg = (
                f"No offset matches the requested timestamp for {tp.topic}[{tp.partition}]. "
                f"Set 'startingOffsetsByTimestampStrategy' to 'latest' to read from the end of "
                f"the log instead of failing."
            )
            raise ValueError(msg)
        case TimestampStrategy.LATEST:
            fallback = get_offsets_per_topic_partition(
                admin,
                {tp: OffsetSpec.latest() for tp in unmatched},
                timeout_s=timeout_s,
                isolation_level=isolation_level,
            )
            resolved.update(fallback)
    return resolved
