# SPDX-License-Identifier: Apache-2.0

"""Driver-side planning for the Kafka data source: discover partitions, resolve offsets, split ranges.

``plan_offset_ranges`` is the entry point; the sections below follow the order it runs in.
"""

from __future__ import annotations

import math
from dataclasses import replace
from itertools import groupby
from operator import attrgetter
from typing import TYPE_CHECKING

from confluent_kafka.admin import OffsetSpec

from pysail.spark.datasource.kafka.client import describe_topics, list_offsets, list_topics
from pysail.spark.datasource.kafka.models import (
    AssignPartitions,
    GlobalTimestamp,
    OffsetRange,
    PartitionOffsets,
    PartitionTimestamps,
    SpecialOffset,
    SubscribePattern,
    SubscribeTopics,
    TimestampStrategy,
    TopicPartitionKey,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from confluent_kafka import IsolationLevel
    from confluent_kafka.admin import AdminClient

    from pysail.spark.datasource.kafka.models import RangeLimit, Subscription
    from pysail.spark.datasource.kafka.options import KafkaReadOptions


# ============================================================================
# Entry point
# ============================================================================


def plan_offset_ranges(admin: AdminClient, options: KafkaReadOptions) -> list[OffsetRange]:
    """Plan the per-task offset ranges of a read, in input-partition order.

    ``earliest``/``latest`` endpoints stay ``-2``/``-1`` sentinels; each task binds
    them when it starts reading, as Spark's batch Kafka source does.
    """
    # Spark discovers the starting and ending partitions independently (one
    # KafkaOffsetReader call per endpoint), so a partition added between the two
    # lookups surfaces as a mismatch below instead of a silently partial read.
    start_partitions = list_topic_partitions(admin, options.subscription, timeout_s=options.admin_timeout_s)
    validate_limit_covers_partitions(options.starting, start_partitions)
    starts = resolve_endpoint_offsets(
        admin,
        start_partitions,
        options.starting,
        timeout_s=options.admin_timeout_s,
        strategy=options.starting_ts_strategy,
        isolation_level=options.isolation_level,
    )

    end_partitions = list_topic_partitions(admin, options.subscription, timeout_s=options.admin_timeout_s)
    validate_limit_covers_partitions(options.ending, end_partitions)
    ends = resolve_endpoint_offsets(
        admin,
        end_partitions,
        options.ending,
        timeout_s=options.admin_timeout_s,
        strategy=TimestampStrategy.LATEST,
        isolation_level=options.isolation_level,
    )

    if starts.keys() != ends.keys():
        msg = (
            "different topic partitions for starting and ending offsets: "
            f"starting={sorted(starts)}, ending={sorted(ends)}"
        )
        raise ValueError(msg)

    offset_ranges = [
        OffsetRange(topic_partition=topic_partition, start=starts[topic_partition], end=ends[topic_partition])
        for topic_partition in sorted(starts)
    ]
    if not needs_split(offset_ranges, options):
        return offset_ranges

    # Resolve sentinels only to place interior split points, then restore the outer sentinels.
    original_ranges = {offset_range.topic_partition: offset_range for offset_range in offset_ranges}
    offset_ranges = resolve_sentinels(
        admin, offset_ranges, timeout_s=options.admin_timeout_s, isolation_level=options.isolation_level
    )
    offset_ranges = split_offset_ranges(
        [offset_range for offset_range in offset_ranges if offset_range.size > 0],
        min_partitions=options.min_partitions,
        max_records_per_partition=options.max_records_per_partition,
    )
    return restore_outer_bounds(offset_ranges, original_ranges)


def needs_split(offset_ranges: list[OffsetRange], options: KafkaReadOptions) -> bool:
    """Match Spark 4.1 ``shouldDivvyUpLargePartitions``, which tests the unresolved sizes."""
    if options.max_records_per_partition is not None and any(
        offset_range.size > options.max_records_per_partition for offset_range in offset_ranges
    ):
        return True
    return options.min_partitions is not None and options.min_partitions > len(offset_ranges)


# ============================================================================
# Partition discovery
# ============================================================================


def list_topic_partitions(
    admin: AdminClient, subscription: Subscription, *, timeout_s: float
) -> list[TopicPartitionKey]:
    """Enumerate the non-internal (topic, partition) pairs selected by the subscription."""
    topics: Sequence[str]
    match subscription:
        case SubscribeTopics():
            topics = subscription.topics
        case SubscribePattern(pattern):
            # Unanchored search matches Spark's SubscribePatternStrategy.
            topics = sorted(topic for topic in list_topics(admin, timeout_s=timeout_s) if pattern.search(topic))
        case AssignPartitions(partitions):
            topics = sorted({topic_partition.topic for topic_partition in partitions})

    if not topics:
        return []
    described = describe_topics(admin, topics, timeout_s=timeout_s)
    topic_partitions = []
    for topic in topics:
        description = described[topic]
        if description.is_internal:
            continue
        for partition in sorted(p.id for p in description.partitions):
            topic_partition = TopicPartitionKey(topic, partition)
            if isinstance(subscription, AssignPartitions) and topic_partition not in subscription.partitions:
                continue
            topic_partitions.append(topic_partition)
    return topic_partitions


def validate_limit_covers_partitions(limit: RangeLimit, topic_partitions: list[TopicPartitionKey]) -> None:
    """Require a per-partition offset/timestamp map to match the assigned partitions exactly."""
    match limit:
        case PartitionOffsets(offsets):
            specified = set(offsets)
        case PartitionTimestamps(timestamps):
            specified = set(timestamps)
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
        f"'{limit.option_name}' must specify all assigned TopicPartitions ({', '.join(details)}). "
        f"Use -1 for latest and -2 for earliest. "
        f"Specified: {sorted(specified)} Assigned: {sorted(assigned)}"
    )
    raise ValueError(msg)


# ============================================================================
# Offset resolution
# ============================================================================


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
            return {topic_partition: offsets[topic_partition] for topic_partition in topic_partitions}
        case PartitionTimestamps(timestamps):
            wanted = {
                topic_partition: OffsetSpec.for_timestamp(timestamps[topic_partition])
                for topic_partition in topic_partitions
            }
        case GlobalTimestamp(timestamp):
            wanted = {topic_partition: OffsetSpec.for_timestamp(timestamp) for topic_partition in topic_partitions}

    resolved = list_offsets(admin, wanted, timeout_s=timeout_s, isolation_level=isolation_level)
    unmatched = [topic_partition for topic_partition in topic_partitions if resolved.get(topic_partition, -1) < 0]
    if not unmatched:
        return resolved
    match strategy:
        case TimestampStrategy.ERROR:
            topic_partition = unmatched[0]
            msg = (
                f"No offset matches the requested timestamp for {topic_partition}. "
                f"Set 'startingOffsetsByTimestampStrategy' to 'latest' to read from the end of "
                f"the log instead of failing."
            )
            raise ValueError(msg)
        case TimestampStrategy.LATEST:
            fallback = list_offsets(
                admin,
                {topic_partition: OffsetSpec.latest() for topic_partition in unmatched},
                timeout_s=timeout_s,
                isolation_level=isolation_level,
            )
            resolved.update(fallback)
    return resolved


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

    lows = list_offsets(
        admin, dict.fromkeys(need_low, OffsetSpec.earliest()), timeout_s=timeout_s, isolation_level=isolation_level
    )
    highs = list_offsets(
        admin, dict.fromkeys(need_high, OffsetSpec.latest()), timeout_s=timeout_s, isolation_level=isolation_level
    )
    out = []
    for offset_range in offset_ranges:
        bound_start = (
            lows[offset_range.topic_partition] if offset_range.start == SpecialOffset.EARLIEST else offset_range.start
        )
        bound_end = (
            highs[offset_range.topic_partition] if offset_range.end == SpecialOffset.LATEST else offset_range.end
        )
        # Spark's KafkaOffsetRangeCalculator drops non-positive ranges after resolving
        # sentinels for splitting. Do this before OffsetRange validates the bounds;
        # the unsplit path still rejects inverted bounds when the task binds them.
        if bound_end <= bound_start:
            continue
        out.append(replace(offset_range, start=bound_start, end=bound_end))
    return out


# ============================================================================
# Range splitting: minPartitions / maxRecordsPerPartition
# ============================================================================


# SPDX-SnippetBegin
# SPDX-License-Identifier: Apache-2.0
# SPDX-SnippetComment: Adapted from https://github.com/apache/spark/blob/v4.1.1/connector/kafka-0-10-sql/src/main/scala/org/apache/spark/sql/kafka010/KafkaOffsetRangeCalculator.scala#L48-L135
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
            n_parts = (offset_range.size + max_records_per_partition - 1) // max_records_per_partition
            divided.extend(divide_range(offset_range, n_parts))
        offset_ranges = divided

    if min_partitions is None or min_partitions <= len(offset_ranges):
        return offset_ranges

    total_size = sum(offset_range.size for offset_range in offset_ranges)
    if total_size == 0:
        return offset_ranges

    # Spark 4.1.1 (upstream lines 90 and 99) keys unsplit ranges by topic-partition, even after maxRecordsPerPartition
    # has produced several chunks for that partition. Preserve this quirk: size 7,
    # maxRecordsPerPartition=4 and minPartitions=3 yields two chunks (3 and 4).
    unsplit = [
        offset_range for offset_range in offset_ranges if part_count(offset_range.size, total_size, min_partitions) == 1
    ]
    unsplit_keys = {offset_range.topic_partition for offset_range in unsplit}
    split_total_size = total_size - sum(offset_range.size for offset_range in unsplit)
    split_min_partitions = max(min_partitions - len(unsplit), 1)

    out: list[OffsetRange] = []
    for offset_range in offset_ranges:
        n_parts = (
            1
            if offset_range.topic_partition in unsplit_keys
            else part_count(offset_range.size, split_total_size, split_min_partitions)
        )
        out.extend(divide_range(offset_range, n_parts))
    return out


def part_count(size: int, total_size: int, min_parts: int) -> int:
    """Match Scala's ``math.round`` (``floor(x + 0.5)``), not Python's banker's rounding."""
    return max(math.floor(size / total_size * min_parts + 0.5), 1)


def divide_range(offset_range: OffsetRange, n_parts: int) -> list[OffsetRange]:
    """Split a range into ``n_parts`` near-equal chunks, with the larger chunks last.

    Mirrors Spark's ``KafkaOffsetRangeCalculator.getDividedPartition`` (10 over 3 -> 3/3/4).
    Never makes more chunks than there are offsets, so no chunk is empty.
    """
    n_parts = min(max(n_parts, 1), offset_range.size)
    chunks: list[OffsetRange] = []
    start = offset_range.start
    remaining = offset_range.size
    for parts_left in range(n_parts, 0, -1):
        length = remaining // parts_left
        chunks.append(replace(offset_range, start=start, end=start + length))
        start += length
        remaining -= length
    return chunks


# SPDX-SnippetEnd


def restore_outer_bounds(
    split_ranges: list[OffsetRange], original_ranges: dict[TopicPartitionKey, OffsetRange]
) -> list[OffsetRange]:
    """Swap each partition's resolved outer endpoints back to the original values.

    Mirrors ``getOffsetRangesFromUnresolvedOffsets``: only interior split points
    are fixed; late-bound outer sentinels stay late-bound. Expects each
    partition's split ranges to be adjacent and in offset order, as
    ``split_offset_ranges`` produces them.
    """
    restored: list[OffsetRange] = []
    for topic_partition, group in groupby(split_ranges, key=attrgetter("topic_partition")):
        partition_splits = list(group)
        original = original_ranges[topic_partition]
        partition_splits[0] = replace(partition_splits[0], start=original.start)
        partition_splits[-1] = replace(partition_splits[-1], end=original.end)
        restored.extend(partition_splits)
    return restored
