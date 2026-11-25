# SPDX-License-Identifier: Apache-2.0

"""Domain types shared by option parsing, planning, and reading in the Kafka data source."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Literal, NamedTuple, TypeAlias

if TYPE_CHECKING:
    import re
    from collections.abc import Mapping

RangeBoundary: TypeAlias = Literal["starting", "ending"]


class SpecialOffset(IntEnum):
    """Spark's per-partition offset sentinels, resolved per task (``-2``/``-1`` in JSON)."""

    EARLIEST = -2
    LATEST = -1


class TimestampStrategy(str, Enum):
    """Behavior when a timestamp lookup finds no matching offset."""

    ERROR = "error"
    LATEST = "latest"


class TopicPartitionKey(NamedTuple):
    """Hashable ``(topic, partition)`` key for per-partition maps; prints as ``topic[partition]``."""

    topic: str
    partition: int

    def __str__(self) -> str:
        return f"{self.topic}[{self.partition}]"


class Watermarks(NamedTuple):
    """The broker's low offset and exclusive high offset."""

    low: int
    high: int


# ----------------------------------------------------------------------------
# Subscription: which topics or partitions a read selects
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class SubscribeTopics:
    """``subscribe``: an explicit, deduplicated and sorted topic list."""

    topics: tuple[str, ...]


@dataclass(frozen=True)
class SubscribePattern:
    """``subscribePattern``: topics whose name contains a match (unanchored, as in Spark)."""

    pattern: re.Pattern[str]


@dataclass(frozen=True)
class AssignPartitions:
    """``assign``: explicit partitions, intersected with what the broker reports."""

    partitions: frozenset[TopicPartitionKey]


Subscription: TypeAlias = SubscribeTopics | SubscribePattern | AssignPartitions


# ----------------------------------------------------------------------------
# Range limit: one boundary (start or end) of the read range,
# mirroring Spark's KafkaOffsetRangeLimit cases
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class PartitionOffsets:
    """Per-partition offsets from ``startingOffsets``/``endingOffsets`` JSON (``-2``/``-1`` are sentinels)."""

    offsets: Mapping[TopicPartitionKey, int]
    option_name: str


@dataclass(frozen=True)
class PartitionTimestamps:
    """Per-partition epoch-ms timestamps from ``startingOffsetsByTimestamp``/``endingOffsetsByTimestamp``."""

    timestamps: Mapping[TopicPartitionKey, int]
    option_name: str


@dataclass(frozen=True)
class GlobalTimestamp:
    """One epoch-ms timestamp applied to every partition, from ``startingTimestamp``/``endingTimestamp``."""

    timestamp: int


RangeLimit: TypeAlias = SpecialOffset | PartitionOffsets | PartitionTimestamps | GlobalTimestamp


# ----------------------------------------------------------------------------
# Offset range: the unit of work for one read task
# ----------------------------------------------------------------------------


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
                f"Inverted offset range for {self.topic_partition}: "
                f"start {self.start} is past end {self.end}. "
                f"Check 'startingOffsets'/'endingOffsets'; the log may also have been truncated "
                f"while the range was being resolved."
            )
            raise ValueError(msg)

    @property
    def size(self) -> int:
        return self.end - self.start
