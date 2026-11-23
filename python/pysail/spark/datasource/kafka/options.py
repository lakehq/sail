"""Option parsing, validation, and librdkafka client configuration for the Kafka data source."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Literal, NamedTuple, TypeAlias

from confluent_kafka import IsolationLevel

if TYPE_CHECKING:
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


class SubscriptionStrategy(str, Enum):
    """How Kafka topics or partitions are selected."""

    ASSIGN = "assign"
    SUBSCRIBE = "subscribe"
    SUBSCRIBE_PATTERN = "subscribePattern"


class TopicPartitionKey(NamedTuple):
    """Hashable ``(topic, partition)`` key for per-partition maps."""

    topic: str
    partition: int


@dataclass(frozen=True)
class SubscribeTopics:
    """``subscribe``: an explicit, deduplicated and sorted topic list."""

    topics: list[str]


@dataclass(frozen=True)
class SubscribePattern:
    """``subscribePattern``: topics whose name contains a match (unanchored, as in Spark)."""

    pattern: re.Pattern[str]


@dataclass(frozen=True)
class AssignPartitions:
    """``assign``: explicit partitions, intersected with what the broker reports."""

    partitions: frozenset[TopicPartitionKey]


Subscription: TypeAlias = SubscribeTopics | SubscribePattern | AssignPartitions


# One boundary (start or end) of the read range, mirroring Spark's KafkaOffsetRangeLimit cases.


@dataclass(frozen=True)
class PartitionOffsets:
    """Per-partition offsets from ``startingOffsets``/``endingOffsets`` JSON (``-2``/``-1`` are sentinels)."""

    offsets: dict[TopicPartitionKey, int]


@dataclass(frozen=True)
class PartitionTimestamps:
    """Per-partition epoch-ms timestamps from ``startingOffsetsByTimestamp``/``endingOffsetsByTimestamp``."""

    timestamps: dict[TopicPartitionKey, int]


@dataclass(frozen=True)
class GlobalTimestamp:
    """One epoch-ms timestamp applied to every partition, from ``startingTimestamp``/``endingTimestamp``."""

    timestamp: int


RangeLimit: TypeAlias = SpecialOffset | PartitionOffsets | PartitionTimestamps | GlobalTimestamp


# Scala String.toInt / String.toLong bounds used by Spark's split options.
INT32_MAX = 2**31 - 1
INT64_MAX = 2**63 - 1

# Source-owned consumer properties rejected to match Spark's validateGeneralOptions:
# overriding them silently moves planned offsets, commits group offsets, or disables EOF stop.
PROTECTED_KAFKA_CONFIGS = {
    "auto.offset.reset": "startingOffsets/endingOffsets",
    "enable.auto.commit": None,
    "enable.partition.eof": None,
}

STRICT_INT_RE = re.compile(r"[+-]?\d+")

# Stable group id: reads use explicit assign() without group membership or commits,
# so concurrent queries do not coordinate or mutate group state. Overridable via kafka.group.id.
DEFAULT_GROUP_ID = "sail-kafka-reader"

# Consumer-only properties stripped from AdminClient (producer-backed in librdkafka)
# to avoid CONFWARN logs. isolation.level is never in client_config: it is parsed
# into an IsolationLevel and passed to list_offsets and the consumer explicitly.
CONSUMER_ONLY_CONFIGS = (
    "group.id",
    "enable.auto.commit",
    "auto.offset.reset",
    "enable.partition.eof",
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


def parse_strict_int(option_value: str, *, label: str) -> int:
    """Parse an integer matching Java/Scala rules (no underscores or surrounding whitespace)."""
    if not STRICT_INT_RE.fullmatch(option_value):
        msg = f"{label} must be an integer, got {option_value!r}"
        raise ValueError(msg)
    return int(option_value)


def check_integer_width(value: int, *, bits: int, label: str) -> int:
    """Reject values that cannot cross Kafka's signed integer API boundary."""
    limit = 1 << (bits - 1)
    if not -limit <= value < limit:
        msg = f"{label} must fit a signed {bits}-bit integer, got: {value}"
        raise ValueError(msg)
    return value


def parse_topic_partition_mapping(option_value: str, name: str) -> dict[TopicPartitionKey, int]:
    """Parse a ``{topic: {partition: int}}`` JSON mapping into a flat per-partition map.

    Partition keys are parsed strictly as signed 32-bit ints (e.g. ``"00"`` -> ``0``)
    to match Spark's ``Map[Int, Long]`` deserialization (last spelling wins).
    """
    # Shape errors raise ValueError like json.loads does: the option string is
    # well typed, its content is malformed (hence the TRY004 suppressions).
    parsed = json.loads(option_value)
    if not isinstance(parsed, dict):
        msg = f"'{name}' JSON must be an object, got: {type(parsed).__name__}"
        raise ValueError(msg)  # noqa: TRY004
    normalized: dict[TopicPartitionKey, int] = {}
    for topic, partitions in parsed.items():
        if not isinstance(partitions, dict):
            msg = f"'{name}' entry for topic {topic!r} must be an object of {{partition: value}}, got: {type(partitions).__name__}"
            raise ValueError(msg)  # noqa: TRY004
        for partition, value in partitions.items():
            label = f"'{name}' partition id for topic {topic!r}"
            partition_id = check_integer_width(parse_strict_int(partition, label=label), bits=32, label=label)
            # bool is an int subclass; reject JSON true/false.
            if not isinstance(value, int) or isinstance(value, bool):
                msg = f"'{name}' value for topic {topic!r} partition {partition!r} must be an integer, got: {type(value).__name__}"
                raise ValueError(msg)  # noqa: TRY004
            check_integer_width(value, bits=64, label=f"'{name}' value for topic {topic!r} partition {partition!r}")
            normalized[TopicPartitionKey(topic, partition_id)] = value
    return normalized


def validate_offsets(
    offsets: SpecialOffset | dict[TopicPartitionKey, int],
    *,
    boundary: RangeBoundary,
) -> None:
    """Enforce Spark's batch-mode rules on a ``startingOffsets``/``endingOffsets`` value.

    Rejects ``latest``/``-1`` at ``startingOffsets``, ``earliest``/``-2`` at
    ``endingOffsets``, and any offset below ``-2`` (which librdkafka treats as
    special sentinels such as ``-1000`` ``OFFSET_STORED``).
    """
    key = f"{boundary}Offsets"
    forbidden = SpecialOffset.LATEST if boundary == "starting" else SpecialOffset.EARLIEST
    allowed = SpecialOffset.EARLIEST if boundary == "starting" else SpecialOffset.LATEST

    if isinstance(offsets, SpecialOffset):
        if offsets is forbidden:
            msg = (
                f"'{key}' does not support {forbidden.name.lower()!r} for batch reads; "
                f"use {allowed.name.lower()!r} or per-partition JSON"
            )
            raise ValueError(msg)
        return

    for tp, value in offsets.items():
        where = f"topic {tp.topic!r} partition {tp.partition}"
        if value == forbidden:
            msg = (
                f"'{key}' does not support offset {forbidden.value} "
                f"({forbidden.name.lower()}) for batch reads, at {where}"
            )
            raise ValueError(msg)
        if value < SpecialOffset.EARLIEST:
            msg = (
                f"'{key}' offset for {where} must be >= 0, "
                f"{SpecialOffset.LATEST.value} (latest), or {SpecialOffset.EARLIEST.value} (earliest), got: {value}"
            )
            raise ValueError(msg)


def parse_range_limit(options: Mapping[str, str], *, boundary: RangeBoundary) -> RangeLimit:
    """Parse the starting or ending boundary of a Kafka read using Spark's precedence.

    Global timestamp wins over per-partition timestamps, which win over offsets.
    Skipped lower-priority options are not parsed, matching Spark's
    ``KafkaSourceProvider.getKafkaOffsetRangeLimit``.
    """
    key = f"{boundary}Timestamp"
    if key in options:
        label = f"'{key}'"
        timestamp = parse_strict_int(options[key].strip(), label=label)
        return GlobalTimestamp(check_integer_width(timestamp, bits=64, label=label))

    key = f"{boundary}OffsetsByTimestamp"
    if key in options:
        return PartitionTimestamps(parse_topic_partition_mapping(options[key], key))

    key = f"{boundary}Offsets"
    if key not in options:
        return SpecialOffset.EARLIEST if boundary == "starting" else SpecialOffset.LATEST

    option_value = options[key].strip()
    try:
        sentinel = SpecialOffset[option_value.upper()]
    except KeyError:
        offsets = parse_topic_partition_mapping(option_value, key)
        validate_offsets(offsets, boundary=boundary)
        return PartitionOffsets(offsets)
    validate_offsets(sentinel, boundary=boundary)
    return sentinel


def parse_timestamp_strategy(option_value: str) -> TimestampStrategy:
    """Parse ``startingOffsetsByTimestampStrategy`` (case-insensitive ``error``/``latest``)."""
    try:
        return TimestampStrategy(option_value.lower())
    except ValueError as e:
        msg = f"'startingOffsetsByTimestampStrategy' must be 'error' or 'latest', got {option_value!r}"
        raise ValueError(msg) from e


def parse_bool(option_value: str, name: str) -> bool:
    """Parse a boolean strictly (case-insensitive ``true``/``false``, matching Scala ``toBoolean``)."""
    normalized = option_value.lower()
    if normalized in ("true", "false"):
        return normalized == "true"
    msg = f"'{name}' must be 'true' or 'false', got {option_value!r}"
    raise ValueError(msg)


def parse_positive_int(option_value: str, name: str, *, max_value: int | None = None) -> int:
    """Parse a positive integer option, optionally capped to a Scala/Kafka width."""
    value = parse_strict_int(option_value, label=f"'{name}'")
    if value <= 0:
        msg = f"'{name}' must be positive, got {value}"
        raise ValueError(msg)
    if max_value is not None and value > max_value:
        msg = f"'{name}' must be at most {max_value}, got {value}"
        raise ValueError(msg)
    return value


def parse_positive_ms(option_value: str, name: str, *, max_value: int = INT64_MAX) -> float:
    """Parse a positive whole number of milliseconds and return seconds.

    Whole milliseconds only, as Spark reads ``kafkaConsumer.pollTimeoutMs`` with ``toLong``;
    the result is in seconds because Python's client timeouts take float seconds.
    """
    return parse_positive_int(option_value, name, max_value=max_value) / 1000


def build_admin_config(client_config: dict[str, str]) -> dict[str, str]:
    """Return the librdkafka config for the planning-time ``AdminClient``."""
    return {k: v for k, v in client_config.items() if k not in CONSUMER_ONLY_CONFIGS}


def parse_isolation_level(option_value: str) -> IsolationLevel:
    """Parse ``kafka.isolation.level`` case-insensitively (``read_uncommitted`` or ``read_committed``)."""
    try:
        return IsolationLevel[option_value.upper()]
    except KeyError:
        msg = f"'kafka.isolation.level' must be 'read_uncommitted' or 'read_committed', got {option_value!r}"
        raise ValueError(msg) from None


def parse_client_config(options: Mapping[str, str]) -> tuple[dict[str, str], IsolationLevel]:
    """Extract and validate ``kafka.*`` client properties and the effective isolation level."""
    client_config = {k[len("kafka.") :]: v for k, v in options.items() if k.startswith("kafka.")}
    if not client_config.get("bootstrap.servers", "").strip():
        msg = "Option 'kafka.bootstrap.servers' is required and must not be blank for the kafka data source"
        raise ValueError(msg)
    for name, alternative in PROTECTED_KAFKA_CONFIGS.items():
        if name in client_config:
            msg = f"Kafka option 'kafka.{name}' is not supported"
            if alternative:
                msg += f"; use the source option(s) '{alternative}' instead"
            raise ValueError(msg)
    # Default matches the Java client Spark uses, not librdkafka's read_committed.
    isolation_level = (
        parse_isolation_level(client_config.pop("isolation.level"))
        if "isolation.level" in client_config
        else IsolationLevel.READ_UNCOMMITTED
    )
    return client_config, isolation_level


def build_consumer_config(client_config: dict[str, str], isolation_level: IsolationLevel) -> dict[str, str | bool]:
    """Return the effective librdkafka config for a batch-read consumer.

    Source-owned settings are applied last: ``auto.offset.reset=error`` keeps
    unserviceable offsets visible to the read loop's ``failOnDataLoss`` logic.
    """
    return {
        "group.id": DEFAULT_GROUP_ID,
        **client_config,
        # librdkafka spells the property value as the lowercase enum name.
        "isolation.level": isolation_level.name.lower(),
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
        "enable.partition.eof": True,
    }


def parse_subscription(options: Mapping[str, str]) -> Subscription:
    """Parse Spark's mutually-exclusive ``assign`` / ``subscribe`` / ``subscribePattern`` option."""
    present = [strategy for strategy in SubscriptionStrategy if strategy.value in options]
    if len(present) != 1:
        choices = ", ".join(strategy.value for strategy in SubscriptionStrategy)
        if present:
            msg = f"Only one of the following options may be specified: {choices}. Got: {', '.join(strategy.value for strategy in present)}"
        else:
            msg = f"One of the following options must be specified for the kafka data source: {choices}"
        raise ValueError(msg)

    strategy = present[0]
    option_value = options[strategy.value]

    match strategy:
        case SubscriptionStrategy.SUBSCRIBE:
            topics = sorted({t.strip() for t in option_value.split(",")} - {""})
            if not topics:
                msg = "Option 'subscribe' must list at least one topic"
                raise ValueError(msg)
            return SubscribeTopics(topics)

        case SubscriptionStrategy.SUBSCRIBE_PATTERN:
            pattern = option_value.strip()
            if not pattern:
                msg = "Option 'subscribePattern' must be a non-empty regular expression"
                raise ValueError(msg)
            try:
                return SubscribePattern(re.compile(pattern))
            except re.error as e:
                msg = f"Option 'subscribePattern' is not a valid regular expression: {pattern!r} ({e})"
                raise ValueError(msg) from e

        case SubscriptionStrategy.ASSIGN:
            # Like Spark's JsonUtils.partitions, accept empty maps/lists and nonexistent
            # int32 ids; list_topic_partitions intersects them with broker metadata.
            # Shape errors raise ValueError, as in parse_topic_partition_mapping.
            parsed = json.loads(option_value)
            if not isinstance(parsed, dict):
                msg = f"'assign' JSON must be an object of {{topic: [partition, ...]}}, got: {option_value!r}"
                raise ValueError(msg)  # noqa: TRY004
            assigned: set[TopicPartitionKey] = set()
            for topic, partitions in parsed.items():
                if not isinstance(partitions, list):
                    msg = f"'assign' entry for topic {topic!r} must be an array of partition ids"
                    raise ValueError(msg)  # noqa: TRY004
                for partition in partitions:
                    if not isinstance(partition, int) or isinstance(partition, bool):
                        msg = f"'assign' partition id for topic {topic!r} must be an integer, got: {partition!r}"
                        raise ValueError(msg)  # noqa: TRY004
                    check_integer_width(partition, bits=32, label=f"'assign' partition id for topic {topic!r}")
                assigned.update(TopicPartitionKey(topic, p) for p in partitions)
            return AssignPartitions(frozenset(assigned))
