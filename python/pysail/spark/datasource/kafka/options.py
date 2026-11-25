# SPDX-License-Identifier: Apache-2.0

"""Spark option parsing and validation for the Kafka data source.

``parse_options`` turns the raw, case-insensitive Spark options into one
``KafkaReadOptions``; the rest of this module are the individual parsers it uses.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, TypeVar

from confluent_kafka import IsolationLevel
from pyspark.sql.datasource import CaseInsensitiveDict

from pysail.spark.datasource.kafka.models import (
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
    from collections.abc import Callable, Mapping

    from pysail.spark.datasource.kafka.models import RangeBoundary, RangeLimit, Subscription


class SubscriptionStrategy(str, Enum):
    """How Kafka topics or partitions are selected (the option names of Spark's strategies)."""

    ASSIGN = "assign"
    SUBSCRIBE = "subscribe"
    SUBSCRIBE_PATTERN = "subscribePattern"


@dataclass(frozen=True, kw_only=True)
class BoundarySpec:
    """Option names and sentinel rules for one end (starting or ending) of a read range."""

    timestamp_option: str
    offsets_by_timestamp_option: str
    offsets_option: str
    default_sentinel: SpecialOffset
    forbidden_sentinel: SpecialOffset


BOUNDARIES: Mapping[RangeBoundary, BoundarySpec] = {
    "starting": BoundarySpec(
        timestamp_option="startingTimestamp",
        offsets_by_timestamp_option="startingOffsetsByTimestamp",
        offsets_option="startingOffsets",
        default_sentinel=SpecialOffset.EARLIEST,
        forbidden_sentinel=SpecialOffset.LATEST,
    ),
    "ending": BoundarySpec(
        timestamp_option="endingTimestamp",
        offsets_by_timestamp_option="endingOffsetsByTimestamp",
        offsets_option="endingOffsets",
        default_sentinel=SpecialOffset.LATEST,
        forbidden_sentinel=SpecialOffset.EARLIEST,
    ),
}

# Option strings matched case-insensitively before falling back to per-partition JSON.
SPECIAL_OFFSETS: Mapping[str, SpecialOffset] = {
    "earliest": SpecialOffset.EARLIEST,
    "latest": SpecialOffset.LATEST,
}


# Scala String.toInt / String.toLong bounds used by Spark's split options.
INT32_MAX = 2**31 - 1
INT64_MAX = 2**63 - 1

DEFAULT_MAX_BATCH_ROWS = 10_000
DEFAULT_MAX_BATCH_BYTES = 8 * 1024 * 1024
DEFAULT_POLL_TIMEOUT_S = 120.0
DEFAULT_ADMIN_TIMEOUT_S = 10.0
MAX_ADMIN_TIMEOUT_MS = 3_600_000

# Source-owned consumer properties rejected to match Spark's validateGeneralOptions:
# overriding them silently moves planned offsets, commits group offsets, or disables EOF stop.
PROTECTED_KAFKA_CONFIGS = {
    "auto.offset.reset": "startingOffsets/endingOffsets",
    "enable.auto.commit": None,
    "enable.partition.eof": None,
}

STRICT_INT_RE = re.compile(r"[+-]?\d+")


@dataclass(frozen=True, kw_only=True)
class KafkaReadOptions:
    """Validated settings for one Kafka batch read.

    Planning on the driver uses the subscription, range limits, and split hints;
    each task on the executors uses the client config, polling, and batching limits.
    """

    subscription: Subscription
    starting: RangeLimit
    ending: RangeLimit
    starting_ts_strategy: TimestampStrategy
    # Excluded from repr: it may hold SASL/SSL secrets.
    client_config: Mapping[str, str] = field(repr=False)
    isolation_level: IsolationLevel
    include_headers: bool
    poll_timeout_s: float
    max_batch_rows: int
    max_batch_bytes: int
    admin_timeout_s: float
    fail_on_data_loss: bool
    min_partitions: int | None
    max_records_per_partition: int | None


def parse_options(raw_options: Mapping[str, str]) -> KafkaReadOptions:
    """Parse and validate the Spark options of a Kafka read (keys are case-insensitive)."""
    options = CaseInsensitiveDict(raw_options)

    client_config, isolation_level = parse_client_config(options)
    subscription = parse_subscription(options)
    starting = parse_range_limit(options, boundary="starting")
    ending = parse_range_limit(options, boundary="ending")
    starting_ts_strategy = parse_timestamp_strategy(
        options.get("startingOffsetsByTimestampStrategy", TimestampStrategy.ERROR.value)
    )
    validate_paired_limits(starting, ending)

    # Default matches Spark's fallback to spark.network.timeout (120s).
    poll_option = "kafkaConsumer.pollTimeoutMs" if "kafkaConsumer.pollTimeoutMs" in options else "pollTimeoutMs"

    return KafkaReadOptions(
        subscription=subscription,
        starting=starting,
        ending=ending,
        starting_ts_strategy=starting_ts_strategy,
        client_config=client_config,
        isolation_level=isolation_level,
        include_headers=parse_option(options, "includeHeaders", parse_bool, False),
        fail_on_data_loss=parse_option(options, "failOnDataLoss", parse_bool, True),
        max_batch_rows=parse_option(options, "maxBatchRows", parse_positive_int, DEFAULT_MAX_BATCH_ROWS),
        max_batch_bytes=parse_option(
            options, "maxBatchBytes", parse_positive_int, DEFAULT_MAX_BATCH_BYTES, max_value=INT32_MAX
        ),
        min_partitions=parse_option(options, "minPartitions", parse_positive_int, None, max_value=INT32_MAX),
        max_records_per_partition=parse_option(
            options, "maxRecordsPerPartition", parse_positive_int, None, max_value=INT64_MAX
        ),
        # Consumer.poll converts seconds to a signed C int in milliseconds.
        poll_timeout_s=parse_option(options, poll_option, parse_positive_ms, DEFAULT_POLL_TIMEOUT_S, max_value=INT32_MAX),
        # librdkafka admin request timeouts accept 1 ms to 1 hour.
        admin_timeout_s=parse_option(
            options, "adminTimeoutMs", parse_positive_ms, DEFAULT_ADMIN_TIMEOUT_S, max_value=MAX_ADMIN_TIMEOUT_MS
        ),
    )


_Parsed = TypeVar("_Parsed")
_Default = TypeVar("_Default")


def parse_option(
    opts: Mapping[str, str], option_name: str, parse: Callable[..., _Parsed], default: _Default, **parse_kwargs: int
) -> _Parsed | _Default:
    """Return ``parse(value, option_name)`` when the option is set, else ``default``."""
    return parse(opts[option_name], option_name, **parse_kwargs) if option_name in opts else default


def parse_client_config(options: CaseInsensitiveDict) -> tuple[dict[str, str], IsolationLevel]:
    """Extract and validate ``kafka.*`` client properties and the effective isolation level."""
    client_config = {k[len("kafka.") :]: v for k, v in options.items() if k.startswith("kafka.")}
    if not client_config.get("bootstrap.servers", "").strip():
        msg = "Option 'kafka.bootstrap.servers' is required and must not be blank for the kafka data source"
        raise ValueError(msg)
    for config_name, alternative in PROTECTED_KAFKA_CONFIGS.items():
        if config_name in client_config:
            msg = f"Kafka option 'kafka.{config_name}' is not supported"
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


def parse_isolation_level(option_value: str) -> IsolationLevel:
    """Parse ``kafka.isolation.level`` case-insensitively (``read_uncommitted`` or ``read_committed``)."""
    try:
        return IsolationLevel[option_value.upper()]
    except KeyError:
        msg = f"'kafka.isolation.level' must be 'read_uncommitted' or 'read_committed', got {option_value!r}"
        raise ValueError(msg) from None


def parse_subscription(options: CaseInsensitiveDict) -> Subscription:
    """Parse Spark's mutually-exclusive ``assign`` / ``subscribe`` / ``subscribePattern`` option."""
    present = [strategy for strategy in SubscriptionStrategy if strategy.value in options]
    if len(present) != 1:
        choices = ", ".join(strategy.value for strategy in SubscriptionStrategy)
        if present:
            given = ", ".join(strategy.value for strategy in present)
            msg = f"Only one of the following options may be specified: {choices}. Got: {given}"
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
            return SubscribeTopics(tuple(topics))

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
            return AssignPartitions(parse_assignment(option_value))


def parse_assignment(option_value: str) -> frozenset[TopicPartitionKey]:
    """Parse ``assign``'s ``{topic: [partition, ...]}`` JSON.

    Like Spark's ``JsonUtils.partitions``, accepts empty maps/lists and nonexistent
    int32 ids; ``list_topic_partitions`` intersects them with broker metadata.
    Shape errors raise ValueError, as in ``parse_topic_partition_mapping``.
    """
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
            partition_id = require_json_int(partition, bits=32, label=f"'assign' partition id for topic {topic!r}")
            assigned.add(TopicPartitionKey(topic, partition_id))
    return frozenset(assigned)


def parse_range_limit(options: CaseInsensitiveDict, *, boundary: RangeBoundary) -> RangeLimit:
    """Parse the starting or ending boundary of a Kafka read using Spark's precedence.

    Global timestamp wins over per-partition timestamps, which win over offsets.
    Skipped lower-priority options are not parsed, matching Spark's
    ``KafkaSourceProvider.getKafkaOffsetRangeLimit``.
    """
    spec = BOUNDARIES[boundary]

    if spec.timestamp_option in options:
        label = f"'{spec.timestamp_option}'"
        timestamp = parse_strict_int(options[spec.timestamp_option].strip(), label=label)
        return GlobalTimestamp(check_integer_width(timestamp, bits=64, label=label))

    if spec.offsets_by_timestamp_option in options:
        option_value = options[spec.offsets_by_timestamp_option]
        return PartitionTimestamps(
            parse_topic_partition_mapping(option_value, spec.offsets_by_timestamp_option),
            spec.offsets_by_timestamp_option,
        )

    if spec.offsets_option not in options:
        return spec.default_sentinel

    option_value = options[spec.offsets_option].strip()
    sentinel = SPECIAL_OFFSETS.get(option_value.lower())
    limit: SpecialOffset | PartitionOffsets = (
        sentinel
        if sentinel is not None
        else PartitionOffsets(parse_topic_partition_mapping(option_value, spec.offsets_option), spec.offsets_option)
    )
    validate_offsets(limit, spec)
    return limit


def parse_topic_partition_mapping(option_value: str, option_name: str) -> dict[TopicPartitionKey, int]:
    """Parse a ``{topic: {partition: int}}`` JSON mapping into a flat per-partition map.

    Partition keys are parsed strictly as signed 32-bit ints (e.g. ``"00"`` -> ``0``)
    to match Spark's ``Map[Int, Long]`` deserialization (last spelling wins).
    """
    # Shape errors raise ValueError like json.loads does: the option string is
    # well typed, its content is malformed (hence the TRY004 suppressions).
    parsed = json.loads(option_value)
    if not isinstance(parsed, dict):
        msg = f"'{option_name}' JSON must be an object, got: {type(parsed).__name__}"
        raise ValueError(msg)  # noqa: TRY004
    normalized: dict[TopicPartitionKey, int] = {}
    for topic, partitions in parsed.items():
        if not isinstance(partitions, dict):
            msg = (
                f"'{option_name}' entry for topic {topic!r} must be an object of {{partition: value}}, "
                f"got: {type(partitions).__name__}"
            )
            raise ValueError(msg)  # noqa: TRY004
        for partition, value in partitions.items():
            label = f"'{option_name}' partition id for topic {topic!r}"
            partition_id = check_integer_width(parse_strict_int(partition, label=label), bits=32, label=label)
            value_label = f"'{option_name}' value for topic {topic!r} partition {partition!r}"
            normalized[TopicPartitionKey(topic, partition_id)] = require_json_int(value, bits=64, label=value_label)
    return normalized


def validate_offsets(limit: SpecialOffset | PartitionOffsets, spec: BoundarySpec) -> None:
    """Enforce Spark's batch-mode rules on a ``startingOffsets``/``endingOffsets`` value.

    Rejects ``latest``/``-1`` at ``startingOffsets``, ``earliest``/``-2`` at
    ``endingOffsets``, and any offset below ``-2`` (which librdkafka treats as
    special sentinels such as ``-1000`` ``OFFSET_STORED``).
    """
    option_name = spec.offsets_option
    forbidden = spec.forbidden_sentinel
    # Each boundary allows only its default sentinel (earliest at start, latest at end).
    allowed = spec.default_sentinel

    if isinstance(limit, SpecialOffset):
        if limit is forbidden:
            msg = (
                f"'{option_name}' does not support {forbidden.name.lower()!r} for batch reads; "
                f"use {allowed.name.lower()!r} or per-partition JSON"
            )
            raise ValueError(msg)
        return

    for topic_partition, value in limit.offsets.items():
        where = f"topic {topic_partition.topic!r} partition {topic_partition.partition}"
        if value == forbidden:
            msg = (
                f"'{option_name}' does not support offset {forbidden.value} "
                f"({forbidden.name.lower()}) for batch reads, at {where}"
            )
            raise ValueError(msg)
        if value < SpecialOffset.EARLIEST:
            msg = (
                f"'{option_name}' offset for {where} must be >= 0, "
                f"{SpecialOffset.LATEST.value} (latest), or {SpecialOffset.EARLIEST.value} (earliest), got: {value}"
            )
            raise ValueError(msg)


def validate_paired_limits(starting: RangeLimit, ending: RangeLimit) -> None:
    """Check paired per-partition start/end maps before any broker lookup, matching Spark."""
    match starting, ending:
        case PartitionOffsets(offsets=starts), PartitionOffsets(offsets=ends):
            kind = "offset"
            has_sentinels = True
        case PartitionTimestamps(timestamps=starts), PartitionTimestamps(timestamps=ends):
            kind = "timestamp"
            has_sentinels = False
        case _:
            return

    if starts.keys() != ends.keys():
        msg = (
            f"Starting and ending {kind} maps must specify the same TopicPartitions: "
            f"starting={sorted(starts)}, ending={sorted(ends)}"
        )
        raise ValueError(msg)

    for topic_partition, start in starts.items():
        end = ends[topic_partition]
        if has_sentinels and (start < 0 or end < 0):
            continue
        if start > end:
            msg = f"Inverted {kind} range for {topic_partition}: start {start} is past end {end}"
            raise ValueError(msg)


def parse_timestamp_strategy(option_value: str) -> TimestampStrategy:
    """Parse ``startingOffsetsByTimestampStrategy`` (case-insensitive ``error``/``latest``)."""
    try:
        return TimestampStrategy(option_value.lower())
    except ValueError as e:
        msg = f"'startingOffsetsByTimestampStrategy' must be 'error' or 'latest', got {option_value!r}"
        raise ValueError(msg) from e


def parse_bool(option_value: str, option_name: str) -> bool:
    """Parse a boolean strictly (case-insensitive ``true``/``false``, matching Scala ``toBoolean``)."""
    normalized = option_value.lower()
    if normalized in ("true", "false"):
        return normalized == "true"
    msg = f"'{option_name}' must be 'true' or 'false', got {option_value!r}"
    raise ValueError(msg)


def parse_positive_int(option_value: str, option_name: str, *, max_value: int | None = None) -> int:
    """Parse a positive integer option, optionally capped to a Scala/Kafka width."""
    value = parse_strict_int(option_value, label=f"'{option_name}'")
    if value <= 0:
        msg = f"'{option_name}' must be positive, got {value}"
        raise ValueError(msg)
    if max_value is not None and value > max_value:
        msg = f"'{option_name}' must be at most {max_value}, got {value}"
        raise ValueError(msg)
    return value


def parse_positive_ms(option_value: str, option_name: str, *, max_value: int = INT64_MAX) -> float:
    """Parse a positive whole number of milliseconds and return seconds.

    Whole milliseconds only, as Spark reads ``kafkaConsumer.pollTimeoutMs`` with ``toLong``;
    the result is in seconds because Python's client timeouts take float seconds.
    """
    return parse_positive_int(option_value, option_name, max_value=max_value) / 1000


def parse_strict_int(option_value: str, *, label: str) -> int:
    """Parse an integer matching Java/Scala rules (no underscores or surrounding whitespace).

    ``label`` names the value in the error message, e.g. ``'minPartitions'``.
    """
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


def require_json_int(value: object, *, bits: int, label: str) -> int:
    """Validate a JSON integer and its Kafka width, excluding JSON booleans."""
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"{label} must be an integer, got: {type(value).__name__}"
        raise ValueError(msg)  # noqa: TRY004
    return check_integer_width(value, bits=bits, label=label)
