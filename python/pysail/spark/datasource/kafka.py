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
import math
import re
import time
from concurrent.futures import TimeoutError as FuturesTimeoutError
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

# Spark's magic per-partition offset sentinels.
_SPARK_OFFSET_LATEST = -1
_SPARK_OFFSET_EARLIEST = -2

# ``startingOffsetsByTimestampStrategy`` values.
_TS_STRATEGY_ERROR = "error"
_TS_STRATEGY_LATEST = "latest"

_SUBSCRIPTION_KEYS = ("assign", "subscribe", "subscribePattern")


def _validate_tp_mapping(parsed: dict, *, label: str) -> dict[str, dict[str, int]]:
    """Validate a parsed ``{topic: {partition: int}}`` JSON mapping.

    The caller has already confirmed the top-level value is a ``dict``. Here we
    check each topic maps to an object of ``{partition: int}`` so malformed
    shapes (e.g. ``{"t": 5}`` or ``{"t": {"0": "x"}}``) fail loudly at parse
    time with a clear message, rather than crashing later during offset
    resolution with an opaque ``AttributeError``/``ValueError``.
    """
    for topic, partitions in parsed.items():
        if not isinstance(partitions, dict):
            msg = f"{label} entry for topic {topic!r} must be an object of {{partition: value}}, got: {type(partitions).__name__}"
            raise ValueError(msg)  # noqa: TRY004
        for partition, value in partitions.items():
            # Partition ids are JSON object keys, so they arrive as strings.
            # They are compared against the broker's integer partition ids, and
            # a non-numeric key would otherwise fail there with a bare
            # ``invalid literal for int()``.
            try:
                int(partition)
            except ValueError as e:
                msg = f"{label} partition id for topic {topic!r} must be an integer, got: {partition!r}"
                raise ValueError(msg) from e
            # bool is an int subclass; reject it so JSON true/false isn't
            # silently coerced to 1/0.
            if not isinstance(value, int) or isinstance(value, bool):
                msg = f"{label} value for topic {topic!r} partition {partition!r} must be an integer, got: {type(value).__name__}"
                raise ValueError(msg)  # noqa: TRY004
    return parsed


def _parse_offset_spec(raw: str, *, option_name: str) -> str | dict[str, dict[str, int]]:
    """Parse a present ``startingOffsets`` / ``endingOffsets`` value.

    Returns either the string ``"earliest"``/``"latest"`` or a mapping
    ``{topic: {partition: offset}}``. In Spark, JSON keys are strings (partition
    ids as strings).

    Spark matches the sentinels case-insensitively and parses whatever else is
    present as JSON, so a blank value is a parse error rather than "unset" —
    only an *absent* option falls back to the endpoint default. Callers reach
    this function only once the option is known to be present.
    """
    stripped = raw.strip()
    lowered = stripped.lower()
    if lowered in (_SENTINEL_EARLIEST, _SENTINEL_LATEST):
        return lowered
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as e:
        msg = f"Invalid '{option_name}' value: {raw!r}. Expected 'earliest', 'latest', or JSON."
        raise ValueError(msg) from e
    if not isinstance(parsed, dict):
        msg = f"'{option_name}' JSON must be an object, got: {type(parsed).__name__}"
        raise ValueError(msg)  # noqa: TRY004
    return _validate_tp_mapping(parsed, label=f"'{option_name}'")


def _parse_global_timestamp(raw: str, *, option_name: str) -> int:
    """Parse a global ``{starting,ending}Timestamp`` (ms since epoch)."""
    try:
        return int(raw.strip())
    except ValueError as e:
        msg = f"'{option_name}' must be an integer (ms since epoch), got {raw!r}"
        raise ValueError(msg) from e


def _parse_timestamp_json(raw: str, *, option_name: str) -> dict[str, dict[str, int]]:
    """Parse ``{starting,ending}OffsetsByTimestamp`` (per-tp JSON, ms since epoch)."""
    try:
        parsed = json.loads(raw.strip())
    except json.JSONDecodeError as e:
        msg = f"Invalid '{option_name}' JSON: {raw!r}"
        raise ValueError(msg) from e
    if not isinstance(parsed, dict):
        msg = f"'{option_name}' JSON must be an object"
        raise ValueError(msg)  # noqa: TRY004
    return _validate_tp_mapping(parsed, label=f"'{option_name}'")


def _resolve_range_option(
    opts: Mapping[str, str], *, endpoint: str
) -> tuple[str | dict[str, dict[str, int]] | None, int | dict[str, dict[str, int]] | None]:
    """Select one range option per endpoint, using Spark's precedence.

    ``KafkaSourceProvider.getKafkaOffsetRangeLimit`` picks the global timestamp
    first, then the per-partition timestamp map, then the offsets — and never
    looks at the lower-priority options, so it neither rejects combinations nor
    parses the values it skipped. Rejecting combinations here instead would fail
    queries that Spark runs happily, and parsing a skipped option would reject
    values Spark never reads (a blank ``startingOffsets`` alongside a
    ``startingTimestamp``, say).

    Returns ``(offsets_spec, timestamp_spec)`` with at most one set; both are
    ``None`` when the endpoint is unspecified and the caller applies the default
    (earliest for starting, latest for ending).
    """
    global_ts_key = f"{endpoint}Timestamp"
    per_tp_ts_key = f"{endpoint}OffsetsByTimestamp"
    offsets_key = f"{endpoint}Offsets"

    if global_ts_key in opts:
        return None, _parse_global_timestamp(opts[global_ts_key], option_name=global_ts_key)
    if per_tp_ts_key in opts:
        return None, _parse_timestamp_json(opts[per_tp_ts_key], option_name=per_tp_ts_key)
    if offsets_key in opts:
        return _parse_offset_spec(opts[offsets_key], option_name=offsets_key), None
    return None, None


def _validate_offset_spec(
    spec: str | dict[str, dict[str, int]] | None,
    *,
    endpoint: str,
    option_name: str,
) -> None:
    """Enforce Spark's batch-mode rules on an already-parsed offset spec.

    Spark's batch Kafka source rejects ``latest`` for ``startingOffsets`` and
    ``earliest`` for ``endingOffsets``, in both the string form and the
    per-partition JSON form (``-1``/``-2``). Without this check the offending
    endpoint silently resolves to a watermark that empties the range, so the
    query returns zero rows instead of failing.

    We also reject any offset below ``-2``. Only ``-1``/``-2`` are Spark
    sentinels; everything below is a librdkafka sentinel that would be honoured
    rather than rejected — notably ``-1000`` (``OFFSET_STORED``), which would
    silently read from committed consumer-group offsets, and ``-1001``
    (``OFFSET_INVALID``).

    This is deliberately separate from ``_validate_tp_mapping``, which is shared
    with the timestamp parsers where negative values are legitimate (pre-1970)
    instants.
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


def _parse_ts_strategy(opts: Mapping[str, str]) -> str:
    """Parse ``startingOffsetsByTimestampStrategy`` (Spark default: ``error``)."""
    option_name = "startingOffsetsByTimestampStrategy"
    raw = opts.get(option_name, _TS_STRATEGY_ERROR)
    value = raw.strip().lower()
    if value not in (_TS_STRATEGY_ERROR, _TS_STRATEGY_LATEST):
        msg = f"'{option_name}' must be {_TS_STRATEGY_ERROR!r} or {_TS_STRATEGY_LATEST!r}, got {raw!r}"
        raise ValueError(msg)
    return value


def _parse_bool(opts: Mapping[str, str], name: str, *, default: bool) -> bool:
    """Parse a boolean option the way Scala's ``toBoolean`` does.

    Spark calls ``.toBoolean`` on these, which accepts ``true``/``false`` in any
    case and throws on anything else. Treating every non-``true`` value as false
    would silently swallow ``includeHeaders=yes`` and ``failOnDataLoss=0``.
    """
    if name not in opts:
        return default
    value = opts[name].strip().lower()
    if value == "true":
        return True
    if value == "false":
        return False
    msg = f"'{name}' must be 'true' or 'false', got {opts[name]!r}"
    raise ValueError(msg)


def _parse_positive_int(opts: Mapping[str, str], name: str, default: str) -> int:
    """Parse a positive integer option, naming the option in any failure."""
    raw = opts.get(name, default)
    try:
        value = int(raw)
    except ValueError as e:
        msg = f"'{name}' must be an integer, got {raw!r}"
        raise ValueError(msg) from e
    if value <= 0:
        msg = f"'{name}' must be positive, got {value}"
        raise ValueError(msg)
    return value


def _parse_optional_positive_int(opts: Mapping[str, str], name: str) -> int | None:
    """Parse an optional positive integer option, returning ``None`` when absent."""
    if name not in opts:
        return None
    return _parse_positive_int(opts, name, default="")


def _parse_positive_ms(opts: Mapping[str, str], names: tuple[str, ...], default: str) -> float:
    """Parse a positive, finite millisecond option, returning it as seconds.

    ``names`` are checked in order so a Spark-standard option name can take
    precedence over an alias.

    ``float`` accepts ``nan`` and ``inf``, and neither is caught by a ``<= 0``
    check: ``inf`` makes ``Consumer.poll()`` block forever, which defeats the
    stall guard entirely, and ``nan`` makes every comparison false.
    """
    name = next((n for n in names if n in opts), names[0])
    raw = opts.get(name, default)
    try:
        value_ms = float(raw)
    except ValueError as e:
        msg = f"'{name}' must be a number of milliseconds, got {raw!r}"
        raise ValueError(msg) from e
    if not math.isfinite(value_ms):
        msg = f"'{name}' must be a finite number of milliseconds, got {raw!r}"
        raise ValueError(msg)
    if value_ms <= 0:
        msg = f"'{name}' must be positive, got {value_ms}"
        raise ValueError(msg)
    return value_ms / 1000.0


def _extract_kafka_config(opts: Mapping[str, str]) -> dict[str, str]:
    """Pass through any option prefixed with ``kafka.`` to librdkafka config.

    Matches Spark's convention: ``kafka.bootstrap.servers`` →
    ``bootstrap.servers``, ``kafka.security.protocol`` → ``security.protocol``,
    etc.

    ``opts`` is a ``CaseInsensitiveDict``, so iteration yields lowercased keys and
    ``KAFKA.Bootstrap.Servers`` is matched here just as Spark matches it. Every
    librdkafka property name is lowercase already, so lowercasing the part after
    the prefix does not change which property is set.
    """
    return {k[len("kafka.") :]: v for k, v in opts.items() if k.startswith("kafka.")}


# Consumer properties this source owns. Spark rejects the same set in
# ``KafkaSourceProvider.validateGeneralOptions`` because each one silently
# rewrites the read that was planned:
#
# * ``auto.offset.reset`` — the planner already resolved a concrete start, so a
#   reset policy can only move it. ``earliest``/``latest`` would turn a start
#   that aged out of the log into a silent jump instead of the data-loss error
#   ``failOnDataLoss`` promises.
# * ``enable.auto.commit`` — a batch read must not commit offsets; with a
#   user-supplied ``kafka.group.id`` this would corrupt that group's position.
# * ``enable.partition.eof`` — the read loop's termination path. Disabling it
#   turns a clean end-of-log stop into a stall until ``stallTimeoutMs``.
#
# The deserializer and interceptor properties Spark also protects have no
# librdkafka equivalent (messages arrive as raw bytes), so they are not listed.
_PROTECTED_KAFKA_CONFIGS = {
    "auto.offset.reset": "startingOffsets/endingOffsets",
    "enable.auto.commit": None,
    "enable.partition.eof": None,
}


def _validate_kafka_config(client_config: Mapping[str, str]) -> None:
    """Reject ``kafka.*`` options that would override this source's own settings."""
    for name, alternative in _PROTECTED_KAFKA_CONFIGS.items():
        if name in client_config:
            msg = f"Kafka option 'kafka.{name}' is not supported"
            if alternative:
                msg += f"; use the source option(s) '{alternative}' instead"
            raise ValueError(msg)


# Static group id. Spark mints a unique id per query
# (``KafkaSourceProvider.batchUniqueGroupId``) because its consumers call
# ``subscribe()`` and so join a real consumer group, where a shared id makes
# concurrent queries steal partitions from each other. Every read here uses
# ``consumer.assign()`` instead, which is librdkafka's simple-consumer path: no
# JoinGroup, no partition assignment from a coordinator, and with
# ``enable.auto.commit`` off, nothing written to ``__consumer_offsets``. The id
# is required by librdkafka but never used for coordination, so a stable one is
# safe here and keeps broker logs readable. Users can still override it with
# ``kafka.group.id``, as Spark allows.
_DEFAULT_GROUP_ID = "sail-kafka-reader"

# Consumer-only properties, stripped before building an ``AdminClient``.
# librdkafka builds the admin handle on a producer, and logs a CONFWARN for
# every consumer property it finds there. The user's ``kafka.*`` config is
# shared between both clients, so a user-supplied ``kafka.group.id`` would
# otherwise produce a warning on every planning call.
_CONSUMER_ONLY_CONFIGS = ("group.id", "enable.auto.commit", "auto.offset.reset", "enable.partition.eof")


def _build_admin_config(client_config: dict[str, str]) -> dict[str, str]:
    """Return the librdkafka config for the planning-time ``AdminClient``.

    Mirrors Spark's ``kafkaParamsForDriver``: the user's ``kafka.*`` options
    carry the connection and security settings, and the source contributes
    nothing the admin protocol does not need.
    """
    return {k: v for k, v in client_config.items() if k not in _CONSUMER_ONLY_CONFIGS}


def _build_consumer_config(client_config: dict[str, str], *, fail_on_data_loss: bool = True) -> dict[str, str | bool]:
    """Return the effective librdkafka config for a batch-read consumer.

    Only executors build consumers; planning goes through an ``AdminClient``
    configured by :func:`_build_admin_config`.

    Source-owned settings are applied *after* the user's ``kafka.*`` options, so
    they stay authoritative exactly as Spark's ``ConfigUpdater`` keeps its own
    values authoritative. ``_validate_kafka_config`` has already rejected the
    ones a user could meaningfully try to set, so this ordering only matters as
    a second line of defence.

    ``auto.offset.reset`` encodes ``failOnDataLoss``: ``error`` surfaces a start
    offset that has aged out of the log, while ``earliest`` reproduces Spark's
    ``failOnDataLoss=false`` behaviour of skipping ahead to what is still there.

    ``isolation.level`` is pinned to the Java client's default because the two
    clients disagree: librdkafka defaults to ``read_committed``, while Spark
    never sets the property and so inherits ``read_uncommitted``. Left alone, a
    topic written by a transactional producer would silently return a different
    row set here than under Spark — aborted records dropped, and nothing past
    the last stable offset — with planning still using the high watermark, so
    the short read would look like a clean one. It is placed before the user's
    config so ``kafka.isolation.level`` can still opt into ``read_committed``.
    """
    return {
        "group.id": _DEFAULT_GROUP_ID,
        "isolation.level": "read_uncommitted",
        **client_config,  # user config first...
        # ...source-owned settings last, so they win.
        "enable.auto.commit": False,
        "auto.offset.reset": "error" if fail_on_data_loss else _SENTINEL_EARLIEST,
        "enable.partition.eof": True,
    }


# ============================================================================
# Subscription strategy — assign / subscribe / subscribePattern
# ============================================================================


def _parse_subscription(opts: Mapping[str, str]) -> tuple[str, object]:
    """Parse Spark's three topic-selection strategies.

    Spark requires exactly one of ``assign``, ``subscribe``, and
    ``subscribePattern``, rejecting both zero and more than one. Returns the
    strategy name and its parsed value: a topic list for ``subscribe``, a
    compiled regex for ``subscribePattern``, and a ``{topic: [partition, ...]}``
    mapping for ``assign``.
    """
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
        # Dedupe: a repeated topic would otherwise be enumerated once per
        # occurrence, producing duplicate input partitions over the same offset
        # range and emitting every row twice. `dict.fromkeys` preserves
        # first-seen order so planning stays stable.
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

    try:
        parsed = json.loads(raw.strip())
    except json.JSONDecodeError as e:
        msg = f"Invalid 'assign' JSON: {raw!r}. Expected {{\"topic\": [partition, ...]}}."
        raise ValueError(msg) from e
    if not isinstance(parsed, dict) or not parsed:
        msg = f"'assign' JSON must be a non-empty object of {{topic: [partition, ...]}}, got: {raw!r}"
        raise ValueError(msg)
    for topic, partitions in parsed.items():
        if not isinstance(partitions, list) or not partitions:
            msg = f"'assign' entry for topic {topic!r} must be a non-empty array of partition ids"
            raise ValueError(msg)
        for partition in partitions:
            if not isinstance(partition, int) or isinstance(partition, bool) or partition < 0:
                msg = f"'assign' partition id for topic {topic!r} must be a non-negative integer, got: {partition!r}"
                raise ValueError(msg)
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
        client_config: dict[str, str],
        include_headers: bool,
        poll_timeout_s: float,
        max_batch_rows: int,
        stall_timeout_s: float,
        admin_timeout_s: float,
        fail_on_data_loss: bool,
    ) -> None:
        super().__init__(partition_id)
        self.topic = topic
        self.kafka_partition = kafka_partition
        self.start_offset = start_offset
        self.end_offset = end_offset  # exclusive
        self.client_config = client_config
        self.include_headers = include_headers
        self.poll_timeout_s = poll_timeout_s
        self.max_batch_rows = max_batch_rows
        self.stall_timeout_s = stall_timeout_s
        self.admin_timeout_s = admin_timeout_s
        self.fail_on_data_loss = fail_on_data_loss


# ============================================================================
# Offset range splitting — minPartitions / maxRecordsPerPartition
# ============================================================================


def _divide_range(start: int, end: int, parts: int) -> list[tuple[int, int]]:
    """Split ``[start, end)`` into ``parts`` contiguous chunks.

    Mirrors ``KafkaOffsetRangeCalculator.getDividedPartition``, which takes
    ``remaining / (parts - i)`` records for chunk ``i``. Chunks differ by at most
    one record, and because each chunk is sized against what is left, the
    remainder lands in the *last* chunks — dividing ten records three ways gives
    3/3/4, not 4/3/3. Distributing it the other way would put every split
    boundary in a different place than Spark.

    Empty chunks are dropped, as Spark drops them with ``filter(_.size > 0)``.
    """
    remaining = end - start
    parts = max(parts, 1)  # callers guarantee this; never silently drop a range
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
    """Reject an inverted range and drop an empty one.

    Only reachable once both endpoints are concrete. Spark applies the same two
    rules — ``KafkaSourceRDD.compute`` asserts ``fromOffset <= untilOffset`` and
    returns an empty iterator when they are equal — but does so per task, since
    a late-bound range has no size until it is read.
    """
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
    admin: AdminClient, ranges: list[tuple[str, int, int, int]], *, timeout_s: float
) -> list[tuple[str, int, int, int]]:
    """Bind any ``earliest``/``latest`` sentinel still present in ``ranges``.

    ``_list_offsets`` is keyed by partition, so a partition needing both
    watermarks cannot ask for them in one request; the two endpoints are batched
    into one call each instead.
    """
    need_low = {(topic, part) for topic, part, start, _ in ranges if start == _SPARK_OFFSET_EARLIEST}
    need_high = {(topic, part) for topic, part, _, end in ranges if end == _SPARK_OFFSET_LATEST}
    if not need_low and not need_high:
        return ranges

    lows = _list_offsets(admin, {k: OffsetSpec.earliest() for k in need_low}, timeout_s=timeout_s)
    highs = _list_offsets(admin, {k: OffsetSpec.latest() for k in need_high}, timeout_s=timeout_s)
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
    """Put each partition's original endpoints back on its first and last chunk.

    Mirrors ``getOffsetRangesFromUnresolvedOffsets``, which after dividing a
    range copies the pre-resolution ``fromOffset`` onto the head chunk and
    ``untilOffset`` onto the last one. Splitting therefore fixes only the
    *interior* boundaries; a late-bound start or end stays late-bound.
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
    """How many chunks a range of ``size`` gets, per Spark's ``getPartCount``.

    Scala's ``math.round`` is floor(x + 0.5); Python's ``round`` is banker's
    rounding, which would place split boundaries differently.
    """
    return max(math.floor(size / total_size * min_parts + 0.5), 1)


def _split_offset_ranges(
    ranges: list[tuple[str, int, int, int]],
    *,
    min_partitions: int | None,
    max_records_per_partition: int | None,
) -> list[tuple[str, int, int, int]]:
    """Apply Spark's ``minPartitions``/``maxRecordsPerPartition`` splitting.

    ``KafkaOffsetRangeCalculator.getRanges`` first caps each range at
    ``maxRecordsPerPartition``, then — if the result still has fewer partitions
    than ``minPartitions`` — splits ranges further in proportion to their size.
    Ignoring these options would silently change both parallelism and the
    observable partition count relative to Spark.
    """
    if max_records_per_partition is not None:
        divided: list[tuple[str, int, int, int]] = []
        for topic, partition, start, end in ranges:
            parts = math.ceil((end - start) / max_records_per_partition)
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

    # Spark splits in two passes. Ranges whose proportional share rounds down to
    # a single chunk are set aside first, and the remaining `minPartitions`
    # budget is then divided among the rest against *their* combined size only.
    # A single proportional pass would let one large range claim the whole
    # budget while the small ranges still contribute a partition each, so the
    # result overshoots: sizes [100, 1, 1] with minPartitions=4 would yield 6
    # ranges instead of Spark's 4.
    #
    # `unsplit` is keyed by (topic, partition) rather than by range, matching
    # Spark's `unsplitRangeTopicPartitions`: when `maxRecordsPerPartition` has
    # already divided one Kafka partition, one small piece pins every piece of
    # that partition to a single chunk.
    unsplit = [r for r in ranges if _part_count(r[3] - r[2], total_size, min_partitions) == 1]
    unsplit_tps = {(topic, partition) for topic, partition, _, _ in unsplit}
    split_total_size = total_size - sum(end - start for _, _, start, end in unsplit)
    split_min_partitions = max(min_partitions - len(unsplit), 1)

    out: list[tuple[str, int, int, int]] = []
    for topic, partition, start, end in ranges:
        # `split_total_size` is only zero when every range is unsplit, and then
        # this branch is never taken, so it cannot divide by zero.
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
        self.stall_timeout_s = stall_timeout_s
        self.admin_timeout_s = admin_timeout_s
        self.fail_on_data_loss = fail_on_data_loss
        self.min_partitions = min_partitions
        self.max_records_per_partition = max_records_per_partition

    # ------------------------------------------------------------------
    # Partition planning — talks to the broker once on the driver
    # ------------------------------------------------------------------

    def partitions(self) -> list[InputPartition]:
        # A short-lived AdminClient enumerates partitions and resolves offsets,
        # as Spark's `KafkaOffsetReaderAdmin` does. Executors get their own
        # consumer; nothing here is reused across the driver/executor boundary.
        # AdminClient exposes no `close()` — the handle is released when it goes
        # out of scope, so there is nothing to unwind in a `finally`.
        #
        # `earliest`/`latest` are deliberately *not* resolved here. Spark's
        # `fetchPartitionOffsets` returns the -2/-1 sentinels for those limits
        # ("Obtain TopicPartition offsets with late binding support") and binds
        # them per task in `KafkaSourceRDD.resolveRange`. Freezing them on the
        # driver would drop records produced between planning and task start,
        # and — worse — an `earliest` start frozen at planning can age out of
        # the log before the task runs, failing a query Spark completes by
        # simply re-resolving. Explicit offsets and timestamps are resolved
        # here, as they are in Spark.
        admin = AdminClient(_build_admin_config(self.client_config))
        admin_timeout_s = self.admin_timeout_s

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
            )
        else:
            ends = _map_offset_spec(tps, self.ending_offsets or _SENTINEL_LATEST)

        ranges = [
            (tp.topic, tp.partition, starts[(tp.topic, tp.partition)], ends[(tp.topic, tp.partition)]) for tp in tps
        ]

        # Splitting needs concrete sizes, so it forces the sentinels to be
        # resolved. Spark does the same in `getOffsetRangesFromUnresolvedOffsets`
        # and then puts the unresolved endpoints back on the outer chunks, so
        # the overall range stays late-bound even when it has been divided.
        needs_split = self.max_records_per_partition is not None or (
            self.min_partitions is not None and self.min_partitions > len(ranges)
        )
        if needs_split:
            outer = {(topic, part): (start, end) for topic, part, start, end in ranges}
            ranges = _resolve_sentinels(admin, ranges, timeout_s=admin_timeout_s)
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
                client_config=self.client_config,
                include_headers=self.include_headers,
                poll_timeout_s=self.poll_timeout_s,
                max_batch_rows=self.max_batch_rows,
                stall_timeout_s=self.stall_timeout_s,
                admin_timeout_s=self.admin_timeout_s,
                fail_on_data_loss=self.fail_on_data_loss,
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

        schema = _build_schema(include_headers=partition.include_headers)

        consumer = Consumer(
            _build_consumer_config(partition.client_config, fail_on_data_loss=partition.fail_on_data_loss)
        )
        try:
            start, end = _bind_partition_range(consumer, partition)
            if _check_range((partition.topic, partition.kafka_partition, start, end)) is None:
                return  # empty range; nothing to read

            tp = TopicPartition(partition.topic, partition.kafka_partition, start)
            consumer.assign([tp])

            buf = _RowBuffer(include_headers=partition.include_headers)
            next_offset = start
            # Wall-clock, not a poll count: the stall bound must not scale with
            # `pollTimeoutMs`. Spark's 120s default for that option would turn a
            # 30-poll budget into an hour of silence before the read gave up.
            last_progress = time.monotonic()

            while next_offset < end:
                record = consumer.poll(partition.poll_timeout_s)
                if record is None:
                    # No message within poll timeout. Normally transient — the
                    # broker is slow, or a fetch is in flight. But if this
                    # keeps happening we're wedged (unreachable broker, dead
                    # partition leader) and would hang forever without a cap.
                    idle_s = time.monotonic() - last_progress
                    if idle_s > partition.stall_timeout_s:
                        msg = (
                            f"Kafka read stalled: no messages for {idle_s:.0f}s on "
                            f"{partition.topic}[{partition.kafka_partition}] at offset "
                            f"{next_offset}, expected up to {end}. Broker may be "
                            f"unreachable or the partition leader unavailable. "
                            f"Raise 'stallTimeoutMs' if the topic is simply idle."
                        )
                        raise TimeoutError(msg)
                    continue
                if record.error():
                    if record.error().code() == KafkaError._PARTITION_EOF:  # noqa: SLF001
                        _check_eof_data_loss(consumer, tp, partition, next_offset=next_offset, end_offset=end)
                        break
                    raise KafkaException(record.error())

                last_progress = time.monotonic()

                if record.offset() >= end:
                    break
                if record.offset() < start:
                    # librdkafka applies `auto.offset.reset` to an out-of-range
                    # start in *both* directions. Under `failOnDataLoss=false`
                    # the policy is `earliest`, so a start past the end of the
                    # log does not read nothing — it rewinds to the beginning of
                    # the log and delivers records the planner never asked for.
                    # Spark returns no rows there instead
                    # (`getEarliestAvailableOffsetBetween` gives up once
                    # `offset >= latest`), so drop anything below the planned
                    # start rather than emitting the whole partition.
                    continue

                buf.append(record)
                next_offset = record.offset() + 1

                if len(buf) >= partition.max_batch_rows:
                    yield buf.to_batch(schema)
                    buf = _RowBuffer(include_headers=partition.include_headers)

            if len(buf) > 0:
                yield buf.to_batch(schema)
        finally:
            consumer.close()


def _bind_partition_range(consumer: Consumer, partition: KafkaInputPartition) -> tuple[int, int]:
    """Bind this partition's ``earliest``/``latest`` sentinels at task start.

    Mirrors ``KafkaSourceRDD.resolveRange``, which late-binds any negative
    endpoint against the watermarks the consumer sees when the task actually
    runs. Doing it here rather than on the driver is what lets the read pick up
    records produced since planning, and what keeps an ``earliest`` start valid
    when the log has aged on in the meantime.

    Fully concrete ranges skip the broker round-trip entirely.
    """
    start, end = partition.start_offset, partition.end_offset
    if start >= 0 and end >= 0:
        return start, end
    low, high = consumer.get_watermark_offsets(
        TopicPartition(partition.topic, partition.kafka_partition),
        timeout=partition.admin_timeout_s,
        cached=False,
    )
    if start == _SPARK_OFFSET_EARLIEST:
        start = low
    if end == _SPARK_OFFSET_LATEST:
        end = high
    return start, end


def _check_eof_data_loss(
    consumer: Consumer,
    tp: TopicPartition,
    partition: KafkaInputPartition,
    *,
    next_offset: int,
    end_offset: int,
) -> None:
    """Decide whether an end-of-partition event before the planned end is data loss.

    EOF means the consumer reached the end of the log. Reaching it below the
    planned exclusive end has two very different causes, and Spark distinguishes
    them:

    * The high watermark still covers the planned end. The offsets we did not
      see were within the available range and are simply not readable — compacted
      away, transaction control markers, or aborted records if the user opted
      into ``read_committed``. Spark skips those and keeps going, so stopping
      cleanly here matches it.
    * The high watermark has fallen below the planned end. Records that existed
      at planning time are gone (retention or truncation), or an explicit
      ``endingOffsets`` points past the log. Spark reports data loss, which under
      the default ``failOnDataLoss=true`` fails the query.

    ``end_offset`` is the bound end for this task, which for a late-bound
    ``latest`` is the high watermark as of task start rather than planning time.
    """
    if next_offset >= end_offset:
        return
    _, high = consumer.get_watermark_offsets(tp, timeout=partition.admin_timeout_s, cached=False)
    if high >= end_offset:
        return
    if not partition.fail_on_data_loss:
        return
    msg = (
        f"Data loss on {partition.topic}[{partition.kafka_partition}]: reached end of log at offset "
        f"{next_offset}, but the read was planned up to {end_offset} and the high watermark "
        f"is now {high}. Records were removed after planning (retention or truncation), or "
        f"'endingOffsets' points past the end of the log. "
        f"Set 'failOnDataLoss' to false to return the records that remain."
    )
    raise ValueError(msg)


# ============================================================================
# Row buffer — accumulates messages, flushes to a RecordBatch
# ============================================================================


class _RowBuffer:
    """Column-major accumulator sized to ``max_batch_rows``."""

    def __init__(self, *, include_headers: bool) -> None:
        self.include_headers = include_headers
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

    def append(self, record) -> None:
        ts_type, ts_ms = record.timestamp()  # librdkafka: 0=NotAvailable, 1=Create, 2=LogAppend
        self.key.append(record.key())
        self.value.append(record.value())
        self.topic.append(record.topic())
        self.partition.append(record.partition())
        self.offset.append(record.offset())
        # Spark emits the -1ms Kafka reports for an untimed record rather than
        # null, surfacing 1969-12-31 23:59:59.999; only `timestampType` is -1.
        # Normalising here keeps that exact value whatever librdkafka reports
        # alongside NOT_AVAILABLE.
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
        return pa.record_batch(arrays, schema=schema)


# ============================================================================
# Broker helpers — enumerate partitions, resolve watermarks
# ============================================================================


# Extra wall-clock budget allowed on top of a request's own timeout before we
# give up waiting for its future. librdkafka bounds the request with
# ``request_timeout`` and completes the future with an error, so under normal
# failure this margin is never reached and the broker's own message wins. It
# exists only so that a future which never resolves at all cannot wedge planning
# forever, which a bare ``Future.result()`` would.
_FUTURE_TIMEOUT_MARGIN_S = 5.0


def _await(future, *, timeout_s: float, what: str):
    """Resolve an admin-call future, refusing to wait indefinitely."""
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
    """Describe ``topics``, returning ``{topic: [partition, ...]}`` for the non-internal ones.

    Mirrors Spark's ``ConsumerStrategy.retrieveAllPartitions``, which calls
    ``admin.describeTopics(...)`` and drops whatever the broker flags with
    ``isInternal``. That flag is the authority: only ``__consumer_offsets`` and
    ``__transaction_state`` carry it, so a *user* topic that happens to start
    with ``__`` stays visible, exactly as it does under Spark. Matching on the
    name prefix instead would silently hide it.

    One request covers every topic, as Spark's does.
    """
    futures = admin.describe_topics(TopicCollection(topics), request_timeout=timeout_s)
    out: dict[str, list[int]] = {}
    for topic, future in futures.items():
        try:
            description = _await(future, timeout_s=timeout_s, what=f"describe topic {topic!r}")
        except KafkaException as e:
            # A per-topic failure is not necessarily a missing topic: it also
            # covers authorization failures (``TOPIC_AUTHORIZATION_FAILED``) and
            # leader-election churn. Reporting all of those as "not found" sends
            # users looking for the wrong misconfiguration, so relay what the
            # broker actually said.
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
    """Enumerate the (topic, partition) pairs selected by the subscription."""
    strategy, value = subscription

    if strategy == "subscribePattern":
        # Only the pattern strategy needs the full topic list; the other two
        # name their topics up front and can go straight to `describe_topics`.
        metadata = admin.list_topics(timeout=timeout_s)
        names = sorted(topic for topic in metadata.topics if value.fullmatch(topic))
        if not names:
            return []
        described = _describe_topics(admin, names, timeout_s=timeout_s)
        return [TopicPartition(topic, p) for topic in sorted(described) for p in described[topic]]

    topics = list(value) if strategy == "subscribe" else list(value.keys())
    described = _describe_topics(admin, topics, timeout_s=timeout_s)

    out: list[TopicPartition] = []
    for topic in topics:
        if topic not in described:
            # The topic exists but the broker flags it internal. Spark's
            # `retrieveAllPartitions` filters it out of every strategy, leaving
            # a read with no partitions rather than an error.
            continue
        available = described[topic]
        if strategy == "subscribe":
            out.extend(TopicPartition(topic, p) for p in available)
            continue
        missing = sorted(set(value[topic]) - set(available))
        if missing:
            msg = (
                f"'assign' names partitions that do not exist for topic {topic!r}: {missing}. "
                f"Available partitions: {available}"
            )
            raise ValueError(msg)
        out.extend(TopicPartition(topic, p) for p in sorted(set(value[topic])))
    return out


def _validate_spec_partitions(
    spec: str | int | dict[str, dict[str, int]] | None,
    tps: list[TopicPartition],
    *,
    option_name: str,
) -> None:
    """Require a per-partition spec to name exactly the assigned partitions.

    Spark asserts ``partitions == partitionOffsets.keySet`` once the broker's
    partitions are known ("If startingOffsets contains specific offsets, you
    must specify all TopicPartitions"). Without that check an omitted partition
    silently falls back to a watermark and an extra or misspelled one is
    dropped, so a typo turns into a successful read of the wrong data. String
    sentinels and global timestamps have no partition keys and pass through.
    """
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
    admin: AdminClient, specs: dict[tuple[str, int], object], *, timeout_s: float
) -> dict[tuple[str, int], int]:
    """Resolve ``{(topic, partition): OffsetSpec}`` to concrete offsets in one request.

    Spark's ``KafkaOffsetReaderAdmin`` resolves every partition with a single
    ``admin.listOffsets`` call. Asking per partition instead would turn planning
    a wide topic into one blocking round-trip per partition.

    ``isolation_level`` is pinned to ``READ_UNCOMMITTED``, matching both the
    Java admin client's default (which is what Spark gets) and the reader's own
    ``isolation.level``. Under ``READ_COMMITTED`` the broker answers a "latest"
    query with the last stable offset instead of the high watermark, so an open
    transaction would silently shrink the planned range.
    """
    if not specs:
        return {}
    futures = admin.list_offsets(
        {TopicPartition(topic, partition): spec for (topic, partition), spec in specs.items()},
        isolation_level=IsolationLevel.READ_UNCOMMITTED,
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
    """Map a starting/ending-offset spec to a per-partition offset.

    ``earliest``/``latest`` — in either the string form or the JSON ``-2``/``-1``
    form — are returned as those sentinels rather than resolved, and are bound
    per task in :func:`_bind_partition_range`. Spark's ``fetchPartitionOffsets``
    does the same, which is what lets a batch read pick up records produced
    after planning and keeps an ``earliest`` start valid if the log ages on.

    No broker call is made here; explicit numeric offsets are used as given.
    """
    if spec == _SENTINEL_EARLIEST:
        return {(tp.topic, tp.partition): _SPARK_OFFSET_EARLIEST for tp in tps}
    if spec == _SENTINEL_LATEST:
        return {(tp.topic, tp.partition): _SPARK_OFFSET_LATEST for tp in tps}

    # Explicit per-topic-partition JSON. `_validate_spec_partitions` has already
    # required an entry for every assigned partition, so a lookup miss is not
    # reachable here. `-1`/`-2` are already the sentinel values, so they pass
    # through unchanged.
    return {(tp.topic, tp.partition): int(spec[tp.topic][str(tp.partition)]) for tp in tps}


def _resolve_by_timestamp(
    admin: AdminClient,
    tps: list[TopicPartition],
    spec: int | dict[str, dict[str, int]],
    *,
    endpoint: str,
    timeout_s: float,
    strategy: str,
) -> dict[tuple[str, int], int]:
    """Resolve a timestamp spec to concrete offsets via a timestamp ``list_offsets``.

    ``spec`` is either a global ``int`` (ms since epoch, applied to every
    partition) or a mapping ``{topic: {partition_str: ts_ms}}``.

    When a partition holds no message with ``ts >= T``, Spark's behaviour
    depends on the endpoint: an ending timestamp falls back to the latest
    offset, while a starting timestamp follows
    ``startingOffsetsByTimestampStrategy`` — ``error`` (the default) fails the
    query, and ``latest`` bounds the read to what is currently in the log.
    Always falling back would silently return an empty result for a mistaken or
    future starting timestamp.

    Note: the lookup searches the topic's actual stored timestamps. If the topic
    is configured with ``message.timestamp.type=LogAppendTime`` the search is
    against broker ingest time; otherwise it's against producer clocks
    (``CreateTime``).
    """
    if isinstance(spec, int):
        wanted = {(tp.topic, tp.partition): OffsetSpec.for_timestamp(spec) for tp in tps}
    else:
        # `_validate_spec_partitions` has already required an entry per assigned
        # partition, so every lookup below is present.
        wanted = {
            (tp.topic, tp.partition): OffsetSpec.for_timestamp(int(spec[tp.topic][str(tp.partition)])) for tp in tps
        }

    # A broker-side failure surfaces as a raised `KafkaException` inside
    # `_list_offsets`, which reports it rather than letting it read as "no
    # timestamp match" — that would turn a lookup failure into a silently empty
    # (starting) or widened (ending) read.
    resolved = _list_offsets(admin, wanted, timeout_s=timeout_s)

    # Partitions with no message at or after the timestamp come back as -1.
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
        )
        resolved.update(fallback)
    return resolved


# ============================================================================
# DataSource
# ============================================================================


class KafkaDataSource(DataSource):
    """Kafka batch data source backed by confluent-kafka.

    Every ``.load()`` resolves to a bounded ``[start, end)`` range per
    partition on the driver, then reads that range on executors. There is
    no streaming / continuous mode.

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

    Options (as in Spark's Kafka source; option names are case-insensitive):

    +--------------------------------+----------+----------+------------------------------------------+
    | Option                         | Required | Default  | Description                              |
    +================================+==========+==========+==========================================+
    | kafka.bootstrap.servers        | Yes      |          | Broker list                              |
    +--------------------------------+----------+----------+------------------------------------------+
    | assign                         | One of   |          | ``{"topic": [partition, ...]}`` JSON     |
    +--------------------------------+ the      +----------+------------------------------------------+
    | subscribe                      | three    |          | Comma-separated topic names              |
    +--------------------------------+          +----------+------------------------------------------+
    | subscribePattern               |          |          | Topic-name regular expression            |
    +--------------------------------+----------+----------+------------------------------------------+
    | startingOffsets                | No       | earliest | "earliest" / per-tp JSON                 |
    +--------------------------------+----------+----------+------------------------------------------+
    | endingOffsets                  | No       | latest   | "latest" / per-tp JSON                   |
    +--------------------------------+----------+----------+------------------------------------------+
    | startingTimestamp              | No       |          | Global ms since epoch                    |
    +--------------------------------+----------+----------+------------------------------------------+
    | endingTimestamp                | No       |          | Global ms since epoch                    |
    +--------------------------------+----------+----------+------------------------------------------+
    | startingOffsetsByTimestamp     | No       |          | Per-tp timestamp JSON (ms since epoch)   |
    +--------------------------------+----------+----------+------------------------------------------+
    | endingOffsetsByTimestamp       | No       |          | Per-tp timestamp JSON (ms since epoch)   |
    +--------------------------------+----------+----------+------------------------------------------+
    | startingOffsetsByTimestampStrategy | No   | error    | ``error`` / ``latest`` on no match       |
    +--------------------------------+----------+----------+------------------------------------------+
    | failOnDataLoss                 | No       | true     | Fail when planned records are gone       |
    +--------------------------------+----------+----------+------------------------------------------+
    | includeHeaders                 | No       | false    | Include the ``headers`` column           |
    +--------------------------------+----------+----------+------------------------------------------+
    | minPartitions                  | No       |          | Lower bound on input partitions          |
    +--------------------------------+----------+----------+------------------------------------------+
    | maxRecordsPerPartition         | No       |          | Upper bound on records per partition     |
    +--------------------------------+----------+----------+------------------------------------------+
    | kafkaConsumer.pollTimeoutMs    | No       | 120000   | ``consumer.poll()`` timeout; also        |
    |                                |          |          | accepted as ``pollTimeoutMs``            |
    +--------------------------------+----------+----------+------------------------------------------+
    | maxBatchRows                   | No       | 10000    | Max rows per Arrow RecordBatch (Sail)    |
    +--------------------------------+----------+----------+------------------------------------------+
    | stallTimeoutMs                 | No       | 300000   | Idle time before failing a read (Sail)   |
    +--------------------------------+----------+----------+------------------------------------------+
    | adminTimeoutMs                 | No       | 10000    | Metadata / offset-lookup timeout (Sail)  |
    +--------------------------------+----------+----------+------------------------------------------+
    | kafka.*                        | No       |          | Any extra librdkafka client config       |
    +--------------------------------+----------+----------+------------------------------------------+

    Where both an offsets option and a timestamp option are given for the same
    endpoint, Spark's precedence applies: the global timestamp wins, then the
    per-partition timestamp map, then the offsets. As in Spark's batch source,
    ``latest`` (and its JSON form ``-1``) is rejected for ``startingOffsets``,
    and ``earliest`` (``-2``) for ``endingOffsets``; per-partition JSON must
    name every assigned partition. Timestamp-based reads resolve through a
    timestamp ``list_offsets`` lookup, which searches the topic's stored
    timestamps — configure the topic with ``message.timestamp.type=LogAppendTime``
    if you want the search against broker ingest time rather than producer clocks.

    Not supported: writes, streaming reads, and filter pushdown.

    Planning cost: partitions are enumerated with one ``describe_topics`` call.
    ``earliest``/``latest`` need no call at all, being bound per task; timestamps
    and any endpoint that ``minPartitions``/``maxRecordsPerPartition`` forces to
    resolve early take one batched ``list_offsets`` call per endpoint, however
    many partitions are involved — the same shape as Spark's
    ``KafkaOffsetReaderAdmin``. ``adminTimeoutMs`` bounds each of them.
    """

    @classmethod
    def name(cls) -> str:
        return "kafka"

    @cached_property
    def _resolved(self) -> dict:
        # Spark data source options are case-insensitive. Sail and PySpark both
        # construct this class with a ``CaseInsensitiveDict``; re-wrapping keeps
        # that behavior when the class is constructed directly (as the parsing
        # tests do) and — unlike ``dict(self.options)`` — does not flatten the
        # mapping back into a case-sensitive one, which would strand every
        # camelCase lookup below against lowercased keys.
        opts = CaseInsensitiveDict(self.options)

        client_config = _extract_kafka_config(opts)
        if "bootstrap.servers" not in client_config:
            msg = "Option 'kafka.bootstrap.servers' is required for the kafka data source"
            raise ValueError(msg)
        _validate_kafka_config(client_config)

        subscription = _parse_subscription(opts)

        starting_offsets, starting_ts = _resolve_range_option(opts, endpoint="starting")
        ending_offsets, ending_ts = _resolve_range_option(opts, endpoint="ending")

        _validate_offset_spec(starting_offsets, endpoint="starting", option_name="startingOffsets")
        _validate_offset_spec(ending_offsets, endpoint="ending", option_name="endingOffsets")

        include_headers = _parse_bool(opts, "includeHeaders", default=False)
        fail_on_data_loss = _parse_bool(opts, "failOnDataLoss", default=True)
        max_batch_rows = _parse_positive_int(opts, "maxBatchRows", "10000")
        stall_timeout_s = _parse_positive_ms(opts, ("stallTimeoutMs",), "300000")
        min_partitions = _parse_optional_positive_int(opts, "minPartitions")
        max_records_per_partition = _parse_optional_positive_int(opts, "maxRecordsPerPartition")
        # A negative poll timeout makes ``consumer.poll()`` block indefinitely,
        # which defeats the ``stallTimeoutMs`` stall guard entirely — that guard
        # can only count polls that return. Zero spins the read loop instead.
        # Neither is a usable configuration, so both are rejected.
        #
        # The default matches Spark's batch source, which falls back to
        # ``spark.network.timeout`` (120s) for ``kafkaConsumer.pollTimeoutMs``.
        poll_timeout_s = _parse_positive_ms(opts, ("kafkaConsumer.pollTimeoutMs", "pollTimeoutMs"), "120000")
        admin_timeout_s = _parse_positive_ms(opts, ("adminTimeoutMs",), "10000")

        return {
            "subscription": subscription,
            "starting_offsets": starting_offsets,
            "ending_offsets": ending_offsets,
            "starting_timestamps": starting_ts,
            "ending_timestamps": ending_ts,
            "starting_ts_strategy": _parse_ts_strategy(opts),
            "client_config": client_config,
            "include_headers": include_headers,
            "poll_timeout_s": poll_timeout_s,
            "max_batch_rows": max_batch_rows,
            "stall_timeout_s": stall_timeout_s,
            "admin_timeout_s": admin_timeout_s,
            "fail_on_data_loss": fail_on_data_loss,
            "min_partitions": min_partitions,
            "max_records_per_partition": max_records_per_partition,
        }

    def schema(self) -> pa.Schema:
        return _build_schema(include_headers=self._resolved["include_headers"])

    def reader(self, schema: pa.Schema) -> KafkaDataSourceReader:
        # Spark's Kafka source has a fixed schema and rejects a user-specified
        # one outright. Sail hands whatever schema the caller supplied straight
        # through to this method, so without a check here planning would
        # advertise the user's columns while `read()` kept emitting Kafka's.
        #
        # Column names and types are compared, but not nullability: the schema
        # Sail passes back has made a round trip through Arrow's C data
        # interface, and a stricter comparison risks rejecting our own schema
        # over a detail that cannot change what `read()` produces.
        expected = _build_schema(include_headers=self._resolved["include_headers"])
        if schema is not None and (schema.names != expected.names or schema.types != expected.types):
            msg = (
                f"The kafka data source has a fixed schema and does not accept a user-specified one.\n"
                f"Expected: {expected}\nGot: {schema}"
            )
            raise ValueError(msg)
        return KafkaDataSourceReader(**self._resolved)
