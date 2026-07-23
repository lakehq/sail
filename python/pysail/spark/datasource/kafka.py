"""Kafka batch data source for Sail, backed by confluent-kafka (librdkafka).

Exposes ``spark.read.format("kafka")`` with a Spark-compatible schema and
option surface. Every read resolves to a bounded ``[start_offset, end_offset)``
range per partition at planning time.

Install the optional dependency before use::

    pip install pysail[kafka]
"""

from __future__ import annotations

import json
from functools import cached_property
from typing import TYPE_CHECKING

import pyarrow as pa

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
except ImportError as e:
    msg = "confluent-kafka is required for the kafka data source. Install it with: pip install pysail[kafka]"
    raise ImportError(msg) from e

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
except ImportError as e:
    msg = "PySpark with the DataSource API is required (PySpark >= 4.0)"
    raise ImportError(msg) from e

if TYPE_CHECKING:
    from collections.abc import Iterator


# ============================================================================
# Fixed Spark-compatible schema
# ============================================================================

# Matches ``org.apache.spark.sql.kafka010.KafkaSource``'s output schema.
# ``headers`` is only appended when the ``includeHeaders`` option is true.

_BASE_FIELDS: list[pa.Field] = [
    pa.field("key", pa.binary(), nullable=True),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("topic", pa.string(), nullable=False),
    pa.field("partition", pa.int32(), nullable=False),
    pa.field("offset", pa.int64(), nullable=False),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("timestampType", pa.int32(), nullable=False),
]

_HEADER_STRUCT = pa.struct(
    [pa.field("key", pa.string(), nullable=False), pa.field("value", pa.binary(), nullable=True)]
)

# Spark's ``org.apache.kafka.common.record.TimestampType`` uses -1/0/1,
# but librdkafka emits 0/1/2. Translate on the way out so downstream code
# that keys off Spark's enum values (e.g. ``timestampType == 0`` for
# CreateTime) behaves the same as against the JVM Kafka source.
_LIBRDKAFKA_TO_SPARK_TS_TYPE = {
    0: -1,  # NOT_AVAILABLE     -> NO_TIMESTAMP_TYPE
    1: 0,  # CREATE_TIME       -> CREATE_TIME
    2: 1,  # LOG_APPEND_TIME   -> LOG_APPEND_TIME
}


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
            # bool is an int subclass; reject it so JSON true/false isn't
            # silently coerced to 1/0.
            if not isinstance(value, int) or isinstance(value, bool):
                msg = f"{label} value for topic {topic!r} partition {partition!r} must be an integer, got: {type(value).__name__}"
                raise ValueError(msg)  # noqa: TRY004
    return parsed


def _parse_offset_spec(raw: str | None, *, default: str | None) -> str | dict[str, dict[str, int]] | None:
    """Parse ``startingOffsets`` / ``endingOffsets``.

    Returns either the string ``"earliest"``/``"latest"``, a mapping
    ``{topic: {partition: offset}}``, or ``None`` if unset (caller supplies
    the default). In Spark, JSON keys are strings (partition ids as strings).
    """
    if raw is None or not raw.strip():
        return default
    stripped = raw.strip()
    if stripped in (_SENTINEL_EARLIEST, _SENTINEL_LATEST):
        return stripped
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as e:
        msg = f"Invalid offset spec: {raw!r}. Expected 'earliest', 'latest', or JSON."
        raise ValueError(msg) from e
    if not isinstance(parsed, dict):
        msg = f"Offset spec JSON must be an object, got: {type(parsed).__name__}"
        raise ValueError(msg)  # noqa: TRY004
    return _validate_tp_mapping(parsed, label="Offset spec")


def _parse_timestamp_spec(
    global_raw: str | None,
    per_tp_raw: str | None,
    *,
    endpoint: str,
) -> int | dict[str, dict[str, int]] | None:
    """Parse ``{starting,ending}Timestamp`` (global ms) and
    ``{starting,ending}OffsetsByTimestamp`` (per-tp JSON).

    Returns an ``int`` (global ms), a mapping ``{topic: {partition: ts_ms}}``,
    or ``None`` if unset. Global and per-tp are mutually exclusive.
    """
    global_set = global_raw is not None and global_raw.strip() != ""
    per_tp_set = per_tp_raw is not None and per_tp_raw.strip() != ""
    if global_set and per_tp_set:
        msg = f"'{endpoint}Timestamp' and '{endpoint}OffsetsByTimestamp' are mutually exclusive"
        raise ValueError(msg)
    if global_set:
        try:
            return int(global_raw.strip())
        except ValueError as e:
            msg = f"'{endpoint}Timestamp' must be an integer (ms since epoch), got {global_raw!r}"
            raise ValueError(msg) from e
    if per_tp_set:
        try:
            parsed = json.loads(per_tp_raw.strip())
        except json.JSONDecodeError as e:
            msg = f"Invalid '{endpoint}OffsetsByTimestamp' JSON: {per_tp_raw!r}"
            raise ValueError(msg) from e
        if not isinstance(parsed, dict):
            msg = f"'{endpoint}OffsetsByTimestamp' JSON must be an object"
            raise ValueError(msg)
        return _validate_tp_mapping(parsed, label=f"'{endpoint}OffsetsByTimestamp'")
    return None


def _extract_kafka_config(opts: dict[str, str]) -> dict[str, str]:
    """Pass through any option prefixed with ``kafka.`` to librdkafka config.

    Matches Spark's convention: ``kafka.bootstrap.servers`` →
    ``bootstrap.servers``, ``kafka.security.protocol`` → ``security.protocol``,
    etc.
    """
    return {k[len("kafka.") :]: v for k, v in opts.items() if k.startswith("kafka.")}


# Static group id prefix. All reads use ``consumer.assign()`` (not
# ``subscribe()``), so the consumer never actually joins a group — the id is
# just required by librdkafka. A stable prefix keeps broker logs clean and
# avoids polluting ``__consumer_offsets`` with a fresh id per query. Users
# can still override by passing ``kafka.group.id``.
_DEFAULT_GROUP_ID = "sail-kafka"


def _build_consumer_config(client_config: dict[str, str], *, role: str) -> dict[str, str]:
    """Return the effective librdkafka config for a batch-read consumer.

    User-supplied ``kafka.*`` options win. We fill in defaults for:

    * ``group.id`` — required by librdkafka; unused because we
      ``assign()`` explicitly rather than ``subscribe()``.
    * ``enable.auto.commit`` — off; reads never commit.
    * ``auto.offset.reset`` — ``error``; the planner resolved the range,
      so a missing start offset means the log was truncated and we should
      fail loudly rather than silently drift.
    * ``enable.partition.eof`` — on; lets the read loop exit cleanly when
      the log ends before the planned high watermark. This happens on
      compacted topics (the watermark is past compacted-away tombstones)
      and with ``read_committed`` isolation (aborted transaction markers
      leave visible offsets unreachable). Without EOF events those cases
      would stall until ``maxEmptyPolls``.
    """
    return {
        "group.id": f"{_DEFAULT_GROUP_ID}-{role}",
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
        "enable.partition.eof": True,
        **client_config,  # user overrides win
    }


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
        max_empty_polls: int,
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
        self.max_empty_polls = max_empty_polls


# ============================================================================
# DataSourceReader
# ============================================================================


class KafkaDataSourceReader(DataSourceReader):
    def __init__(
        self,
        *,
        topics: list[str],
        starting_offsets: str | dict[str, dict[str, int]] | None,
        ending_offsets: str | dict[str, dict[str, int]] | None,
        starting_timestamps: int | dict[str, dict[str, int]] | None,
        ending_timestamps: int | dict[str, dict[str, int]] | None,
        client_config: dict[str, str],
        include_headers: bool,
        poll_timeout_s: float,
        max_batch_rows: int,
        max_empty_polls: int,
        admin_timeout_s: float,
    ) -> None:
        self.topics = topics
        self.starting_offsets = starting_offsets
        self.ending_offsets = ending_offsets
        self.starting_timestamps = starting_timestamps
        self.ending_timestamps = ending_timestamps
        self.client_config = client_config
        self.include_headers = include_headers
        self.poll_timeout_s = poll_timeout_s
        self.max_batch_rows = max_batch_rows
        self.max_empty_polls = max_empty_polls
        self.admin_timeout_s = admin_timeout_s

    # ------------------------------------------------------------------
    # Partition planning — talks to the broker once on the driver
    # ------------------------------------------------------------------

    def partitions(self) -> list[InputPartition]:
        # A short-lived admin consumer is used to enumerate partitions and
        # resolve earliest/latest watermarks. Executors get their own consumer.
        consumer = Consumer(_build_consumer_config(self.client_config, role="planner"))
        admin_timeout_s = self.admin_timeout_s
        try:
            tps = _list_topic_partitions(consumer, self.topics, timeout_s=admin_timeout_s)
            if self.starting_timestamps is not None:
                starts = _resolve_by_timestamp(
                    consumer,
                    tps,
                    self.starting_timestamps,
                    endpoint="starting",
                    timeout_s=admin_timeout_s,
                )
            else:
                starts = _resolve_offsets(
                    consumer,
                    tps,
                    self.starting_offsets or _SENTINEL_EARLIEST,
                    endpoint="starting",
                    timeout_s=admin_timeout_s,
                )
            if self.ending_timestamps is not None:
                ends = _resolve_by_timestamp(
                    consumer,
                    tps,
                    self.ending_timestamps,
                    endpoint="ending",
                    timeout_s=admin_timeout_s,
                )
            else:
                ends = _resolve_offsets(
                    consumer,
                    tps,
                    self.ending_offsets or _SENTINEL_LATEST,
                    endpoint="ending",
                    timeout_s=admin_timeout_s,
                )
        finally:
            consumer.close()

        parts: list[InputPartition] = []
        pid = 0
        for tp in tps:
            key = (tp.topic, tp.partition)
            start = starts[key]
            end = ends[key]
            if end <= start:
                continue  # empty range; skip
            parts.append(
                KafkaInputPartition(
                    pid,
                    topic=tp.topic,
                    kafka_partition=tp.partition,
                    start_offset=start,
                    end_offset=end,
                    client_config=self.client_config,
                    include_headers=self.include_headers,
                    poll_timeout_s=self.poll_timeout_s,
                    max_batch_rows=self.max_batch_rows,
                    max_empty_polls=self.max_empty_polls,
                )
            )
            pid += 1
        return parts

    # ------------------------------------------------------------------
    # Per-partition read — runs on executors
    # ------------------------------------------------------------------

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        if not isinstance(partition, KafkaInputPartition):
            msg = f"Expected KafkaInputPartition, got {type(partition)}"
            raise TypeError(msg)

        schema = _build_schema(include_headers=partition.include_headers)

        consumer = Consumer(_build_consumer_config(partition.client_config, role="reader"))
        try:
            consumer.assign([TopicPartition(partition.topic, partition.kafka_partition, partition.start_offset)])

            buf = _RowBuffer(include_headers=partition.include_headers)
            next_offset = partition.start_offset
            end = partition.end_offset
            empty_polls = 0
            max_empty_polls = partition.max_empty_polls

            while next_offset < end:
                msg = consumer.poll(partition.poll_timeout_s)
                if msg is None:
                    # No message within poll timeout. Normally transient — the
                    # broker is slow, or a fetch is in flight. But if this
                    # keeps happening we're wedged (unreachable broker, dead
                    # partition leader) and would hang forever without a cap.
                    empty_polls += 1
                    if empty_polls > max_empty_polls:
                        stalled_ms = int(max_empty_polls * partition.poll_timeout_s * 1000)
                        msg_err = (
                            f"Kafka read stalled: no messages for {stalled_ms}ms on "
                            f"{partition.topic}[{partition.kafka_partition}] at offset "
                            f"{next_offset}, expected up to {end}. Broker may be "
                            f"unreachable or the partition leader unavailable."
                        )
                        raise TimeoutError(msg_err)
                    continue
                if msg.error():
                    # EOF is not an error — the log ended before the planned
                    # high watermark. Compacted topics and aborted transaction
                    # markers can leave a gap between the last visible message
                    # and the watermark; treat it as a clean stop.
                    if msg.error().code() == KafkaError._PARTITION_EOF:  # noqa: SLF001
                        break
                    raise KafkaException(msg.error())

                if msg.offset() >= end:
                    break

                empty_polls = 0
                buf.append(msg)
                next_offset = msg.offset() + 1

                if len(buf) >= partition.max_batch_rows:
                    yield buf.to_batch(schema)
                    buf = _RowBuffer(include_headers=partition.include_headers)

            if len(buf) > 0:
                yield buf.to_batch(schema)
        finally:
            consumer.close()


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
        self.timestamp_us: list[int | None] = []
        self.timestamp_type: list[int] = []
        self.headers: list[list[dict] | None] = []

    def __len__(self) -> int:
        return len(self.offset)

    def append(self, msg) -> None:
        ts_type, ts_ms = msg.timestamp()  # librdkafka: 0=NotAvailable, 1=Create, 2=LogAppend
        self.key.append(msg.key())
        self.value.append(msg.value())
        self.topic.append(msg.topic())
        self.partition.append(msg.partition())
        self.offset.append(msg.offset())
        self.timestamp_us.append(ts_ms * 1000 if ts_type != 0 else None)
        self.timestamp_type.append(_LIBRDKAFKA_TO_SPARK_TS_TYPE.get(ts_type, -1))
        if self.include_headers:
            hdrs = msg.headers()
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


def _list_topic_partitions(consumer: Consumer, topics: list[str], *, timeout_s: float) -> list[TopicPartition]:
    metadata = consumer.list_topics(timeout=timeout_s)
    out: list[TopicPartition] = []
    for topic in topics:
        tmeta = metadata.topics.get(topic)
        if tmeta is None or tmeta.error is not None:
            msg = f"Topic {topic!r} not found on broker"
            raise ValueError(msg)
        out.extend(TopicPartition(topic, p) for p in sorted(tmeta.partitions.keys()))
    return out


def _watermark(consumer: Consumer, tp: TopicPartition, *, endpoint: str, timeout_s: float) -> int:
    """Read the low (starting) or high (ending) watermark for a partition."""
    low, high = consumer.get_watermark_offsets(tp, timeout=timeout_s, cached=False)
    return low if endpoint == "starting" else high


def _resolve_offsets(
    consumer: Consumer,
    tps: list[TopicPartition],
    spec: str | dict[str, dict[str, int]],
    *,
    endpoint: str,
    timeout_s: float,
) -> dict[tuple[str, int], int]:
    """Resolve a starting/ending-offset spec into concrete offsets per (topic, partition)."""
    if spec in (_SENTINEL_EARLIEST, _SENTINEL_LATEST):
        sentinel_endpoint = "starting" if spec == _SENTINEL_EARLIEST else "ending"
        return {
            (tp.topic, tp.partition): _watermark(consumer, tp, endpoint=sentinel_endpoint, timeout_s=timeout_s)
            for tp in tps
        }

    # Explicit per-topic-partition JSON. Missing entries fall back to the
    # sensible default per endpoint (earliest for starting, latest for ending)
    # to match Spark's behavior on newly discovered partitions.
    out: dict[tuple[str, int], int] = {}
    for tp in tps:
        raw = spec.get(tp.topic, {}).get(str(tp.partition))
        if raw is None:
            out[(tp.topic, tp.partition)] = _watermark(consumer, tp, endpoint=endpoint, timeout_s=timeout_s)
        elif raw == _SPARK_OFFSET_LATEST:
            out[(tp.topic, tp.partition)] = _watermark(consumer, tp, endpoint="ending", timeout_s=timeout_s)
        elif raw == _SPARK_OFFSET_EARLIEST:
            out[(tp.topic, tp.partition)] = _watermark(consumer, tp, endpoint="starting", timeout_s=timeout_s)
        else:
            out[(tp.topic, tp.partition)] = int(raw)
    return out


def _resolve_by_timestamp(
    consumer: Consumer,
    tps: list[TopicPartition],
    spec: int | dict[str, dict[str, int]],
    *,
    endpoint: str,
    timeout_s: float,
) -> dict[tuple[str, int], int]:
    """Resolve a timestamp spec to concrete offsets via ``offsets_for_times``.

    ``spec`` is either a global ``int`` (ms since epoch, applied to every
    partition) or a mapping ``{topic: {partition_str: ts_ms}}``. Partitions
    with no message satisfying ``ts >= T`` resolve to the high watermark,
    which bounds the read to whatever's currently in the log.

    Partitions that exist on the broker but are absent from a per-partition
    ``spec`` fall back to the same defaults as ``_resolve_offsets``: earliest
    for a starting endpoint, latest for an ending endpoint. This matches
    Spark's behaviour on newly discovered partitions and avoids silently
    skipping data if the user omits a partition from the JSON.

    Note: ``offsets_for_times`` searches the topic's actual stored
    timestamps. If the topic is configured with
    ``message.timestamp.type=LogAppendTime`` the search is against broker
    ingest time; otherwise it's against producer clocks (``CreateTime``).
    """
    # Build the lookup list without any sentinel values — a user-supplied
    # timestamp of -1 is a valid (if odd) instant, so we track which
    # partitions have an explicit spec by set membership instead.
    to_lookup: list[TopicPartition] = []
    if isinstance(spec, int):
        to_lookup = [TopicPartition(tp.topic, tp.partition, spec) for tp in tps]
        specified: set[tuple[str, int]] = {(tp.topic, tp.partition) for tp in tps}
    else:
        specified = set()
        for tp in tps:
            raw = spec.get(tp.topic, {}).get(str(tp.partition))
            if raw is None:
                continue
            specified.add((tp.topic, tp.partition))
            to_lookup.append(TopicPartition(tp.topic, tp.partition, int(raw)))

    resolved_list = consumer.offsets_for_times(to_lookup, timeout=timeout_s) if to_lookup else []
    resolved_map = {(r.topic, r.partition): r.offset for r in resolved_list}

    out: dict[tuple[str, int], int] = {}
    for tp in tps:
        key = (tp.topic, tp.partition)
        if key not in specified:
            out[key] = _watermark(consumer, tp, endpoint=endpoint, timeout_s=timeout_s)
            continue
        offset = resolved_map.get(key, -1)
        if offset < 0:
            # No message with ts >= T in the log. Bound to the current high
            # watermark so the read covers everything up to "now" without
            # failing.
            out[key] = _watermark(consumer, tp, endpoint="ending", timeout_s=timeout_s)
        else:
            out[key] = offset
    return out


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

    Supported options (subset of Spark's Kafka source):

    +--------------------------------+----------+----------+------------------------------------------+
    | Option                         | Required | Default  | Description                              |
    +================================+==========+==========+==========================================+
    | kafka.bootstrap.servers        | Yes      |          | Broker list                              |
    +--------------------------------+----------+----------+------------------------------------------+
    | subscribe                      | Yes      |          | Comma-separated topic names              |
    +--------------------------------+----------+----------+------------------------------------------+
    | startingOffsets                | No       | earliest | "earliest" / "latest" / per-tp JSON      |
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
    | includeHeaders                 | No       | false    | Include the ``headers`` column           |
    +--------------------------------+----------+----------+------------------------------------------+
    | maxBatchRows                   | No       | 10000    | Max rows per Arrow RecordBatch           |
    +--------------------------------+----------+----------+------------------------------------------+
    | pollTimeoutMs                  | No       | 1000     | ``consumer.poll()`` timeout              |
    +--------------------------------+----------+----------+------------------------------------------+
    | maxEmptyPolls                  | No       | 30       | Consecutive empty polls before failing   |
    +--------------------------------+----------+----------+------------------------------------------+
    | adminTimeoutMs                 | No       | 10000    | Broker metadata / offset-lookup timeout  |
    +--------------------------------+----------+----------+------------------------------------------+
    | kafka.*                        | No       |          | Any extra librdkafka client config       |
    +--------------------------------+----------+----------+------------------------------------------+

    Only one starting spec may be set at a time (``startingOffsets`` vs
    ``startingOffsetsByTimestamp`` vs ``startingTimestamp``); same for
    ending. Timestamp-based reads rely on Kafka's ``offsets_for_times``,
    which searches the topic's stored timestamps — configure the topic
    with ``message.timestamp.type=LogAppendTime`` if you want the search
    against broker ingest time rather than producer clocks.

    Not supported: ``subscribePattern`` / ``assign``, ``failOnDataLoss``,
    writes.
    """

    @classmethod
    def name(cls) -> str:
        return "kafka"

    @cached_property
    def _resolved(self) -> dict:
        opts = dict(self.options)

        client_config = _extract_kafka_config(opts)
        if "bootstrap.servers" not in client_config:
            msg = "Option 'kafka.bootstrap.servers' is required for the kafka data source"
            raise ValueError(msg)

        subscribe = opts.get("subscribe")
        if not subscribe:
            msg = "Option 'subscribe' is required (comma-separated topics)"
            raise ValueError(msg)
        topics = [t.strip() for t in subscribe.split(",") if t.strip()]
        if not topics:
            msg = "Option 'subscribe' must list at least one topic"
            raise ValueError(msg)

        starting_offsets_raw = opts.get("startingOffsets")
        ending_offsets_raw = opts.get("endingOffsets")

        starting_ts = _parse_timestamp_spec(
            opts.get("startingTimestamp"),
            opts.get("startingOffsetsByTimestamp"),
            endpoint="starting",
        )
        ending_ts = _parse_timestamp_spec(
            opts.get("endingTimestamp"),
            opts.get("endingOffsetsByTimestamp"),
            endpoint="ending",
        )

        if starting_ts is not None and starting_offsets_raw is not None:
            msg = "'startingOffsets' cannot be combined with a starting-timestamp option"
            raise ValueError(msg)
        if ending_ts is not None and ending_offsets_raw is not None:
            msg = "'endingOffsets' cannot be combined with an ending-timestamp option"
            raise ValueError(msg)

        starting_offsets = _parse_offset_spec(starting_offsets_raw, default=None)
        ending_offsets = _parse_offset_spec(ending_offsets_raw, default=None)

        include_headers = opts.get("includeHeaders", "false").lower() == "true"
        max_batch_rows = int(opts.get("maxBatchRows", "10000"))
        poll_timeout_s = float(opts.get("pollTimeoutMs", "1000")) / 1000.0
        max_empty_polls = int(opts.get("maxEmptyPolls", "30"))
        admin_timeout_s = float(opts.get("adminTimeoutMs", "10000")) / 1000.0

        if max_batch_rows <= 0:
            msg = f"'maxBatchRows' must be positive, got {max_batch_rows}"
            raise ValueError(msg)
        if max_empty_polls <= 0:
            msg = f"'maxEmptyPolls' must be positive, got {max_empty_polls}"
            raise ValueError(msg)
        if admin_timeout_s <= 0:
            msg = f"'adminTimeoutMs' must be positive, got {admin_timeout_s * 1000}"
            raise ValueError(msg)

        return {
            "topics": topics,
            "starting_offsets": starting_offsets,
            "ending_offsets": ending_offsets,
            "starting_timestamps": starting_ts,
            "ending_timestamps": ending_ts,
            "client_config": client_config,
            "include_headers": include_headers,
            "poll_timeout_s": poll_timeout_s,
            "max_batch_rows": max_batch_rows,
            "max_empty_polls": max_empty_polls,
            "admin_timeout_s": admin_timeout_s,
        }

    def schema(self) -> pa.Schema:
        return _build_schema(include_headers=self._resolved["include_headers"])

    def reader(self, schema: pa.Schema) -> KafkaDataSourceReader:  # noqa: ARG002
        return KafkaDataSourceReader(**self._resolved)
