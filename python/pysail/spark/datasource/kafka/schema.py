# SPDX-License-Identifier: Apache-2.0

"""The fixed Spark-compatible Kafka schema and the Arrow row buffer that fills it."""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING, TypedDict

import pyarrow as pa
from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME, TIMESTAMP_NOT_AVAILABLE

if TYPE_CHECKING:
    from confluent_kafka import Message


# ----------------------------------------------------------------------------
# Fixed Spark-compatible schema
# ----------------------------------------------------------------------------

# Matches the column names, order, types, and nullability of
# ``org.apache.spark.sql.kafka010.KafkaRecordToRowConverter``'s output schema.
# ``headers`` is only appended when the ``includeHeaders`` option is true.
#
# Spark uses default-nullable StructFields, including populated metadata columns.

BASE_FIELDS: tuple[pa.Field, ...] = (
    pa.field("key", pa.binary(), nullable=True),
    pa.field("value", pa.binary(), nullable=True),
    pa.field("topic", pa.string(), nullable=True),
    pa.field("partition", pa.int32(), nullable=True),
    pa.field("offset", pa.int64(), nullable=True),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
    pa.field("timestampType", pa.int32(), nullable=True),
)

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


def build_schema(*, include_headers: bool) -> pa.Schema:
    fields = list(BASE_FIELDS)
    if include_headers:
        fields.append(pa.field("headers", pa.list_(HEADER_STRUCT), nullable=True))
    return pa.schema(fields)


# ----------------------------------------------------------------------------
# Row buffer: accumulates messages, flushes to a RecordBatch
# ----------------------------------------------------------------------------


class KafkaHeader(TypedDict):
    """An Arrow header struct, including nullable Kafka header values."""

    key: str
    value: bytes | None


def record_size(record: Message, *, include_headers: bool) -> int:
    """Conservative Arrow payload estimate, not a process-memory measurement."""
    size = 64 + len(record.key() or b"") + len(record.value() or b"") + len(record.topic().encode("utf-8"))
    if include_headers:
        size += sum(16 + len(key.encode("utf-8")) + len(value or b"") for key, value in record.headers() or [])
    return size


class RowBuffer:
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
        """Build a batch typed by ``schema`` (from ``build_schema``) and empty the buffer."""
        columns: list[list] = [
            self.key,
            self.value,
            self.topic,
            self.partition,
            self.offset,
            self.timestamp_us,
            self.timestamp_type,
        ]
        if self.include_headers:
            columns.append(self.headers)
        arrays = [pa.array(column, type=field.type) for column, field in zip(columns, schema, strict=True)]
        batch = pa.record_batch(arrays, schema=schema)
        # Release Python payloads before yielding across backpressure boundaries.
        self.clear()
        return batch
