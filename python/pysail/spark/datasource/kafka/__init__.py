"""Kafka batch data source for Sail, backed by confluent-kafka (librdkafka).

Exposes ``spark.read.format("kafka")`` with a Spark-compatible schema and
option surface. Every read resolves to a bounded ``[start, end)`` range per
partition, with ``earliest``/``latest`` bound per task rather than on the
driver, as Spark's batch Kafka source does.

Install the optional dependency before use::

    pip install pysail[kafka]
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, TypeVar

try:
    from confluent_kafka import IsolationLevel  # noqa: F401
except ImportError as e:
    msg = "confluent-kafka >= 2.5 is required for the kafka data source. Install it with: pip install pysail[kafka]"
    raise ImportError(msg) from e

try:
    from pyspark.sql.datasource import CaseInsensitiveDict, DataSource
except ImportError as e:
    msg = "PySpark with the DataSource API is required (PySpark >= 4.0)."
    raise ImportError(msg) from e

from pysail.spark.datasource.kafka.options import (
    INT32_MAX,
    INT64_MAX,
    TimestampStrategy,
    parse_bool,
    parse_client_config,
    parse_positive_int,
    parse_positive_ms,
    parse_range_limit,
    parse_subscription,
    parse_timestamp_strategy,
)
from pysail.spark.datasource.kafka.reader import KafkaDataSourceReader, KafkaInputPartition, build_schema

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    import pyarrow as pa

__all__ = ["KafkaDataSource", "KafkaDataSourceReader", "KafkaInputPartition"]


_Parsed = TypeVar("_Parsed")
_Default = TypeVar("_Default")


def parse_option(
    opts: Mapping[str, str], key: str, parse: Callable[..., _Parsed], default: _Default, **kwargs: int
) -> _Parsed | _Default:
    return parse(opts[key], key, **kwargs) if key in opts else default


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
    def _resolved(self) -> KafkaDataSourceReader:
        opts = CaseInsensitiveDict(self.options)

        client_config, isolation_level = parse_client_config(opts)
        subscription = parse_subscription(opts)
        starting = parse_range_limit(opts, boundary="starting")
        ending = parse_range_limit(opts, boundary="ending")
        starting_ts_strategy = parse_timestamp_strategy(
            opts.get("startingOffsetsByTimestampStrategy", TimestampStrategy.ERROR.value)
        )

        include_headers = parse_option(opts, "includeHeaders", parse_bool, False)
        fail_on_data_loss = parse_option(opts, "failOnDataLoss", parse_bool, True)
        max_batch_rows = parse_option(opts, "maxBatchRows", parse_positive_int, 10000)
        max_batch_bytes = parse_option(opts, "maxBatchBytes", parse_positive_int, 8388608, max_value=INT32_MAX)
        min_partitions = parse_option(opts, "minPartitions", parse_positive_int, None, max_value=INT32_MAX)
        max_records_per_partition = parse_option(
            opts, "maxRecordsPerPartition", parse_positive_int, None, max_value=INT64_MAX
        )
        # Default matches Spark's fallback to spark.network.timeout (120s).
        poll_key = "kafkaConsumer.pollTimeoutMs" if "kafkaConsumer.pollTimeoutMs" in opts else "pollTimeoutMs"
        poll_timeout_s = parse_option(opts, poll_key, parse_positive_ms, 120.0)
        # librdkafka admin request timeouts accept 1 ms to 1 hour.
        admin_timeout_s = parse_option(opts, "adminTimeoutMs", parse_positive_ms, 10.0, max_value=3600000)

        return KafkaDataSourceReader(
            subscription=subscription,
            starting=starting,
            ending=ending,
            starting_ts_strategy=starting_ts_strategy,
            client_config=client_config,
            isolation_level=isolation_level,
            include_headers=include_headers,
            poll_timeout_s=poll_timeout_s,
            max_batch_rows=max_batch_rows,
            max_batch_bytes=max_batch_bytes,
            admin_timeout_s=admin_timeout_s,
            fail_on_data_loss=fail_on_data_loss,
            min_partitions=min_partitions,
            max_records_per_partition=max_records_per_partition,
        )

    def schema(self) -> pa.Schema:
        return build_schema(include_headers=self._resolved.include_headers)

    def reader(self, schema: pa.Schema | None) -> KafkaDataSourceReader:
        # Kafka's schema is fixed and every field is nullable; reject any user-supplied
        # schema that changes names, types, or nullability (ignoring metadata).
        expected = build_schema(include_headers=self._resolved.include_headers)
        if schema is not None and not schema.equals(expected, check_metadata=False):
            msg = (
                f"The kafka data source has a fixed schema and does not accept a user-specified one.\n"
                f"Expected: {expected}\nGot: {schema}"
            )
            raise ValueError(msg)
        return self._resolved
