# SPDX-License-Identifier: Apache-2.0

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
from typing import TYPE_CHECKING

try:
    from confluent_kafka import IsolationLevel  # noqa: F401
except ImportError as e:
    msg = "confluent-kafka >= 2.5 is required for the kafka data source. Install it with: pip install pysail[kafka]"
    raise ImportError(msg) from e

try:
    from pyspark.sql.datasource import DataSource
except ImportError as e:
    msg = "PySpark with the DataSource API is required (PySpark >= 4.0)."
    raise ImportError(msg) from e

from pysail.spark.datasource.kafka.options import parse_options
from pysail.spark.datasource.kafka.reader import KafkaDataSourceReader
from pysail.spark.datasource.kafka.schema import build_schema

if TYPE_CHECKING:
    import pyarrow as pa

    from pysail.spark.datasource.kafka.options import KafkaReadOptions

__all__ = ["KafkaDataSource"]


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
    def read_options(self) -> KafkaReadOptions:
        return parse_options(self.options)

    def schema(self) -> pa.Schema:
        return build_schema(include_headers=self.read_options.include_headers)

    def reader(self, schema: pa.Schema | None) -> KafkaDataSourceReader:
        # Kafka's schema is fixed and every field is nullable; reject any user-supplied
        # schema that changes names, types, or nullability (ignoring metadata).
        expected = build_schema(include_headers=self.read_options.include_headers)
        if schema is not None and not schema.equals(expected, check_metadata=False):
            msg = (
                f"The kafka data source has a fixed schema and does not accept a user-specified one.\n"
                f"Expected: {expected}\nGot: {schema}"
            )
            raise ValueError(msg)
        return KafkaDataSourceReader(options=self.read_options)
