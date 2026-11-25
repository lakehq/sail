# SPDX-License-Identifier: Apache-2.0

"""Spark data source reader: plan input partitions and stream bounded Arrow batches."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql.datasource import DataSourceReader, InputPartition

from pysail.spark.datasource.kafka.client import create_admin_client, open_consumer
from pysail.spark.datasource.kafka.cursor import PartitionCursor, bind_offset_range
from pysail.spark.datasource.kafka.planning import plan_offset_ranges
from pysail.spark.datasource.kafka.schema import RowBuffer, build_schema, record_size

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

    from pysail.spark.datasource.kafka.models import OffsetRange
    from pysail.spark.datasource.kafka.options import KafkaReadOptions

LOGGER = logging.getLogger(__name__)


class KafkaInputPartition(InputPartition):
    """One read task: the planned offset range of one Kafka partition.

    ``earliest``/``latest`` endpoints are still ``-2``/``-1`` sentinels here; the task binds them.
    """

    def __init__(self, partition_id: int, offset_range: OffsetRange) -> None:
        super().__init__(partition_id)
        self.offset_range = offset_range


class KafkaDataSourceReader(DataSourceReader):
    """Plans offset ranges on the driver and reads one range per task on the executors."""

    def __init__(self, *, options: KafkaReadOptions) -> None:
        self.options = options

    def partitions(self) -> list[InputPartition]:
        """Plan the read on the driver; see ``planning.plan_offset_ranges``."""
        admin = create_admin_client(self.options.client_config)
        return [
            KafkaInputPartition(partition_id, offset_range)
            for partition_id, offset_range in enumerate(plan_offset_ranges(admin, self.options))
        ]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        """Read one planned range on an executor, yielding size- and row-bounded Arrow batches."""
        if not isinstance(partition, KafkaInputPartition):
            msg = f"Expected KafkaInputPartition, got {type(partition)}"
            raise TypeError(msg)

        planned = partition.offset_range
        with open_consumer(
            self.options.client_config, self.options.isolation_level, planned.topic_partition
        ) as consumer:
            offset_range = bind_offset_range(consumer, planned, timeout_s=self.options.admin_timeout_s)
            if offset_range.size == 0:
                return
            cursor = PartitionCursor(consumer, offset_range, self.options)
            yield from self.read_batches(cursor)
            if cursor.partial_read:
                LOGGER.warning(
                    "Kafka partial read on %s: skipped unavailable offsets; failOnDataLoss=false",
                    offset_range.topic_partition,
                )

    def read_batches(self, cursor: PartitionCursor) -> Iterator[pa.RecordBatch]:
        """Buffer the cursor's records into batches bounded by ``maxBatchRows`` and ``maxBatchBytes``."""
        schema = build_schema(include_headers=self.options.include_headers)
        max_rows, max_bytes = self.options.max_batch_rows, self.options.max_batch_bytes
        row_buffer = RowBuffer(include_headers=self.options.include_headers)
        for record in cursor:
            record_bytes = record_size(record, include_headers=self.options.include_headers)
            if row_buffer and row_buffer.size_bytes + record_bytes > max_bytes:
                yield row_buffer.to_batch(schema)
            row_buffer.append(record, record_bytes)
            if len(row_buffer) >= max_rows or row_buffer.size_bytes >= max_bytes:
                yield row_buffer.to_batch(schema)
        if row_buffer:
            yield row_buffer.to_batch(schema)
