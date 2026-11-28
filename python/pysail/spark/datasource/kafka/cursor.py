# SPDX-License-Identifier: Apache-2.0

"""Per-task Kafka cursor: range binding, polling, and data-loss recovery."""

from __future__ import annotations

from enum import Enum, auto
from typing import TYPE_CHECKING

from confluent_kafka import KafkaError, KafkaException, TopicPartition

from pysail.spark.datasource.kafka.client import OUT_OF_RANGE_CODES, query_watermarks
from pysail.spark.datasource.kafka.models import OffsetRange, SpecialOffset

if TYPE_CHECKING:
    from collections.abc import Iterator

    from confluent_kafka import Consumer, Message

    from pysail.spark.datasource.kafka.models import TopicPartitionKey, Watermarks
    from pysail.spark.datasource.kafka.options import KafkaReadOptions

# Bound repeated out-of-range recovery attempts that cannot advance the cursor.
MAX_STALLED_RECOVERIES = 3


class ReadAction(Enum):
    """Whether a broker event completes the task or allows another poll."""

    CONTINUE = auto()
    STOP = auto()


def bind_offset_range(consumer: Consumer, planned: OffsetRange, *, timeout_s: float) -> OffsetRange:
    """Late-bind ``earliest``/``latest`` sentinels at task start, mirroring ``KafkaSourceRDD.resolveRange``."""
    if planned.is_bound:
        return planned
    watermarks = query_watermarks(consumer, planned.topic_partition, timeout_s=timeout_s)
    start = watermarks.low if planned.start == SpecialOffset.EARLIEST else planned.start
    end = watermarks.high if planned.end == SpecialOffset.LATEST else planned.end
    return OffsetRange(planned.topic_partition, start, end)


class EofOutcome(Enum):
    """How a ``_PARTITION_EOF`` event ends (or does not end) a read."""

    COMPLETE = "complete"  # the planned range was read
    PARTIAL = "partial"  # the log ended early and failOnDataLoss=false
    STALE = "stale"  # the log has grown past this EOF; keep polling


class PartitionCursor:
    """The read position of one task in one Kafka partition.

    Iteration polls the consumer and applies Spark's ``failOnDataLoss``
    rules to every gap, EOF, and out-of-range event on the way.
    """

    def __init__(self, consumer: Consumer, offset_range: OffsetRange, options: KafkaReadOptions) -> None:
        """Assign the consumer to the range and initialize its read position."""
        self.consumer = consumer
        self.offset_range = offset_range
        self.options = options
        self.assignment = TopicPartition(self.topic_partition.topic, self.topic_partition.partition, offset_range.start)
        self.next_offset = offset_range.start
        self.stalled_recoveries = 0
        self.partial_read = False
        consumer.assign([self.assignment])

    @property
    def topic_partition(self) -> TopicPartitionKey:
        return self.offset_range.topic_partition

    @property
    def end(self) -> int:
        return self.offset_range.end

    def __iter__(self) -> Iterator[Message]:
        """Yield visible records until the planned range is complete."""
        while self.next_offset < self.end:
            record = self.consumer.poll(self.options.poll_timeout_s)
            if record is None:
                self.handle_empty_poll()
                continue
            if record.error() is not None:
                if self.handle_error_event(record) is ReadAction.STOP:
                    self.next_offset = self.end
                    return
                continue

            self.stalled_recoveries = 0
            record_offset = record.offset()
            if record_offset < self.next_offset:
                msg = (
                    f"Kafka returned offset {record_offset} for {self.topic_partition} "
                    f"while offset {self.next_offset} was requested"
                )
                raise ValueError(msg)
            if record_offset > self.next_offset:
                self.check_gap(record_offset)
            if record_offset >= self.end:
                self.next_offset = self.end
                return
            self.next_offset = record_offset + 1
            yield record
            # Do not retain the previous message while waiting for the next one.
            del record

    def handle_empty_poll(self) -> None:
        """Advance past invisible (control or aborted) records, or time out if the position did not move."""
        position = self.consumer.position([self.assignment])[0].offset
        if position <= self.next_offset:
            msg = (
                f"Cannot fetch record for offset {self.next_offset} on {self.topic_partition} in "
                f"{self.options.poll_timeout_s * 1000:g} milliseconds"
            )
            raise TimeoutError(msg)
        # Kafka may advance past invisible/control records without returning a visible message.
        self.check_gap(position)
        self.next_offset = position
        self.stalled_recoveries = 0

    def handle_error_event(self, record: Message) -> ReadAction:
        """Translate an EOF or out-of-range event into the next read action."""
        error = record.error()
        code = error.code()
        if code == KafkaError._PARTITION_EOF:  # noqa: SLF001
            outcome = self.classify_eof(record.offset())
            if outcome is EofOutcome.STALE:
                # The log has grown past this EOF; it can still advance past control records.
                if record.offset() > self.next_offset:
                    self.next_offset = record.offset()
                    self.stalled_recoveries = 0
                return ReadAction.CONTINUE
            self.partial_read |= outcome is EofOutcome.PARTIAL
            return ReadAction.STOP
        if code in OUT_OF_RANGE_CODES:
            return self.recover_out_of_range()
        raise KafkaException(error)

    def recover_out_of_range(self) -> ReadAction:
        """Fail under ``failOnDataLoss``, otherwise resume at the available overlap."""
        if self.options.fail_on_data_loss:
            msg = (
                f"Data loss on {self.topic_partition}: offset {self.next_offset} is no "
                f"longer available on the broker. Records were removed after planning (retention or "
                f"truncation), or the offset is past the end of the log. "
                f"Set 'failOnDataLoss' to false to skip missing records."
            )
            raise ValueError(msg)
        watermarks = self.query_watermarks()
        if watermarks.low >= watermarks.high or self.next_offset >= watermarks.high or watermarks.low >= self.end:
            self.partial_read = True
            return ReadAction.STOP
        recovered = max(self.next_offset, watermarks.low)
        # Re-assigning the single partition restarts fetching after _AUTO_OFFSET_RESET
        # puts librdkafka in FETCH_NONE (where consumer.seek() fails with _STATE).
        self.consumer.assign([TopicPartition(self.topic_partition.topic, self.topic_partition.partition, recovered)])
        if recovered > self.next_offset:
            self.next_offset = recovered
            self.stalled_recoveries = 0
            self.partial_read = True
            return ReadAction.CONTINUE
        self.stalled_recoveries += 1
        if self.stalled_recoveries >= MAX_STALLED_RECOVERIES:
            msg = f"Kafka out-of-range recovery made no progress on {self.topic_partition} at offset {self.next_offset}"
            raise TimeoutError(msg)
        return ReadAction.CONTINUE

    def classify_eof(self, eof_offset: int | None) -> EofOutcome:
        """Classify a ``_PARTITION_EOF`` event as complete, partial (permissive), stale, or data loss."""
        if eof_offset is not None and eof_offset < 0:
            eof_offset = None
        position = self.next_offset if eof_offset is None else eof_offset
        if position >= self.end:
            if position > self.next_offset:
                self.check_gap(position)
            return EofOutcome.COMPLETE
        watermarks = self.query_watermarks()
        if eof_offset is not None and watermarks.high > eof_offset:
            if eof_offset > self.next_offset:
                self.raise_if_gap_lost(watermarks, record_offset=eof_offset)
            return EofOutcome.STALE
        if watermarks.high >= self.end:
            self.raise_if_gap_lost(watermarks, record_offset=self.end)
            return EofOutcome.COMPLETE
        if not self.options.fail_on_data_loss:
            return EofOutcome.PARTIAL
        msg = (
            f"Data loss on {self.topic_partition}: reached end of log at offset "
            f"{self.next_offset}, but the read was planned up to {self.end} and the high watermark "
            f"is now {watermarks.high}. Records were removed after planning (retention or truncation), or "
            f"'endingOffsets' points past the end of the log. "
            f"Set 'failOnDataLoss' to false to return the records that remain."
        )
        raise ValueError(msg)

    def check_gap(self, record_offset: int) -> None:
        """Under ``failOnDataLoss``, tell unreadable in-log gaps (compaction, control markers) from retention loss."""
        if not self.options.fail_on_data_loss:
            return
        self.raise_if_gap_lost(self.query_watermarks(cached=True), record_offset=record_offset)

    def raise_if_gap_lost(self, watermarks: Watermarks, *, record_offset: int) -> None:
        """Under ``failOnDataLoss``, raise data loss when ``watermarks.low`` has advanced past ``next_offset``."""
        if not self.options.fail_on_data_loss or watermarks.low <= self.next_offset:
            return
        msg = (
            f"Data loss on {self.topic_partition}: offsets {self.next_offset} to "
            f"{min(record_offset, self.end) - 1} were requested but are no longer in the log, which now starts at "
            f"{watermarks.low}. Records were removed after planning (retention or truncation). "
            f"Set 'failOnDataLoss' to false to skip missing records."
        )
        raise ValueError(msg)

    def query_watermarks(self, *, cached: bool = False) -> Watermarks:
        return query_watermarks(
            self.consumer, self.topic_partition, timeout_s=self.options.admin_timeout_s, cached=cached
        )
