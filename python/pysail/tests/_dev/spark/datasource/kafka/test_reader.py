# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Kafka read loop (data loss, batching, consumer lifecycle) against a fake consumer."""

from __future__ import annotations

import os
import subprocess
import sys
from textwrap import dedent

import pytest

from pysail.tests._dev.spark.datasource.kafka.fakes import (
    TOPIC,
    FakeConsumer,
    FakeMessage,
    eof_message,
    eof_message_at,
    make_reader,
    offset_out_of_range_message,
    read_all,
)

pytestmark = pytest.mark.spark(version=">=4.0", reason="Uses the Python DataSource API")


def read_planned_partition(reader, consumer, messages, *, watermarks_after=None):
    consumer.messages = messages
    consumer.watermarks_after_assign = watermarks_after
    return reader.partitions()[0]


def test_read_reads_the_planned_range(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [FakeMessage(i) for i in range(3)])
    rows = read_all(reader, partition)
    assert [r["offset"] for r in rows] == [0, 1, 2]
    assert [r["value"] for r in rows] == [b"v0", b"v1", b"v2"]


def test_read_eof_within_available_range_stops_cleanly(consumer_factory):
    # Compacted or aborted-transaction offsets are unreadable but not lost:
    # the high watermark still covers the planned end, so Spark skips them.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [FakeMessage(0), eof_message()])
    rows = read_all(reader, partition)
    assert [r["offset"] for r in rows] == [0]


def test_read_eof_below_shrunken_watermark_is_data_loss(consumer_factory):
    # The log no longer reaches the planned end: records that existed at
    # planning time are gone, which Spark reports as data loss.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(0), eof_message()],
        watermarks_after={(TOPIC, 0): (0, 1)},
    )
    with pytest.raises(ValueError, match="Data loss"):
        read_all(reader, partition)


def test_read_fail_on_data_loss_false_returns_partial(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer, failOnDataLoss="false")
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(0), eof_message()],
        watermarks_after={(TOPIC, 0): (0, 1)},
    )
    assert [r["offset"] for r in read_all(reader, partition)] == [0]


def test_read_out_of_range_offset_is_data_loss_by_default(consumer_factory):
    # `auto.offset.reset=error` surfaces the offset the broker will not
    # serve instead of moving the position, which is what lets
    # `failOnDataLoss=true` report it.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [offset_out_of_range_message()])
    with pytest.raises(ValueError, match="no longer available on the broker"):
        read_all(reader, partition)


def test_read_fail_on_data_loss_false_recovers_within_the_planned_range(consumer_factory):
    # Spark's `getEarliestAvailableOffsetBetween`: resume at the earliest
    # offset that is both still in the log and inside the planned range.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer, failOnDataLoss="false")
    partition = read_planned_partition(
        reader,
        consumer,
        [offset_out_of_range_message(), FakeMessage(2), FakeMessage(3), FakeMessage(4)],
        watermarks_after={(TOPIC, 0): (2, 5)},
    )
    assert [r["offset"] for r in read_all(reader, partition)] == [2, 3, 4]
    assert consumer.assignments == [[(TOPIC, 0, 0)], [(TOPIC, 0, 2)]]


@pytest.mark.parametrize("watermarks", [(7, 9), (2, 2)])
def test_read_fail_on_data_loss_false_stops_when_nothing_overlaps(consumer_factory, watermarks):
    # The planned range and what is left of the log have no overlap, so
    # there is nothing to rewind to. Under `auto.offset.reset=earliest` this
    # is where librdkafka would instead replay the whole partition.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)})
    reader = make_reader(consumer_factory, consumer, failOnDataLoss="false")
    partition = read_planned_partition(
        reader,
        consumer,
        [offset_out_of_range_message()],
        watermarks_after={(TOPIC, 0): watermarks},
    )
    assert read_all(reader, partition) == []
    assert consumer.assignments == [[(TOPIC, 0, 0)]]


def test_read_forward_gap_within_the_log_is_skipped(consumer_factory):
    # Compacted offsets, transaction control markers and aborted records are
    # unreadable but not lost: the log still starts at or below the
    # requested offset, so Spark skips them.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [FakeMessage(3), FakeMessage(4)])
    assert [r["offset"] for r in read_all(reader, partition)] == [3, 4]


def test_read_forward_gap_below_the_log_start_is_data_loss(consumer_factory):
    # Same shape of gap, different cause: the log now starts above the
    # requested offset, so the records in between are gone rather than
    # merely invisible. Accepting this silently is what a loop that only
    # tracks the last record delivered would do.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(3), FakeMessage(4)],
        watermarks_after={(TOPIC, 0): (3, 5)},
    )
    with pytest.raises(ValueError, match="no longer in the log"):
        read_all(reader, partition)


def test_read_forward_gap_below_the_log_start_is_skipped_when_allowed(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer, failOnDataLoss="false")
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(3), FakeMessage(4)],
        watermarks_after={(TOPIC, 0): (3, 5)},
    )
    assert [r["offset"] for r in read_all(reader, partition)] == [3, 4]


def test_read_stale_eof_does_not_truncate_the_batch(consumer_factory):
    # librdkafka stamps the EOF event with the position it had reached when
    # it queued the event, not when the application polls it. Records queued
    # behind it are still to come, so stopping on it drops them.
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(0), eof_message_at(1), FakeMessage(1), FakeMessage(2)],
    )
    assert [r["offset"] for r in read_all(reader, partition)] == [0, 1, 2]


def test_read_eof_at_the_planned_end_completes_the_read(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [FakeMessage(0), eof_message_at(3)])
    assert [r["offset"] for r in read_all(reader, partition)] == [0]


def test_read_eof_at_planned_end_advancing_past_deleted_offsets_is_data_loss(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(0), eof_message_at(3)],
        watermarks_after={(TOPIC, 0): (3, 3)},
    )
    with pytest.raises(ValueError, match="no longer in the log"):
        read_all(reader, partition)


def test_read_stale_eof_advancing_past_deleted_offsets_is_data_loss(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(0), eof_message_at(3), FakeMessage(3), FakeMessage(4)],
        watermarks_after={(TOPIC, 0): (3, 5)},
    )
    with pytest.raises(ValueError, match="no longer in the log"):
        read_all(reader, partition)


def test_read_eof_without_offset_at_deleted_range_is_data_loss(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(0), eof_message()],
        watermarks_after={(TOPIC, 0): (5, 5)},
    )
    with pytest.raises(ValueError, match="no longer in the log"):
        read_all(reader, partition)


def test_read_eof_below_the_planned_end_with_a_shrunken_log_is_data_loss(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [FakeMessage(0), eof_message_at(1)],
        watermarks_after={(TOPIC, 0): (0, 1)},
    )
    with pytest.raises(ValueError, match="Data loss"):
        read_all(reader, partition)


def test_read_stops_at_the_exclusive_end(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 2)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [FakeMessage(i) for i in range(4)])
    assert [r["offset"] for r in read_all(reader, partition)] == [0, 1]


def test_read_batches_are_capped_at_max_batch_rows(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)})
    reader = make_reader(consumer_factory, consumer, maxBatchRows="2")
    partition = read_planned_partition(reader, consumer, [FakeMessage(i) for i in range(5)])
    assert [batch.num_rows for batch in reader.read(partition)] == [2, 2, 1]


def test_read_poll_timeout_fails_without_retrying(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 1)})
    reader = make_reader(consumer_factory, consumer, pollTimeoutMs="1")
    partition = read_planned_partition(reader, consumer, [None, FakeMessage(0)])
    with pytest.raises(TimeoutError, match="Cannot fetch record for offset 0"):
        read_all(reader, partition)
    assert len(consumer.messages) == 1


def test_read_empty_poll_can_advance_past_invisible_records(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)}, positions=[2])
    reader = make_reader(consumer_factory, consumer, pollTimeoutMs="1")
    partition = read_planned_partition(reader, consumer, [None, FakeMessage(2)])
    assert [r["offset"] for r in read_all(reader, partition)] == [2]


def test_read_empty_poll_advancing_past_deleted_offsets_is_data_loss(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)}, positions=[2])
    reader = make_reader(consumer_factory, consumer, pollTimeoutMs="1")
    partition = read_planned_partition(
        reader,
        consumer,
        [None, FakeMessage(2)],
        watermarks_after={(TOPIC, 0): (2, 3)},
    )
    with pytest.raises(ValueError, match="no longer in the log"):
        read_all(reader, partition)


def test_read_timestamp_type_is_translated_to_spark_values(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 2)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(
        reader,
        consumer,
        [
            FakeMessage(0, ts=(1, 1700000000000)),  # librdkafka CREATE_TIME
            FakeMessage(1, ts=(2, 1700000000000)),  # librdkafka LOG_APPEND_TIME
        ],
    )
    assert [r["timestampType"] for r in read_all(reader, partition)] == [0, 1]


def test_read_missing_timestamp_keeps_sparks_minus_one_ms(consumer_factory):
    # Spark passes Kafka's -1ms through millisToMicros rather than emitting
    # null, surfacing 1969-12-31 23:59:59.999.
    import datetime

    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 1)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [FakeMessage(0, ts=(0, -1))])
    row = read_all(reader, partition)[0]
    assert row["timestampType"] == -1
    assert row["timestamp"] == datetime.datetime(1969, 12, 31, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc)


def test_read_tombstones_and_headers(consumer_factory):
    class CustomMessage(FakeMessage):
        def __init__(self, offset, *, key, value, headers):
            super().__init__(offset)
            self._key = key
            self._value = value
            self._headers = headers

        def key(self):
            return self._key

        def value(self):
            return self._value

        def headers(self):
            return self._headers

    messages = [
        CustomMessage(0, key=None, value=None, headers=None),
        CustomMessage(1, key=b"k1", value=b"", headers=[]),
        CustomMessage(2, key=b"k2", value=b"v2", headers=[("h", None)]),
        CustomMessage(3, key=b"k3", value=b"v3", headers=[("h", b"first"), ("h", b"second")]),
    ]
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, len(messages))})
    reader = make_reader(consumer_factory, consumer, includeHeaders="true")
    partition = read_planned_partition(reader, consumer, messages)
    rows = read_all(reader, partition)
    assert [(r["key"], r["value"], r["headers"]) for r in rows] == [
        (None, None, None),
        (b"k1", b"", None),
        (b"k2", b"v2", [{"key": "h", "value": None}]),
        (b"k3", b"v3", [{"key": "h", "value": b"first"}, {"key": "h", "value": b"second"}]),
    ]


def test_read_reader_consumer_is_closed(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 1)})
    reader = make_reader(consumer_factory, consumer)
    partition = read_planned_partition(reader, consumer, [FakeMessage(0)])
    read_all(reader, partition)
    assert consumer.closed


def test_read_lifecycle_reader_and_partitions_deserialize_in_a_fresh_process(consumer_factory):
    from pyspark import cloudpickle

    consumer = FakeConsumer()
    reader = make_reader(
        consumer_factory,
        consumer,
        includeHeaders="true",
        maxBatchBytes="1024",
        **{"kafka.sasl.password": "test-only-secret"},
    )
    partitions = reader.partitions()
    payload = cloudpickle.dumps((reader, partitions))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            dedent(
                """
                import sys
                from pyspark import cloudpickle
                from pysail.spark.datasource.kafka.models import TimestampStrategy

                reader, parts = cloudpickle.load(sys.stdin.buffer)
                assert reader.options.starting_ts_strategy is TimestampStrategy.ERROR
                assert reader.options.include_headers
                assert reader.options.max_batch_bytes == 1024
                assert reader.options.client_config['sasl.password'] == 'test-only-secret'
                assert not hasattr(parts[0], 'client_config')
                assert 'test-only-secret' not in repr(parts[0]) + repr(reader) + repr(reader.options)
                offset_range = parts[0].offset_range
                assert (offset_range.topic_partition.topic, offset_range.start, offset_range.end) == ('orders', -2, -1)
                print('deserialized')
                """
            ),
        ],
        # pytest also supports source checkouts without an editable installation.
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        input=payload,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr.decode()
    assert result.stdout.strip() == b"deserialized"


def test_read_lifecycle_consumer_is_closed_on_arrow_conversion_failure(consumer_factory):
    class InvalidTimestamp(FakeMessage):
        def timestamp(self):
            return (1, 2**63 - 1)

    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 1)}, messages=[InvalidTimestamp(0)])
    reader = make_reader(consumer_factory, consumer)
    with pytest.raises(OverflowError):
        read_all(reader, reader.partitions()[0])
    assert consumer.closed


def test_read_lifecycle_refreshes_the_log_start_after_an_earlier_harmless_gap(consumer_factory):
    class MovingLog(FakeConsumer):
        def poll(self, timeout=None):
            record = super().poll(timeout)
            if record.offset() == 5:
                self.watermarks[(TOPIC, 0)] = (5, 7)
            return record

    consumer = MovingLog(watermarks={(TOPIC, 0): (0, 7)}, messages=[FakeMessage(i) for i in [1, 5, 6]])
    reader = make_reader(consumer_factory, consumer)
    with pytest.raises(ValueError, match="no longer in the log"):
        read_all(reader, reader.partitions()[0])
    assert consumer.closed


def test_read_lifecycle_out_of_range_recovery_without_progress_is_bounded(consumer_factory):
    polls = 0

    class StuckConsumer(FakeConsumer):
        def poll(self, timeout=None):  # noqa: ARG002
            nonlocal polls
            polls += 1
            if polls > 10:
                pytest.fail("recovery looped without making progress")
            return offset_out_of_range_message()

    consumer = StuckConsumer()
    reader = make_reader(consumer_factory, consumer, failOnDataLoss="false")
    with pytest.raises(TimeoutError, match="out-of-range recovery made no progress"):
        read_all(reader, reader.partitions()[0])
    assert consumer.closed


def test_read_lifecycle_byte_budget_flushes_and_preserves_oversized_records(consumer_factory):
    class Payload(FakeMessage):
        def value(self):
            return b"x" * (1000 if self.offset() == 1 else 200)

    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 3)}, messages=[Payload(i) for i in range(3)])
    reader = make_reader(consumer_factory, consumer, maxBatchBytes="300")
    batches = list(reader.read(reader.partitions()[0]))
    assert [b.num_rows for b in batches] == [1, 1, 1]
    assert [len(r["value"]) for b in batches for r in b.to_pylist()] == [200, 1000, 200]


def test_flushed_batch_releases_buffered_payloads(consumer_factory):
    # Message accessors must be repeatable: size estimation reads value() too.
    first_payload = bytes(range(256)) * 4

    class Payload(FakeMessage):
        def value(self):
            return first_payload if self.offset() == 0 else b"second"

    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 2)}, messages=[Payload(0), Payload(1)])
    reader = make_reader(consumer_factory, consumer, maxBatchRows="2")
    references = sys.getrefcount(first_payload)
    generator = reader.read(reader.partitions()[0])
    try:
        batch = next(generator)
        assert batch.column("value").to_pylist() == [first_payload, b"second"]
        # The cursor may retain the latest message; earlier payloads must have
        # left the Python row buffer before backpressure pauses this generator.
        assert sys.getrefcount(first_payload) == references
    finally:
        generator.close()
    assert consumer.closed


def test_read_lifecycle_cleanup_error_does_not_replace_the_read_failure(consumer_factory, caplog):
    class BrokenClose(FakeConsumer):
        def close(self):
            msg = "cleanup failed with test-only-secret"
            raise RuntimeError(msg)

    consumer = BrokenClose(messages=[offset_out_of_range_message()])
    reader = make_reader(consumer_factory, consumer)
    with pytest.raises(ValueError, match="Data loss"):
        read_all(reader, reader.partitions()[0])
    assert "close" in caplog.text.lower()
    assert "test-only-secret" not in caplog.text


def test_read_lifecycle_recovery_logs_a_partial_read(consumer_factory, caplog):
    consumer = FakeConsumer(
        messages=[offset_out_of_range_message(), FakeMessage(8), FakeMessage(9)],
        watermarks_after_assign={(TOPIC, 0): (8, 10)},
    )
    reader = make_reader(consumer_factory, consumer, failOnDataLoss="false")
    assert [r["offset"] for r in read_all(reader, reader.partitions()[0])] == [8, 9]
    assert "partial read on orders[0]" in caplog.text


@pytest.mark.parametrize("path", ["bind", "gap", "eof", "recover"])
def test_watermark_timeout_has_context_and_closes_consumer(consumer_factory, monkeypatch, path):
    from confluent_kafka import KafkaError, KafkaException

    def timeout(*_args, **_kwargs):
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT))  # noqa: SLF001

    messages = {
        "bind": [],
        "gap": [FakeMessage(3)],
        "eof": [eof_message_at(1)],
        "recover": [offset_out_of_range_message()],
    }
    consumer = FakeConsumer(messages=messages[path])
    options = (
        {}
        if path == "bind"
        else {
            "startingOffsets": '{"orders":{"0":0}}',
            "endingOffsets": '{"orders":{"0":5}}',
        }
    )
    if path == "recover":
        options["failOnDataLoss"] = "false"
    reader = make_reader(consumer_factory, consumer, **options)
    partition = reader.partitions()[0]
    monkeypatch.setattr(consumer, "get_watermark_offsets", timeout)
    with pytest.raises(TimeoutError, match=r"orders\[0\].*adminTimeoutMs"):
        read_all(reader, partition)
    assert consumer.closed


def test_watermark_broker_error_is_preserved(consumer_factory, monkeypatch):
    from confluent_kafka import KafkaError, KafkaException

    error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)

    def denied(*_args, **_kwargs):
        raise KafkaException(error)

    consumer = FakeConsumer()
    reader = make_reader(consumer_factory, consumer)
    partition = reader.partitions()[0]
    monkeypatch.setattr(consumer, "get_watermark_offsets", denied)
    with pytest.raises(KafkaException) as exc:
        read_all(reader, partition)
    assert exc.value.args[0] is error
    assert consumer.closed


def test_gap_error_is_limited_to_requested_offsets(consumer_factory):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (3, 11)}, messages=[FakeMessage(10)])
    reader = make_reader(
        consumer_factory, consumer, startingOffsets='{"orders":{"0":0}}', endingOffsets='{"orders":{"0":5}}'
    )
    with pytest.raises(ValueError, match="offsets 0 to 4 were requested"):
        read_all(reader, reader.partitions()[0])


def test_permissive_gaps_need_no_watermark_lookup(consumer_factory, monkeypatch, caplog):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 199)}, messages=[FakeMessage(i) for i in range(0, 199, 2)])
    reader = make_reader(
        consumer_factory,
        consumer,
        failOnDataLoss="false",
        maxBatchRows="25",
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":199}}',
    )
    calls = []
    original = consumer.get_watermark_offsets

    def query(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(consumer, "get_watermark_offsets", query)
    rows = read_all(reader, reader.partitions()[0])
    assert [r["offset"] for r in rows] == list(range(0, 199, 2))
    assert not calls
    assert "partial read" not in caplog.text


def test_permissive_gaps_do_not_classify_retention(consumer_factory, caplog):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (3, 5)}, messages=[FakeMessage(3), FakeMessage(4)])
    reader = make_reader(
        consumer_factory,
        consumer,
        failOnDataLoss="false",
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":5}}',
    )
    assert [r["offset"] for r in read_all(reader, reader.partitions()[0])] == [3, 4]
    assert "partial read" not in caplog.text


def test_truncated_eof_logs_a_partial_read(consumer_factory, caplog):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (2, 3)}, messages=[FakeMessage(2), eof_message_at(3)])
    reader = make_reader(
        consumer_factory,
        consumer,
        failOnDataLoss="false",
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":5}}',
    )
    assert [r["offset"] for r in read_all(reader, reader.partitions()[0])] == [2]
    assert caplog.text.count("partial read on orders[0]") == 1


def test_strict_gaps_use_cached_watermarks_without_broker_rpc(consumer_factory, monkeypatch):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 9)}, messages=[FakeMessage(i) for i in range(0, 9, 2)])
    reader = make_reader(
        consumer_factory,
        consumer,
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":9}}',
    )
    calls: list[bool] = []
    original = consumer.get_watermark_offsets

    def spy(*args, cached=False, **kwargs):
        calls.append(cached)
        return original(*args, cached=cached, **kwargs)

    monkeypatch.setattr(consumer, "get_watermark_offsets", spy)
    rows = read_all(reader, reader.partitions()[0])
    assert [r["offset"] for r in rows] == [0, 2, 4, 6, 8]
    assert calls
    assert all(calls), f"expected only cached watermark lookups on gaps, got cached flags: {calls}"


@pytest.mark.parametrize("cached_result", [None, (-1001, -1001), (-1001, 5), (0, -1001)])
def test_strict_gaps_fall_back_to_live_query_when_cached_watermark_is_invalid(
    consumer_factory, monkeypatch, cached_result
):
    consumer = FakeConsumer(watermarks={(TOPIC, 0): (0, 5)}, messages=[FakeMessage(3), FakeMessage(4)])
    reader = make_reader(
        consumer_factory,
        consumer,
        startingOffsets='{"orders":{"0":0}}',
        endingOffsets='{"orders":{"0":5}}',
    )
    calls: list[bool] = []

    def fallback_spy(_tp, timeout=None, *, cached=False):  # noqa: ARG001
        calls.append(cached)
        if cached:
            return cached_result
        return (0, 5)

    monkeypatch.setattr(consumer, "get_watermark_offsets", fallback_spy)
    rows = read_all(reader, reader.partitions()[0])
    assert [r["offset"] for r in rows] == [3, 4]
    assert calls == [True, False]
