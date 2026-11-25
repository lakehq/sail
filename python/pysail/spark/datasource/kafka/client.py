# SPDX-License-Identifier: Apache-2.0

"""confluent-kafka (librdkafka) client construction, lifecycle, and error translation."""

from __future__ import annotations

import logging
from concurrent.futures import TimeoutError as FuturesTimeoutError
from contextlib import contextmanager
from typing import TYPE_CHECKING, TypeVar

from confluent_kafka import Consumer, IsolationLevel, KafkaError, KafkaException, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient

from pysail.spark.datasource.kafka.models import TopicPartitionKey, Watermarks

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from concurrent.futures import Future

    from confluent_kafka.admin import OffsetSpec, TopicDescription

LOGGER = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
# Client configuration
# ----------------------------------------------------------------------------

# Stable group id: reads use explicit assign() without group membership or commits,
# so concurrent queries do not coordinate or mutate group state. Overridable via kafka.group.id.
DEFAULT_GROUP_ID = "sail-kafka-reader"

# Consumer-only properties stripped from AdminClient (producer-backed in librdkafka)
# to avoid CONFWARN logs. Planning passes the isolation level to list_offsets explicitly.
CONSUMER_ONLY_CONFIGS = frozenset(
    {
        "group.id",
        "isolation.level",
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
    }
)


def build_admin_config(client_config: Mapping[str, str]) -> dict[str, str]:
    """Return the librdkafka config for the planning-time ``AdminClient``."""
    return {k: v for k, v in client_config.items() if k not in CONSUMER_ONLY_CONFIGS}


# The Java client's default isolation level and the only spellings its ConsumerConfig accepts:
# kafka-clients 3.9 defines isolation.level with ConfigDef.ValidString.in("read_committed",
# "read_uncommitted"), an exact, case-sensitive match, and Spark's executor consumers inherit it.
# librdkafka accepts any letter case, so the check is done here
# (see options.ISOLATION_LEVEL_OPTION for the planning side).
DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.name.lower()
ISOLATION_LEVEL_VALUES = tuple(sorted(level.name.lower() for level in IsolationLevel))


def build_consumer_config(client_config: Mapping[str, str]) -> dict[str, str | bool]:
    """Return the effective librdkafka config for a batch-read consumer.

    ``isolation.level`` is the user's raw value, validated as Spark's executor
    consumers validate it, so e.g. ``READ_COMMITTED`` fails here even though
    planning accepted it. Source-owned settings are applied last:
    ``auto.offset.reset=error`` keeps unserviceable offsets visible to the read
    loop's ``failOnDataLoss`` logic.
    """
    isolation_level = client_config.get("isolation.level", DEFAULT_ISOLATION_LEVEL)
    if isolation_level not in ISOLATION_LEVEL_VALUES:
        msg = (
            f"Invalid value {isolation_level} for configuration isolation.level: "
            f"String must be one of: {', '.join(ISOLATION_LEVEL_VALUES)}"
        )
        raise ValueError(msg)
    return {
        "group.id": DEFAULT_GROUP_ID,
        **client_config,
        "isolation.level": isolation_level,
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
        "enable.partition.eof": True,
    }


# ----------------------------------------------------------------------------
# Client lifecycle
# ----------------------------------------------------------------------------


def create_admin_client(client_config: Mapping[str, str]) -> AdminClient:
    """Create a short-lived planning ``AdminClient``, matching Spark's ``KafkaOffsetReaderAdmin``.

    The Python ``AdminClient`` exposes no ``close()``; it is released when garbage collected.
    """
    return AdminClient(build_admin_config(client_config))


@contextmanager
def open_consumer(client_config: Mapping[str, str], topic_partition: TopicPartitionKey) -> Iterator[Consumer]:
    """Open a consumer and close it without leaking credentials or masking read errors.

    A failed close is re-raised only when the read itself succeeded.
    """
    consumer = Consumer(build_consumer_config(client_config))
    read_failed = False
    try:
        yield consumer
    except BaseException:
        read_failed = True
        raise
    finally:
        try:
            consumer.close()
        except Exception:
            # Omit client exception text (may contain security config).
            LOGGER.warning("Failed to close Kafka consumer for %s", topic_partition)
            if not read_failed:
                raise


# ----------------------------------------------------------------------------
# Broker calls and error translation
# ----------------------------------------------------------------------------

# Wall-clock cushion over librdkafka's request_timeout so an unresolved future cannot hang planning.
FUTURE_TIMEOUT_MARGIN_S = 5.0

# An offset the broker will not serve arrives two ways: as the broker's own
# ``OFFSET_OUT_OF_RANGE``, and as the local ``_AUTO_OFFSET_RESET`` librdkafka
# raises when ``auto.offset.reset=error`` declines to move the position itself.
OUT_OF_RANGE_CODES = frozenset(
    {
        KafkaError.OFFSET_OUT_OF_RANGE,
        KafkaError._AUTO_OFFSET_RESET,  # noqa: SLF001
    }
)


_Result = TypeVar("_Result")


def await_future(future: Future[_Result], *, timeout_s: float, what: str) -> _Result:
    """Resolve an admin-call future with a hard wall-clock bound."""
    try:
        return future.result(timeout=timeout_s + FUTURE_TIMEOUT_MARGIN_S)
    except FuturesTimeoutError as e:
        msg = (
            f"Timed out after {timeout_s:g}s waiting for the broker to {what}. "
            f"The broker may be unreachable or a partition leader unavailable; "
            f"raise 'adminTimeoutMs' if the cluster is simply slow."
        )
        raise TimeoutError(msg) from e


def kafka_error(exc: KafkaException) -> KafkaError | None:
    """Return the underlying ``KafkaError`` from a ``KafkaException``, if present."""
    if exc.args and isinstance(exc.args[0], KafkaError):
        return exc.args[0]
    return None


def query_watermarks(
    consumer: Consumer, topic_partition: TopicPartitionKey, *, timeout_s: float, cached: bool = False
) -> Watermarks:
    """Query broker watermarks, adding task context to client timeouts.

    When ``cached`` is true, prefer librdkafka's fetch-response watermarks
    (updated from each ``FetchResponse``'s ``log_start_offset``) and fall
    back to a live broker query if either cached watermark is unavailable.
    """
    kafka_topic_partition = TopicPartition(topic_partition.topic, topic_partition.partition)
    try:
        if cached:
            cached_result = consumer.get_watermark_offsets(kafka_topic_partition, cached=True)
            if cached_result is not None and all(offset >= 0 for offset in cached_result):
                return Watermarks(*cached_result)
        result = consumer.get_watermark_offsets(kafka_topic_partition, timeout=timeout_s, cached=False)
        if result is None:
            msg = f"Failed to query offsets for {topic_partition}"
            raise ValueError(msg)
        return Watermarks(*result)
    except KafkaException as e:
        error = kafka_error(e)
        if error is None or error.code() != KafkaError._TIMED_OUT:  # noqa: SLF001
            raise
        msg = (
            f"Timed out querying offsets for {topic_partition} after "
            f"{timeout_s:g}s; check broker connectivity or raise 'adminTimeoutMs'."
        )
        raise TimeoutError(msg) from e


def list_topics(admin: AdminClient, *, timeout_s: float) -> list[str]:
    """List broker topic names, adding context to metadata failures."""
    try:
        return list(admin.list_topics(timeout=timeout_s).topics)
    except KafkaException as e:
        error = kafka_error(e)
        if error is not None and error.code() == KafkaError._TIMED_OUT:  # noqa: SLF001
            msg = (
                f"Timed out after {timeout_s:g}s waiting for the broker to list topics. "
                f"The broker may be unreachable; raise 'adminTimeoutMs' if the cluster is simply slow."
            )
            raise TimeoutError(msg) from e
        msg = f"Failed to list topics on broker: {error or e}"
        raise ValueError(msg) from e


def describe_topics(admin: AdminClient, topics: Sequence[str], *, timeout_s: float) -> dict[str, TopicDescription]:
    """Fetch topic descriptions in one request, preserving broker metadata."""
    futures = admin.describe_topics(TopicCollection(list(topics)), request_timeout=timeout_s)
    out: dict[str, TopicDescription] = {}
    for topic, future in futures.items():
        try:
            description = await_future(future, timeout_s=timeout_s, what=f"describe topic {topic!r}")
        except KafkaException as e:
            error = kafka_error(e)
            if error is not None and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                msg = f"Topic {topic!r} not found on broker"
            else:
                msg = f"Topic {topic!r} is not available: {error or e}"
            raise ValueError(msg) from e
        out[topic] = description
    return out


def list_offsets(
    admin: AdminClient,
    specs: dict[TopicPartitionKey, OffsetSpec],
    *,
    timeout_s: float,
    isolation_level: IsolationLevel,
) -> dict[TopicPartitionKey, int]:
    """Resolve ``{TopicPartitionKey: OffsetSpec}`` in a single batched ``list_offsets`` call."""
    if not specs:
        return {}
    futures = admin.list_offsets(
        {TopicPartition(tp.topic, tp.partition): spec for tp, spec in specs.items()},
        isolation_level=isolation_level,
        request_timeout=timeout_s,
    )
    out: dict[TopicPartitionKey, int] = {}
    for kafka_topic_partition, future in futures.items():
        topic_partition = TopicPartitionKey(kafka_topic_partition.topic, kafka_topic_partition.partition)
        try:
            result = await_future(future, timeout_s=timeout_s, what=f"list offsets for {topic_partition}")
        except KafkaException as e:
            error = kafka_error(e)
            msg = f"Failed to look up offsets for {topic_partition}: {error or e}"
            raise ValueError(msg) from e
        out[topic_partition] = result.offset
    return out
