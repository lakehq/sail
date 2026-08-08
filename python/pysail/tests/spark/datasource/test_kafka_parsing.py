"""Unit tests for pure option-parsing helpers in the Kafka data source.

These don't touch Kafka or Docker — they cover the parsing/validation surface
that would otherwise only be exercised through the integration tests.
"""

from __future__ import annotations

import pytest

from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 1):
    pytest.skip("Python data source requires Spark 4.1+", allow_module_level=True)

pytest.importorskip("confluent_kafka")

from pysail.spark.datasource.kafka import (
    KafkaDataSource,
    _build_consumer_config,
    _extract_kafka_config,
    _parse_offset_spec,
    _parse_timestamp_spec,
    _validate_offset_spec,
)

_TS_MS = 1700000000000
_POLL_TIMEOUT_MS = 250

# ---------------------------------------------------------------------------
# _parse_offset_spec
# ---------------------------------------------------------------------------


class TestParseOffsetSpec:
    def test_none_returns_default(self):
        assert _parse_offset_spec(None, default="earliest") == "earliest"
        assert _parse_offset_spec(None, default=None) is None

    def test_blank_returns_default(self):
        assert _parse_offset_spec("", default="latest") == "latest"
        assert _parse_offset_spec("   ", default="latest") == "latest"

    def test_earliest_sentinel(self):
        assert _parse_offset_spec("earliest", default=None) == "earliest"

    def test_latest_sentinel(self):
        assert _parse_offset_spec("latest", default=None) == "latest"

    def test_stripped_sentinel(self):
        assert _parse_offset_spec("  earliest  ", default=None) == "earliest"

    def test_json_object(self):
        result = _parse_offset_spec('{"t": {"0": 5}}', default=None)
        assert result == {"t": {"0": 5}}

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid offset spec"):
            _parse_offset_spec("not-json", default=None)

    def test_non_object_json_raises(self):
        with pytest.raises(ValueError, match="must be an object"):
            _parse_offset_spec("[1, 2, 3]", default=None)

    def test_non_object_topic_value_raises(self):
        with pytest.raises(ValueError, match="topic 't' must be an object"):
            _parse_offset_spec('{"t": 5}', default=None)

    def test_non_int_partition_value_raises(self):
        with pytest.raises(ValueError, match="partition '0' must be an integer"):
            _parse_offset_spec('{"t": {"0": "x"}}', default=None)

    def test_bool_partition_value_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _parse_offset_spec('{"t": {"0": true}}', default=None)


# ---------------------------------------------------------------------------
# _parse_timestamp_spec
# ---------------------------------------------------------------------------


class TestParseTimestampSpec:
    def test_both_unset_returns_none(self):
        assert _parse_timestamp_spec(None, None, endpoint="starting") is None
        assert _parse_timestamp_spec("", "", endpoint="starting") is None

    def test_global_int(self):
        assert _parse_timestamp_spec("1700000000000", None, endpoint="starting") == _TS_MS

    def test_global_int_stripped(self):
        assert _parse_timestamp_spec("  1700000000000  ", None, endpoint="ending") == _TS_MS

    def test_per_tp_json(self):
        result = _parse_timestamp_spec(None, '{"t": {"0": 1700000000000}}', endpoint="starting")
        assert result == {"t": {"0": _TS_MS}}

    def test_both_set_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            _parse_timestamp_spec("1000", '{"t": {}}', endpoint="starting")

    def test_endpoint_appears_in_error(self):
        with pytest.raises(ValueError, match=r"'ending"):
            _parse_timestamp_spec("1000", '{"t": {}}', endpoint="ending")

    def test_invalid_global_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _parse_timestamp_spec("not-a-number", None, endpoint="starting")

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid 'starting"):
            _parse_timestamp_spec(None, "not-json", endpoint="starting")

    def test_non_object_json_raises(self):
        with pytest.raises(ValueError, match="must be an object"):
            _parse_timestamp_spec(None, "[1, 2, 3]", endpoint="starting")

    def test_non_object_topic_value_raises(self):
        with pytest.raises(ValueError, match="topic 't' must be an object"):
            _parse_timestamp_spec(None, '{"t": 5}', endpoint="starting")

    def test_non_int_partition_value_raises(self):
        with pytest.raises(ValueError, match="partition '0' must be an integer"):
            _parse_timestamp_spec(None, '{"t": {"0": "x"}}', endpoint="ending")


# ---------------------------------------------------------------------------
# _extract_kafka_config
# ---------------------------------------------------------------------------


class TestExtractKafkaConfig:
    def test_strips_kafka_prefix(self):
        opts = {
            "kafka.bootstrap.servers": "localhost:9092",
            "kafka.security.protocol": "SASL_SSL",
            "subscribe": "orders",
            "includeHeaders": "true",
        }
        assert _extract_kafka_config(opts) == {
            "bootstrap.servers": "localhost:9092",
            "security.protocol": "SASL_SSL",
        }

    def test_no_kafka_options(self):
        assert _extract_kafka_config({"subscribe": "t"}) == {}

    def test_empty_dict(self):
        assert _extract_kafka_config({}) == {}


# ---------------------------------------------------------------------------
# _build_consumer_config
# ---------------------------------------------------------------------------


class TestBuildConsumerConfig:
    def test_defaults_applied(self):
        config = _build_consumer_config({"bootstrap.servers": "localhost:9092"}, role="reader")
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == "sail-kafka-reader"
        assert config["enable.auto.commit"] is False
        assert config["auto.offset.reset"] == "error"

    def test_role_reflected_in_default_group_id(self):
        planner = _build_consumer_config({}, role="planner")
        reader = _build_consumer_config({}, role="reader")
        assert planner["group.id"] == "sail-kafka-planner"
        assert reader["group.id"] == "sail-kafka-reader"

    def test_user_group_id_overrides_default(self):
        config = _build_consumer_config(
            {"bootstrap.servers": "localhost:9092", "group.id": "my-group"},
            role="reader",
        )
        assert config["group.id"] == "my-group"

    def test_user_enable_auto_commit_overrides_default(self):
        # Users can override safety defaults if they really want to — we log
        # nothing about it. Round-trip verifies the merge order.
        config = _build_consumer_config(
            {"bootstrap.servers": "localhost:9092", "enable.auto.commit": "true"},
            role="reader",
        )
        assert config["enable.auto.commit"] == "true"


# ---------------------------------------------------------------------------
# KafkaDataSource._resolved — offset/timestamp mutual exclusion.
# `_resolved` only parses options (no broker I/O), so it is unit-testable.
# ---------------------------------------------------------------------------


def _resolved(**extra):
    opts = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t", **extra}
    return KafkaDataSource(options=opts)._resolved  # noqa: SLF001


class TestResolvedMutualExclusion:
    def test_blank_starting_offsets_with_timestamp_ok(self):
        # Spark treats a blank offsets value as unset, so it must not conflict
        # with a timestamp-based start.
        resolved = _resolved(startingOffsets="", startingTimestamp="1700000000000")
        assert resolved["starting_offsets"] is None
        assert resolved["starting_timestamps"] == _TS_MS

    def test_whitespace_ending_offsets_with_timestamp_ok(self):
        resolved = _resolved(endingOffsets="   ", endingTimestamp="1700000000000")
        assert resolved["ending_offsets"] is None
        assert resolved["ending_timestamps"] == _TS_MS

    def test_real_starting_offsets_with_timestamp_raises(self):
        with pytest.raises(ValueError, match="cannot be combined"):
            _resolved(startingOffsets="earliest", startingTimestamp="1700000000000")

    def test_real_ending_offsets_with_timestamp_raises(self):
        with pytest.raises(ValueError, match="cannot be combined"):
            _resolved(endingOffsets="latest", endingTimestamp="1700000000000")


# ---------------------------------------------------------------------------
# Topic list handling
# ---------------------------------------------------------------------------


class TestResolvedTopics:
    def test_duplicate_topics_deduped(self):
        # A repeated topic would otherwise be enumerated once per occurrence,
        # producing duplicate input partitions over the same offset range and
        # emitting every row twice.
        assert _resolved(subscribe="t,t, t")["topics"] == ["t"]

    def test_dedup_preserves_first_seen_order(self):
        assert _resolved(subscribe="b,a,b,c")["topics"] == ["b", "a", "c"]

    def test_distinct_topics_unchanged(self):
        assert _resolved(subscribe="a, b ,c")["topics"] == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# _validate_offset_spec — Spark batch-mode sentinel rules
# ---------------------------------------------------------------------------


class TestValidateOffsetSpec:
    def test_none_is_noop(self):
        _validate_offset_spec(None, endpoint="starting", option_name="startingOffsets")
        _validate_offset_spec(None, endpoint="ending", option_name="endingOffsets")

    def test_allowed_string_sentinels(self):
        _validate_offset_spec("earliest", endpoint="starting", option_name="startingOffsets")
        _validate_offset_spec("latest", endpoint="ending", option_name="endingOffsets")

    def test_latest_rejected_for_starting(self):
        with pytest.raises(ValueError, match="does not support 'latest'"):
            _validate_offset_spec("latest", endpoint="starting", option_name="startingOffsets")

    def test_earliest_rejected_for_ending(self):
        with pytest.raises(ValueError, match="does not support 'earliest'"):
            _validate_offset_spec("earliest", endpoint="ending", option_name="endingOffsets")

    def test_allowed_json_sentinels(self):
        _validate_offset_spec({"t": {"0": -2}}, endpoint="starting", option_name="startingOffsets")
        _validate_offset_spec({"t": {"0": -1}}, endpoint="ending", option_name="endingOffsets")

    def test_json_latest_rejected_for_starting(self):
        with pytest.raises(ValueError, match="does not support offset -1"):
            _validate_offset_spec({"t": {"0": -1}}, endpoint="starting", option_name="startingOffsets")

    def test_json_earliest_rejected_for_ending(self):
        with pytest.raises(ValueError, match="does not support offset -2"):
            _validate_offset_spec({"t": {"0": -2}}, endpoint="ending", option_name="endingOffsets")

    def test_concrete_offsets_allowed(self):
        _validate_offset_spec({"t": {"0": 0, "1": 12345}}, endpoint="starting", option_name="startingOffsets")
        _validate_offset_spec({"t": {"0": 0, "1": 12345}}, endpoint="ending", option_name="endingOffsets")

    @pytest.mark.parametrize("endpoint", ["starting", "ending"])
    def test_offset_below_negative_two_rejected(self, endpoint):
        with pytest.raises(ValueError, match="must be >= 0"):
            _validate_offset_spec({"t": {"0": -3}}, endpoint=endpoint, option_name="opt")

    @pytest.mark.parametrize("offset", [-1000, -1001])
    def test_librdkafka_sentinels_rejected(self, offset):
        # -1000 is OFFSET_STORED and -1001 is OFFSET_INVALID. Passed through,
        # OFFSET_STORED would silently read from committed group offsets.
        with pytest.raises(ValueError, match="must be >= 0"):
            _validate_offset_spec({"t": {"0": offset}}, endpoint="starting", option_name="startingOffsets")

    def test_error_names_the_offending_partition(self):
        with pytest.raises(ValueError, match=r"topic 't' partition '3'"):
            _validate_offset_spec({"t": {"0": 5, "3": -7}}, endpoint="starting", option_name="startingOffsets")


class TestResolvedOffsetValidation:
    """The validator must be wired into both call sites in `_resolved`."""

    def test_starting_latest_rejected(self):
        with pytest.raises(ValueError, match="does not support 'latest'"):
            _resolved(startingOffsets="latest")

    def test_ending_earliest_rejected(self):
        with pytest.raises(ValueError, match="does not support 'earliest'"):
            _resolved(endingOffsets="earliest")

    def test_starting_json_latest_rejected(self):
        with pytest.raises(ValueError, match="does not support offset -1"):
            _resolved(startingOffsets='{"t": {"0": -1}}')

    def test_ending_json_earliest_rejected(self):
        with pytest.raises(ValueError, match="does not support offset -2"):
            _resolved(endingOffsets='{"t": {"0": -2}}')

    def test_legal_sentinel_combination_accepted(self):
        # The combination used by the integration suite.
        resolved = _resolved(startingOffsets='{"t": {"0": -2}}', endingOffsets='{"t": {"0": -1}}')
        assert resolved["starting_offsets"] == {"t": {"0": -2}}
        assert resolved["ending_offsets"] == {"t": {"0": -1}}

    def test_defaults_unaffected(self):
        resolved = _resolved()
        assert resolved["starting_offsets"] is None
        assert resolved["ending_offsets"] is None


# ---------------------------------------------------------------------------
# Numeric option validation
# ---------------------------------------------------------------------------


class TestSpecTopicValidation:
    """A per-topic spec naming an unsubscribed topic must fail loudly.

    Resolution looks specs up by subscribed topic, so an unmatched key is
    otherwise dropped silently and the partition quietly keeps its default
    watermark.
    """

    @pytest.mark.parametrize(
        "option",
        ["startingOffsets", "endingOffsets", "startingOffsetsByTimestamp", "endingOffsetsByTimestamp"],
    )
    def test_unknown_topic_rejected(self, option):
        with pytest.raises(ValueError, match="not in 'subscribe'"):
            _resolved(subscribe="orders", **{option: '{"ordres": {"0": 5}}'})

    def test_error_lists_the_unknown_topics(self):
        with pytest.raises(ValueError, match=r"\['a', 'b'\]"):
            _resolved(subscribe="orders", startingOffsets='{"a": {"0": 1}, "b": {"0": 2}}')

    def test_subscribed_topic_accepted(self):
        assert _resolved(subscribe="orders", startingOffsets='{"orders": {"0": 5}}')["starting_offsets"] == {
            "orders": {"0": 5}
        }

    def test_string_spec_unaffected(self):
        assert _resolved(subscribe="orders", startingOffsets="earliest")["starting_offsets"] == "earliest"

    def test_global_timestamp_spec_unaffected(self):
        assert _resolved(subscribe="orders", startingTimestamp=str(_TS_MS))["starting_timestamps"] == _TS_MS


class TestNumericOptionValidation:
    @pytest.mark.parametrize("option", ["maxBatchRows", "maxEmptyPolls", "pollTimeoutMs", "adminTimeoutMs"])
    def test_non_numeric_value_names_the_option(self, option):
        # A bare int()/float() failure reports only the offending literal, giving
        # the user no way to tell which option was rejected.
        with pytest.raises(ValueError, match=rf"'{option}' must be"):
            _resolved(**{option: "abc"})

    @pytest.mark.parametrize("option", ["maxBatchRows", "maxEmptyPolls", "adminTimeoutMs"])
    def test_non_positive_rejected(self, option):
        with pytest.raises(ValueError, match=rf"'{option}' must be positive"):
            _resolved(**{option: "0"})

    def test_defaults_applied(self):
        resolved = _resolved()
        assert resolved["max_batch_rows"] == 10000  # noqa: PLR2004
        assert resolved["max_empty_polls"] == 30  # noqa: PLR2004
        assert resolved["admin_timeout_s"] == 10.0  # noqa: PLR2004


class TestPollTimeoutValidation:
    @pytest.mark.parametrize("value", ["0", "-1", "-1000", "-0.5"])
    def test_non_positive_rejected(self, value):
        # A negative timeout makes `consumer.poll()` block indefinitely, which
        # defeats the `maxEmptyPolls` stall guard; zero spins the read loop.
        with pytest.raises(ValueError, match="'pollTimeoutMs' must be positive"):
            _resolved(pollTimeoutMs=value)

    def test_positive_accepted(self):
        assert _resolved(pollTimeoutMs=str(_POLL_TIMEOUT_MS))["poll_timeout_s"] == _POLL_TIMEOUT_MS / 1000

    def test_default_applied(self):
        assert _resolved()["poll_timeout_s"] == 1.0
