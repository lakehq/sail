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
    _build_consumer_config,
    _extract_kafka_config,
    _parse_offset_spec,
    _parse_timestamp_spec,
)

_TS_MS = 1700000000000

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
