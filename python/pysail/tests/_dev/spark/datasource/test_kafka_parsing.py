"""Unit tests for pure option-parsing helpers in the Kafka data source.

These don't touch Kafka or Docker — they cover the parsing/validation surface
that would otherwise only be exercised through the integration tests.
"""

from __future__ import annotations

import itertools
import json

import pytest

from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 0):
    pytest.skip("Python data source requires Spark 4.0+", allow_module_level=True)

pytest.importorskip("confluent_kafka")

from confluent_kafka import Consumer, IsolationLevel, KafkaException
from confluent_kafka.admin import AdminClient
from pyspark.sql.datasource import CaseInsensitiveDict, DataSourceReader

from pysail.spark.datasource.kafka import (
    KafkaDataSource,
    _build_admin_config,
    _build_consumer_config,
    _divide_range,
    _parse_global_timestamp,
    _parse_offset_spec,
    _parse_subscription,
    _parse_timestamp_json,
    _planning_isolation_level,
    _resolve_range_option,
    _split_offset_ranges,
    _validate_kafka_config,
    _validate_offset_spec,
)

_TS_MS = 1700000000000
_POLL_TIMEOUT_MS = 250


@pytest.mark.parametrize("parser", [_parse_offset_spec, _parse_timestamp_json])
@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_partition_limit_value_width(parser, value):
    with pytest.raises(ValueError, match="signed 64-bit"):
        parser(json.dumps({"t": {"0": value}}), option_name="limit")


@pytest.mark.parametrize("parser", [_parse_offset_spec, _parse_timestamp_json])
@pytest.mark.parametrize("partition", [-(2**31) - 1, 2**31])
def test_partition_limit_id_width(parser, partition):
    with pytest.raises(ValueError, match="signed 32-bit"):
        parser(json.dumps({"t": {str(partition): 0}}), option_name="limit")


@pytest.mark.parametrize("partition", [-(2**31) - 1, 2**31])
def test_assignment_partition_id_width(partition):
    with pytest.raises(ValueError, match="signed 32-bit"):
        _parse_subscription(CaseInsensitiveDict({"assign": json.dumps({"t": [partition]})}))


# ---------------------------------------------------------------------------
# _parse_offset_spec
# ---------------------------------------------------------------------------


class TestParseOffsetSpec:
    def test_earliest_sentinel(self):
        assert _parse_offset_spec("earliest", option_name="startingOffsets") == "earliest"

    def test_latest_sentinel(self):
        assert _parse_offset_spec("latest", option_name="endingOffsets") == "latest"

    def test_stripped_sentinel(self):
        assert _parse_offset_spec("  earliest  ", option_name="startingOffsets") == "earliest"

    @pytest.mark.parametrize("raw", ["EARLIEST", "Earliest", "eArLiEsT"])
    def test_sentinel_is_case_insensitive(self, raw):
        # Spark lowercases before matching, so `EARLIEST` is a valid value.
        assert _parse_offset_spec(raw, option_name="startingOffsets") == "earliest"

    def test_blank_is_a_parse_error(self):
        # Spark distinguishes absent from present-but-blank: a present blank
        # value is parsed as JSON and fails. Only absence means "use default".
        with pytest.raises(ValueError, match="Invalid 'startingOffsets' value"):
            _parse_offset_spec("", option_name="startingOffsets")

    def test_json_object(self):
        assert _parse_offset_spec('{"t": {"0": 5}}', option_name="startingOffsets") == {"t": {"0": 5}}

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid 'startingOffsets' value"):
            _parse_offset_spec("not-json", option_name="startingOffsets")

    def test_non_object_json_raises(self):
        with pytest.raises(ValueError, match="must be an object"):
            _parse_offset_spec("[1, 2, 3]", option_name="startingOffsets")

    def test_non_object_topic_value_raises(self):
        with pytest.raises(ValueError, match="topic 't' must be an object"):
            _parse_offset_spec('{"t": 5}', option_name="startingOffsets")

    def test_non_int_partition_value_raises(self):
        with pytest.raises(ValueError, match="partition '0' must be an integer"):
            _parse_offset_spec('{"t": {"0": "x"}}', option_name="startingOffsets")

    def test_bool_partition_value_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _parse_offset_spec('{"t": {"0": true}}', option_name="startingOffsets")

    def test_non_numeric_partition_key_raises(self):
        with pytest.raises(ValueError, match="partition id for topic 't' must be an integer"):
            _parse_offset_spec('{"t": {"zero": 1}}', option_name="startingOffsets")

    @pytest.mark.parametrize("key", ["00", " 0", "+0"])
    def test_partition_key_is_canonicalized(self, key):
        # Spark reads these keys as `Map[Int, Long]`, so every spelling of 0 is
        # partition 0. Keeping the user's spelling passes the partition-set
        # check (which compares `int(key)`) and then misses the `str(partition)`
        # lookup during resolution, raising a bare `KeyError`.
        spec = _parse_offset_spec('{"t": {"%s": 5}}' % key, option_name="startingOffsets")  # noqa: UP031
        assert spec == {"t": {"0": 5}}

    def test_duplicate_partition_keys_collapse(self):
        # Two spellings of one id are one entry in the Scala `Map` too.
        assert _parse_offset_spec('{"t": {"0": 5, "00": 9}}', option_name="startingOffsets") == {"t": {"0": 9}}


# ---------------------------------------------------------------------------
# Timestamp option parsing
# ---------------------------------------------------------------------------


class TestParseTimestamps:
    def test_global_int(self):
        assert _parse_global_timestamp("1700000000000", option_name="startingTimestamp") == _TS_MS

    @pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
    def test_global_timestamp_width(self, value):
        with pytest.raises(ValueError, match="signed 64-bit"):
            _parse_global_timestamp(str(value), option_name="startingTimestamp")

    @pytest.mark.parametrize("value", [-(2**63), 2**63 - 1])
    def test_global_timestamp_width_boundaries(self, value):
        assert _parse_global_timestamp(str(value), option_name="startingTimestamp") == value

    def test_global_int_stripped(self):
        assert _parse_global_timestamp("  1700000000000  ", option_name="endingTimestamp") == _TS_MS

    def test_invalid_global_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _parse_global_timestamp("not-a-number", option_name="startingTimestamp")

    def test_partition_key_is_canonicalized(self):
        result = _parse_timestamp_json('{"t": {"00": 1700000000000}}', option_name="startingOffsetsByTimestamp")
        assert result == {"t": {"0": _TS_MS}}

    def test_per_tp_json(self):
        result = _parse_timestamp_json('{"t": {"0": 1700000000000}}', option_name="startingOffsetsByTimestamp")
        assert result == {"t": {"0": _TS_MS}}

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid 'startingOffsetsByTimestamp' JSON"):
            _parse_timestamp_json("not-json", option_name="startingOffsetsByTimestamp")

    def test_non_object_json_raises(self):
        with pytest.raises(ValueError, match="must be an object"):
            _parse_timestamp_json("[1, 2, 3]", option_name="startingOffsetsByTimestamp")

    def test_non_object_topic_value_raises(self):
        with pytest.raises(ValueError, match="topic 't' must be an object"):
            _parse_timestamp_json('{"t": 5}', option_name="startingOffsetsByTimestamp")

    def test_non_int_partition_value_raises(self):
        with pytest.raises(ValueError, match="partition '0' must be an integer"):
            _parse_timestamp_json('{"t": {"0": "x"}}', option_name="endingOffsetsByTimestamp")


# ---------------------------------------------------------------------------
# _resolve_range_option — Spark's precedence between the range options
# ---------------------------------------------------------------------------


class TestRangeOptionPrecedence:
    def test_absent_returns_none(self):
        assert _resolve_range_option(CaseInsensitiveDict({}), endpoint="starting") == (None, None)

    def test_offsets_only(self):
        offsets, timestamps = _resolve_range_option(
            CaseInsensitiveDict({"startingOffsets": "earliest"}), endpoint="starting"
        )
        assert offsets == "earliest"
        assert timestamps is None

    def test_global_timestamp_beats_everything(self):
        # Spark checks the global timestamp first and never looks at the rest,
        # so the lower-priority values are not even parsed.
        offsets, timestamps = _resolve_range_option(
            CaseInsensitiveDict(
                {
                    "startingTimestamp": "1700000000000",
                    "startingOffsetsByTimestamp": "not-even-json",
                    "startingOffsets": "not-even-json",
                }
            ),
            endpoint="starting",
        )
        assert offsets is None
        assert timestamps == _TS_MS

    def test_per_partition_timestamp_beats_offsets(self):
        offsets, timestamps = _resolve_range_option(
            CaseInsensitiveDict(
                {
                    "endingOffsetsByTimestamp": '{"t": {"0": 1700000000000}}',
                    "endingOffsets": "not-even-json",
                }
            ),
            endpoint="ending",
        )
        assert offsets is None
        assert timestamps == {"t": {"0": _TS_MS}}

    def test_combination_is_not_rejected(self):
        # Spark applies precedence rather than failing, so a job that supplies
        # a fallback keeps working.
        resolved = _resolved(startingOffsets="earliest", startingTimestamp="1700000000000")
        assert resolved["starting_offsets"] is None
        assert resolved["starting_timestamps"] == _TS_MS

    def test_blank_lower_priority_option_is_ignored(self):
        resolved = _resolved(endingOffsets="", endingTimestamp="1700000000000")
        assert resolved["ending_offsets"] is None
        assert resolved["ending_timestamps"] == _TS_MS

    def test_blank_winning_option_raises(self):
        with pytest.raises(ValueError, match="Invalid 'startingOffsets' value"):
            _resolved(startingOffsets="")


# ---------------------------------------------------------------------------
# startingOffsetsByTimestampStrategy
# ---------------------------------------------------------------------------


class TestTimestampStrategy:
    def test_default_is_error(self):
        assert _resolved()["starting_ts_strategy"] == "error"

    @pytest.mark.parametrize("raw", ["latest", "LATEST", " Latest "])
    def test_latest_accepted(self, raw):
        assert _resolved(startingOffsetsByTimestampStrategy=raw)["starting_ts_strategy"] == "latest"

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="startingOffsetsByTimestampStrategy"):
            _resolved(startingOffsetsByTimestampStrategy="earliest")


# ---------------------------------------------------------------------------
# Protected consumer properties
# ---------------------------------------------------------------------------


class TestProtectedKafkaConfigs:
    @pytest.mark.parametrize("name", ["auto.offset.reset", "enable.auto.commit", "enable.partition.eof"])
    def test_rejected(self, name):
        with pytest.raises(ValueError, match=f"kafka.{name}' is not supported".replace(".", r"\.")):
            _validate_kafka_config({"bootstrap.servers": "localhost:9092", name: "whatever"})

    def test_auto_offset_reset_names_the_alternative(self):
        with pytest.raises(ValueError, match="startingOffsets"):
            _validate_kafka_config({"auto.offset.reset": "earliest"})

    def test_group_id_still_allowed(self):
        _validate_kafka_config({"bootstrap.servers": "localhost:9092", "group.id": "mine"})

    def test_rejected_through_resolved(self):
        with pytest.raises(ValueError, match="is not supported"):
            _resolved(**{"kafka.enable.auto.commit": "true"})


# ---------------------------------------------------------------------------
# _build_consumer_config
# ---------------------------------------------------------------------------


class TestBuildConsumerConfig:
    def test_defaults_applied(self):
        config = _build_consumer_config({"bootstrap.servers": "localhost:9092"})
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == "sail-kafka-reader"
        assert config["enable.auto.commit"] is False
        assert config["auto.offset.reset"] == "error"
        assert config["enable.partition.eof"] is True

    def test_user_group_id_overrides_default(self):
        config = _build_consumer_config({"bootstrap.servers": "localhost:9092", "group.id": "my-group"})
        assert config["group.id"] == "my-group"

    def test_source_owned_settings_win(self):
        # `_validate_kafka_config` rejects these before we get here; the merge
        # order is the second line of defence, and Spark likewise keeps its own
        # consumer settings authoritative.
        config = _build_consumer_config(
            {"enable.auto.commit": "true", "auto.offset.reset": "earliest", "enable.partition.eof": "false"}
        )
        assert config["enable.auto.commit"] is False
        assert config["auto.offset.reset"] == "error"
        assert config["enable.partition.eof"] is True

    def test_isolation_level_matches_the_java_client_default(self):
        # librdkafka defaults to `read_committed`; Spark never sets the property
        # and so inherits the Java client's `read_uncommitted`. Left to the
        # librdkafka default, a transactional topic would silently return a
        # different row set here than under Spark.
        config = _build_consumer_config({})
        assert config["isolation.level"] == "read_uncommitted"

    def test_user_isolation_level_overrides_default(self):
        config = _build_consumer_config({"isolation.level": "read_committed"})
        assert config["isolation.level"] == "read_committed"


class TestPlanningIsolationLevel:
    def test_defaults_to_read_uncommitted(self):
        # `KafkaOffsetReaderAdmin` falls back to the Java client's default.
        assert _planning_isolation_level({}) == IsolationLevel.READ_UNCOMMITTED

    def test_follows_the_user_setting(self):
        # Spark derives `ListOffsetsOptions` from the driver Kafka params, so a
        # user asking for `read_committed` gets last-stable-offset semantics
        # when a "latest" endpoint or a timestamp is resolved.
        assert _planning_isolation_level({"isolation.level": "READ_COMMITTED"}) == IsolationLevel.READ_COMMITTED

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError, match="must be 'read_uncommitted' or 'read_committed'"):
            _planning_isolation_level({"isolation.level": "read_everything"})

    def test_resolved_config_carries_the_librdkafka_spelling(self):
        # Planning accepts any case, but librdkafka only accepts the lowercase
        # values. The executors' consumers must see the same setting planning
        # used, not the raw spelling that would fail when each task starts.
        client_config = _resolved(**{"kafka.isolation.level": " READ_COMMITTED "})["client_config"]
        assert client_config["isolation.level"] == "read_committed"
        assert _build_consumer_config(client_config)["isolation.level"] == "read_committed"
        assert _planning_isolation_level(client_config) == IsolationLevel.READ_COMMITTED

    def test_invalid_value_fails_option_validation(self):
        with pytest.raises(ValueError, match="must be 'read_uncommitted' or 'read_committed'"):
            _resolved(**{"kafka.isolation.level": "read_everything"})


class TestBuildAdminConfig:
    def test_connection_settings_pass_through(self):
        settings = {
            "bootstrap.servers": "localhost:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "test-user",
            "sasl.password": "test-only-secret",
            "ssl.ca.location": "/test/ca.pem",
            "enable.ssl.certificate.verification": "true",
        }
        config = _resolved(**{f"KAFKA.{key.upper()}": value for key, value in settings.items()})["client_config"]
        assert _build_admin_config(config) == settings
        assert all(_build_consumer_config(config)[key] == value for key, value in settings.items())

    @pytest.mark.parametrize(
        ("client", "build_config"), [(AdminClient, _build_admin_config), (Consumer, _build_consumer_config)]
    )
    def test_invalid_security_protocol_fails_without_plaintext_fallback(self, client, build_config):
        config = _resolved(**{"kafka.security.protocol": "not-a-protocol"})["client_config"]
        # Real librdkafka validation fails before it can open a connection.
        with pytest.raises(KafkaException, match=r"security\.protocol"):
            client(build_config(config))

    def test_consumer_only_properties_are_stripped(self):
        # librdkafka builds the admin handle on a producer and logs a CONFWARN
        # for every consumer property it finds there, so a user-supplied
        # `kafka.group.id` would warn on every planning call.
        config = _build_admin_config({"bootstrap.servers": "localhost:9092", "group.id": "my-group"})
        assert config == {"bootstrap.servers": "localhost:9092"}

    def test_isolation_level_is_stripped(self):
        # A consumer property too; planning passes it to `list_offsets` instead.
        config = _build_admin_config({"bootstrap.servers": "localhost:9092", "isolation.level": "read_committed"})
        assert config == {"bootstrap.servers": "localhost:9092"}


# ---------------------------------------------------------------------------
# KafkaDataSource._resolved — `_resolved` only parses options (no broker I/O),
# so it is unit-testable.
# ---------------------------------------------------------------------------


def _resolved(**extra):
    opts = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t", **extra}
    return KafkaDataSource(options=opts)._resolved  # noqa: SLF001


# ---------------------------------------------------------------------------
# Subscription strategies — assign / subscribe / subscribePattern
# ---------------------------------------------------------------------------


class TestSubscription:
    def test_subscribe_topic_list(self):
        assert _parse_subscription(CaseInsensitiveDict({"subscribe": "a, b ,c"})) == ("subscribe", ["a", "b", "c"])

    def test_subscribe_dedupes(self):
        # A repeated topic would otherwise be enumerated once per occurrence,
        # producing duplicate input partitions and emitting every row twice.
        assert _parse_subscription(CaseInsensitiveDict({"subscribe": "t,t, t"})) == ("subscribe", ["t"])

    def test_subscribe_preserves_first_seen_order(self):
        assert _parse_subscription(CaseInsensitiveDict({"subscribe": "b,a,b,c"})) == ("subscribe", ["b", "a", "c"])

    def test_subscribe_empty_raises(self):
        with pytest.raises(ValueError, match="at least one topic"):
            _parse_subscription(CaseInsensitiveDict({"subscribe": " , "}))

    def test_subscribe_pattern(self):
        strategy, pattern = _parse_subscription(CaseInsensitiveDict({"subscribePattern": "orders-.*"}))
        assert strategy == "subscribePattern"
        assert pattern.fullmatch("orders-eu")
        assert not pattern.fullmatch("shipments")

    def test_subscribe_pattern_invalid_regex_raises(self):
        with pytest.raises(ValueError, match="not a valid regular expression"):
            _parse_subscription(CaseInsensitiveDict({"subscribePattern": "orders-["}))

    def test_assign(self):
        strategy, value = _parse_subscription(CaseInsensitiveDict({"assign": '{"t": [0, 2]}'}))
        assert strategy == "assign"
        assert value == {"t": [0, 2]}

    def test_assign_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid 'assign' JSON"):
            _parse_subscription(CaseInsensitiveDict({"assign": "not-json"}))

    def test_assign_empty_object(self):
        # `JsonUtils.partitions` accepts an empty map; the assignment simply
        # selects nothing.
        assert _parse_subscription(CaseInsensitiveDict({"assign": "{}"})) == ("assign", {})

    def test_assign_empty_partition_list(self):
        assert _parse_subscription(CaseInsensitiveDict({"assign": '{"t": []}'})) == ("assign", {"t": []})

    def test_assign_negative_partition(self):
        # Spark deserializes `Seq[Int]` and filters against the broker's
        # partitions later, so an id that cannot exist is skipped, not rejected.
        assert _parse_subscription(CaseInsensitiveDict({"assign": '{"t": [-1]}'})) == ("assign", {"t": [-1]})

    def test_assign_bool_partition_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _parse_subscription(CaseInsensitiveDict({"assign": '{"t": [true]}'}))

    def test_none_specified_raises(self):
        with pytest.raises(ValueError, match="must be specified"):
            _parse_subscription(CaseInsensitiveDict({"kafka.bootstrap.servers": "localhost:9092"}))

    def test_more_than_one_raises(self):
        with pytest.raises(ValueError, match="Only one of the following options"):
            _parse_subscription(CaseInsensitiveDict({"subscribe": "t", "assign": '{"t": [0]}'}))


# ---------------------------------------------------------------------------
# Option key case-insensitivity
#
# Spark data source options are case-insensitive, and both PySpark and Sail
# construct the data source with a `CaseInsensitiveDict`. These tests pin that
# the same options work regardless of case, and — via the explicit
# `CaseInsensitiveDict` case — that `_resolved` does not flatten the mapping
# back into a case-sensitive dict, which would strand every camelCase lookup
# against the wrapper's lowercased keys.
# ---------------------------------------------------------------------------


class TestOptionCaseInsensitivity:
    def test_upper_case_option_keys(self):
        source = KafkaDataSource(
            options={
                "KAFKA.BOOTSTRAP.SERVERS": "localhost:9092",
                "SUBSCRIBE": "t",
                "STARTINGOFFSETS": "earliest",
                "INCLUDEHEADERS": "true",
            }
        )
        resolved = source._resolved  # noqa: SLF001
        assert resolved["client_config"]["bootstrap.servers"] == "localhost:9092"
        assert resolved["subscription"] == ("subscribe", ["t"])
        assert resolved["starting_offsets"] == "earliest"
        assert resolved["include_headers"] is True

    def test_lower_case_option_keys(self):
        resolved = _resolved(startingoffsets="earliest", maxbatchrows="99")
        assert resolved["starting_offsets"] == "earliest"
        assert resolved["max_batch_rows"] == 99  # noqa: PLR2004

    def test_case_insensitive_dict_input_is_not_flattened(self):
        # This is what PySpark and Sail actually pass to the constructor.
        source = KafkaDataSource(
            options=CaseInsensitiveDict(
                {
                    "kafka.bootstrap.servers": "localhost:9092",
                    "subscribe": "t",
                    "startingOffsets": "earliest",
                    "maxBatchRows": "77",
                }
            )
        )
        resolved = source._resolved  # noqa: SLF001
        assert resolved["starting_offsets"] == "earliest"
        assert resolved["max_batch_rows"] == 77  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Numeric options
# ---------------------------------------------------------------------------


class TestNumericOptions:
    def test_idle_deadline_defaults_to_the_effective_poll_budget(self):
        assert _resolved(pollTimeoutMs="80")["stall_timeout_s"] == pytest.approx(0.08)
        assert _resolved(pollTimeoutMs="80", stallTimeoutMs="20")["stall_timeout_s"] == pytest.approx(0.02)

    @pytest.mark.parametrize("option", ["maxBatchBytes", "maxBatchWaitMs"])
    @pytest.mark.parametrize("value", ["", "0", "-1", "nan", "inf"])
    def test_batch_resource_limits_reject_invalid_values(self, option, value):
        with pytest.raises(ValueError, match=option):
            _resolved(**{option: value})

    def test_poll_timeout_default_matches_spark(self):
        # Spark's batch default for kafkaConsumer.pollTimeoutMs is
        # spark.network.timeout, i.e. 120s.
        assert _resolved()["poll_timeout_s"] == 120.0  # noqa: PLR2004

    def test_spark_poll_timeout_option_name(self):
        assert _resolved(**{"kafkaConsumer.pollTimeoutMs": str(_POLL_TIMEOUT_MS)})["poll_timeout_s"] == 0.25  # noqa: PLR2004

    def test_short_poll_timeout_alias(self):
        assert _resolved(pollTimeoutMs=str(_POLL_TIMEOUT_MS))["poll_timeout_s"] == 0.25  # noqa: PLR2004

    def test_spark_name_wins_over_alias(self):
        resolved = _resolved(**{"kafkaConsumer.pollTimeoutMs": "500", "pollTimeoutMs": "250"})
        assert resolved["poll_timeout_s"] == 0.5  # noqa: PLR2004

    @pytest.mark.parametrize("raw", ["Infinity", "inf", "-inf", "nan", "NaN"])
    def test_non_finite_poll_timeout_rejected(self, raw):
        # `float()` accepts these and a `<= 0` check does not catch them:
        # `poll(inf)` blocks forever, defeating the stallTimeoutMs guard.
        with pytest.raises(ValueError, match=r"finite|must be positive"):
            _resolved(pollTimeoutMs=raw)

    @pytest.mark.parametrize("raw", ["0", "-1"])
    def test_non_positive_poll_timeout_rejected(self, raw):
        with pytest.raises(ValueError, match="must be positive"):
            _resolved(pollTimeoutMs=raw)

    def test_min_partitions_absent(self):
        assert _resolved()["min_partitions"] is None
        assert _resolved()["max_records_per_partition"] is None

    def test_min_partitions_parsed(self):
        assert _resolved(minPartitions="4")["min_partitions"] == 4  # noqa: PLR2004
        assert _resolved(maxRecordsPerPartition="100")["max_records_per_partition"] == 100  # noqa: PLR2004

    def test_min_partitions_must_be_positive(self):
        with pytest.raises(ValueError, match="must be positive"):
            _resolved(minPartitions="0")

    def test_min_partitions_is_bounded_by_int32(self):
        # Spark parses this with `String.toInt`, which rejects anything wider.
        # Python integers are unbounded, so without the bound the value goes
        # straight into the splitter as a chunk count.
        assert _resolved(minPartitions=str(2**31 - 1))["min_partitions"] == 2**31 - 1
        with pytest.raises(ValueError, match="must be at most 2147483647"):
            _resolved(minPartitions=str(2**31))

    def test_max_records_per_partition_is_bounded_by_int64(self):
        # Spark parses this one with `String.toLong`.
        assert _resolved(maxRecordsPerPartition=str(2**63 - 1))["max_records_per_partition"] == 2**63 - 1
        with pytest.raises(ValueError, match="must be at most 9223372036854775807"):
            _resolved(maxRecordsPerPartition=str(2**63))


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


# ---------------------------------------------------------------------------
# minPartitions / maxRecordsPerPartition splitting
# ---------------------------------------------------------------------------


class TestDivideRange:
    def test_single_part(self):
        assert _divide_range(0, 10, 1) == [(0, 10)]

    def test_even_split(self):
        assert _divide_range(0, 10, 2) == [(0, 5), (5, 10)]

    def test_remainder_goes_to_latest_chunks(self):
        # Spark sizes each chunk against what is left (`remaining / (parts - i)`),
        # so ten records over three chunks are 3/3/4 rather than 4/3/3.
        assert _divide_range(0, 10, 3) == [(0, 3), (3, 6), (6, 10)]
        assert _divide_range(0, 10, 4) == [(0, 2), (2, 4), (4, 7), (7, 10)]

    def test_more_parts_than_records(self):
        assert _divide_range(0, 2, 5) == [(0, 1), (1, 2)]

    def test_large_partition_hint_skips_empty_iterations(self, monkeypatch):
        def bounded_range(parts):
            assert parts == 1, "the splitter must not iterate billions of empty chunks"
            return range(parts)

        monkeypatch.setattr("pysail.spark.datasource.kafka.range", bounded_range, raising=False)
        assert _divide_range(9, 10, 2**31 - 1) == [(9, 10)]

    def test_chunks_are_contiguous_and_complete(self):
        chunks = _divide_range(100, 137, 4)
        assert chunks[0][0] == 100  # noqa: PLR2004
        assert chunks[-1][1] == 137  # noqa: PLR2004
        assert all(a[1] == b[0] for a, b in itertools.pairwise(chunks))


class TestSplitOffsetRanges:
    def test_no_options_is_identity(self):
        ranges = [("t", 0, 0, 100)]
        assert _split_offset_ranges(ranges, min_partitions=None, max_records_per_partition=None) == ranges

    def test_max_records_caps_each_range(self):
        out = _split_offset_ranges([("t", 0, 0, 10)], min_partitions=None, max_records_per_partition=4)
        assert out == [("t", 0, 0, 3), ("t", 0, 3, 6), ("t", 0, 6, 10)]
        assert all(end - start <= 4 for _, _, start, end in out)  # noqa: PLR2004

    def test_min_partitions_below_count_is_identity(self):
        ranges = [("t", 0, 0, 10), ("t", 1, 0, 10)]
        assert _split_offset_ranges(ranges, min_partitions=2, max_records_per_partition=None) == ranges

    def test_min_partitions_splits_proportionally(self):
        # One partition, 100 records, minPartitions=4 -> four equal chunks.
        out = _split_offset_ranges([("t", 0, 0, 100)], min_partitions=4, max_records_per_partition=None)
        assert out == [("t", 0, 0, 25), ("t", 0, 25, 50), ("t", 0, 50, 75), ("t", 0, 75, 100)]

    def test_min_partitions_weights_by_size(self):
        # 90 vs 10 records with minPartitions=10 -> 9 chunks and 1 chunk.
        out = _split_offset_ranges(
            [("t", 0, 0, 90), ("t", 1, 0, 10)], min_partitions=10, max_records_per_partition=None
        )
        assert len([r for r in out if r[1] == 0]) == 9  # noqa: PLR2004
        assert len([r for r in out if r[1] == 1]) == 1

    def test_both_options_apply_in_order(self):
        out = _split_offset_ranges([("t", 0, 0, 100)], min_partitions=8, max_records_per_partition=50)
        # maxRecords gives 2 ranges, then minPartitions splits those up to 8.
        assert len(out) == 8  # noqa: PLR2004
        assert out[0][2] == 0
        assert out[-1][3] == 100  # noqa: PLR2004

    def test_records_are_never_lost(self):
        out = _split_offset_ranges([("t", 0, 5, 47)], min_partitions=7, max_records_per_partition=9)
        assert sum(end - start for _, _, start, end in out) == 42  # noqa: PLR2004
        assert out[0][2] == 5  # noqa: PLR2004
        assert out[-1][3] == 47  # noqa: PLR2004

    def test_small_ranges_do_not_inflate_the_partition_count(self):
        # Spark sets aside ranges too small to split before dividing the
        # `minPartitions` budget, so the big range takes only the budget the
        # small ones left behind. A single proportional pass would give the big
        # range 4 chunks and still add one per small range, overshooting to 6.
        out = _split_offset_ranges(
            [("t", 0, 0, 100), ("t", 1, 0, 1), ("t", 2, 0, 1)],
            min_partitions=4,
            max_records_per_partition=None,
        )
        assert out == [("t", 0, 0, 50), ("t", 0, 50, 100), ("t", 1, 0, 1), ("t", 2, 0, 1)]

    def test_skewed_split_matches_spark_budget(self):
        out = _split_offset_ranges(
            [("t", 0, 0, 1000), ("t", 1, 0, 5)], min_partitions=8, max_records_per_partition=None
        )
        assert len(out) == 8  # noqa: PLR2004
        assert [r for r in out if r[1] == 1] == [("t", 1, 0, 5)]
        assert out[0][2] == 0
        assert out[-2][3] == 1000  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestReaderCompatibility:
    def test_noop_filter_handling_uses_the_available_pyspark_api(self):
        reader = KafkaDataSource(options={"kafka.bootstrap.servers": "unused", "subscribe": "t"}).reader(None)
        # Upstream detects the missing method on 4.0 before converting Filter
        # objects (which are also unavailable there). On 4.1+, the base method
        # rejects all filters, so no override is needed on either version.
        assert hasattr(reader, "pushFilters") == hasattr(DataSourceReader, "pushFilters")
        if hasattr(reader, "pushFilters"):
            filters = [object()]
            assert reader.pushFilters(filters) is filters


class TestSchema:
    def test_reader_rejects_a_wrong_column_type(self):
        import pyarrow as pa

        source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
        wrong = pa.schema([pa.field(f.name, pa.string() if f.name == "value" else f.type) for f in source.schema()])
        with pytest.raises(ValueError, match="fixed schema"):
            source.reader(wrong)

    def test_reader_rejects_a_narrowed_nullability(self):
        # Spark checks a user-supplied schema with
        # `equalsIgnoreCompatibleNullability`, under which narrowing a nullable
        # column to non-nullable is incompatible. It matters most for `key` and
        # `value`, which Kafka really does emit as null.
        import pyarrow as pa

        source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
        tightened = pa.schema([pa.field(f.name, f.type, nullable=False) for f in source.schema()])
        with pytest.raises(ValueError, match="fixed schema"):
            source.reader(tightened)

    def test_reader_ignores_schema_metadata(self):
        # Metadata round-trips through Sail and cannot change what `read()`
        # produces, so it must not be grounds for rejecting our own schema.
        source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
        with_metadata = source.schema().with_metadata({b"sail": b"1"})
        assert source.reader(with_metadata) is not None


@pytest.fixture(scope="module", autouse=True)
def spark_doctest():
    """Override the parent fixture: these unit tests need no Spark server."""


@pytest.mark.parametrize("servers", ["", " ", "\t\n"])
def test_blank_bootstrap_servers_fail_before_client_creation(servers):
    with pytest.raises(ValueError, match="must not be blank"):
        _resolved(**{"kafka.bootstrap.servers": servers})


@pytest.mark.parametrize("timeout", ["0.5", "3600001", "1e20"])
def test_admin_timeout_respects_client_limits(timeout):
    with pytest.raises(ValueError, match=r"adminTimeoutMs.*between 1 and 3600000"):
        _resolved(adminTimeoutMs=timeout)


def test_admin_timeout_limit_does_not_restrict_python_deadlines():
    assert _resolved(adminTimeoutMs="3600000")["admin_timeout_s"] == pytest.approx(3600)
    assert _resolved(stallTimeoutMs="1e20")["stall_timeout_s"] == pytest.approx(1e17)


def test_consumer_tuning_does_not_warn_on_admin_handle(caplog):
    import logging

    settings = {
        "queued.max.messages.kbytes": "1024",
        "queued.min.messages": "10",
        "fetch.max.bytes": "52428800",
        "max.partition.fetch.bytes": "1048576",
        "fetch.message.max.bytes": "1048576",
        "fetch.min.bytes": "1",
        "fetch.wait.max.ms": "100",
        "fetch.error.backoff.ms": "100",
        "check.crcs": "true",
        "session.timeout.ms": "45000",
        "heartbeat.interval.ms": "3000",
        "max.poll.interval.ms": "300000",
        "group.instance.id": "test-reader",
        "auto.commit.interval.ms": "5000",
        "partition.assignment.strategy": "range",
        "enable.auto.offset.store": "false",
    }
    with caplog.at_level(logging.WARNING):
        admin = AdminClient(_build_admin_config(settings), logger=logging.getLogger(__name__))
        admin.poll(0)
    assert not any("consumer property" in record.message for record in caplog.records)
    config = _build_consumer_config(settings)
    assert all(config[key] == value for key, value in settings.items())
