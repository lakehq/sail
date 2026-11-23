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

from pysail.spark.datasource.kafka import KafkaDataSource
from pysail.spark.datasource.kafka.options import (
    AssignPartitions,
    GlobalTimestamp,
    PartitionOffsets,
    PartitionTimestamps,
    SpecialOffset,
    SubscribePattern,
    SubscribeTopics,
    TimestampStrategy,
    TopicPartitionKey,
    build_admin_config,
    build_consumer_config,
    parse_range_limit,
    parse_subscription,
    validate_offsets,
)
from pysail.spark.datasource.kafka.planning import OffsetRange, divide_range, split_offset_ranges

_TS_MS = 1700000000000
_READ_UNCOMMITTED = IsolationLevel.READ_UNCOMMITTED
_POLL_TIMEOUT_MS = 250


def _tp(partition):
    """Key for partition ``partition`` of the test topic ``t``."""
    return TopicPartitionKey("t", partition)


@pytest.fixture(scope="module", autouse=True)
def spark_doctest():
    """Override the parent fixture: these unit tests need no Spark server."""


def _resolved(**extra):
    opts = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t", **extra}
    return KafkaDataSource(options=opts).reader(None)


@pytest.mark.parametrize("option", ["startingOffsets", "startingOffsetsByTimestamp"])
@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_partition_limit_value_width(option, value):
    with pytest.raises(ValueError, match="signed 64-bit"):
        _resolved(**{option: json.dumps({"t": {"0": value}})})


@pytest.mark.parametrize("option", ["startingOffsets", "startingOffsetsByTimestamp"])
@pytest.mark.parametrize("partition", [-(2**31) - 1, 2**31])
def test_partition_limit_id_width(option, partition):
    with pytest.raises(ValueError, match="signed 32-bit"):
        _resolved(**{option: json.dumps({"t": {str(partition): 0}})})


@pytest.mark.parametrize("partition", [-(2**31) - 1, 2**31])
def test_assignment_partition_id_width(partition):
    with pytest.raises(ValueError, match="signed 32-bit"):
        parse_subscription(CaseInsensitiveDict({"assign": json.dumps({"t": [partition]})}))


# ---------------------------------------------------------------------------
# Offset options (startingOffsets / endingOffsets)
# ---------------------------------------------------------------------------


class TestParseOffsetSpec:
    def test_earliest_sentinel(self):
        assert _resolved(startingOffsets="earliest").starting is SpecialOffset.EARLIEST

    def test_latest_sentinel(self):
        assert _resolved(endingOffsets="latest").ending is SpecialOffset.LATEST

    def test_stripped_sentinel(self):
        assert _resolved(startingOffsets="  earliest  ").starting is SpecialOffset.EARLIEST

    @pytest.mark.parametrize("raw", ["EARLIEST", "Earliest", "eArLiEsT"])
    def test_sentinel_is_case_insensitive(self, raw):
        # Spark lowercases before matching, so `EARLIEST` is a valid value.
        assert _resolved(startingOffsets=raw).starting is SpecialOffset.EARLIEST

    def test_blank_is_a_parse_error(self):
        # Spark distinguishes absent from present-but-blank: a present blank
        # value is parsed as JSON and fails. Only absence means "use default".
        with pytest.raises(json.JSONDecodeError):
            _resolved(startingOffsets="")

    def test_json_object(self):
        assert _resolved(startingOffsets='{"t": {"0": 5}}').starting == PartitionOffsets({_tp(0): 5})

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _resolved(startingOffsets="not-json")

    def test_non_object_json_raises(self):
        with pytest.raises(ValueError, match="must be an object"):
            _resolved(startingOffsets="[1, 2, 3]")

    def test_non_object_topic_value_raises(self):
        with pytest.raises(ValueError, match="topic 't' must be an object"):
            _resolved(startingOffsets='{"t": 5}')

    def test_non_int_partition_value_raises(self):
        with pytest.raises(ValueError, match="partition '0' must be an integer"):
            _resolved(startingOffsets='{"t": {"0": "x"}}')

    def test_bool_partition_value_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _resolved(startingOffsets='{"t": {"0": true}}')

    def test_non_numeric_partition_key_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _resolved(startingOffsets='{"t": {"zero": 1}}')

    @pytest.mark.parametrize("key", ["1_0", " 0", "0 "])
    def test_python_only_int_partition_keys_rejected(self, key):
        with pytest.raises(ValueError, match="must be an integer"):
            _resolved(startingOffsets='{"t": {"%s": 5}}' % key)  # noqa: UP031

    @pytest.mark.parametrize("key", ["00", "+0"])
    def test_partition_key_is_canonicalized(self, key):
        # Spark reads these keys as `Map[Int, Long]`, so every spelling of 0 is
        # partition 0.
        spec = _resolved(startingOffsets='{"t": {"%s": 5}}' % key).starting  # noqa: UP031
        assert spec == PartitionOffsets({_tp(0): 5})

    def test_duplicate_partition_keys_collapse(self):
        # Two spellings of one id are one entry in the Scala `Map` too.
        limit = _resolved(startingOffsets='{"t": {"0": 5, "00": 9}}').starting
        assert limit == PartitionOffsets({_tp(0): 9})


# ---------------------------------------------------------------------------
# Timestamp option parsing
# ---------------------------------------------------------------------------


class TestParseTimestamps:
    @pytest.mark.parametrize("boundary", ["starting", "ending"])
    @pytest.mark.parametrize("raw", ["1700000000000", "  1700000000000  ", "\t1700000000000\n"])
    def test_global_int(self, boundary, raw):
        assert getattr(_resolved(**{f"{boundary}Timestamp": raw}), boundary) == GlobalTimestamp(_TS_MS)

    @pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
    def test_global_timestamp_width(self, value):
        with pytest.raises(ValueError, match="signed 64-bit"):
            _resolved(startingTimestamp=str(value))

    @pytest.mark.parametrize("value", [-(2**63), 2**63 - 1])
    def test_global_timestamp_width_boundaries(self, value):
        assert _resolved(startingTimestamp=str(value)).starting == GlobalTimestamp(value)

    def test_global_timestamp_rejects_underscores(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _resolved(endingTimestamp="1_700_000_000_000")

    def test_invalid_global_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _resolved(startingTimestamp="not-a-number")

    def test_partition_key_is_canonicalized(self):
        result = _resolved(startingOffsetsByTimestamp='{"t": {"00": 1700000000000}}').starting
        assert result == PartitionTimestamps({_tp(0): _TS_MS})

    def test_per_tp_json(self):
        result = _resolved(startingOffsetsByTimestamp='{"t": {"0": 1700000000000}}').starting
        assert result == PartitionTimestamps({_tp(0): _TS_MS})

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _resolved(startingOffsetsByTimestamp="not-json")

    def test_non_object_json_raises(self):
        with pytest.raises(ValueError, match="must be an object"):
            _resolved(startingOffsetsByTimestamp="[1, 2, 3]")

    def test_non_object_topic_value_raises(self):
        with pytest.raises(ValueError, match="topic 't' must be an object"):
            _resolved(startingOffsetsByTimestamp='{"t": 5}')

    def test_non_int_partition_value_raises(self):
        with pytest.raises(ValueError, match="partition '0' must be an integer"):
            _resolved(endingOffsetsByTimestamp='{"t": {"0": "x"}}')


# ---------------------------------------------------------------------------
# parse_range_limit — Spark's precedence between the range options
# ---------------------------------------------------------------------------


class TestRangeOptionPrecedence:
    def test_absent_returns_default_sentinel(self):
        assert parse_range_limit(CaseInsensitiveDict({}), boundary="starting") is SpecialOffset.EARLIEST
        assert parse_range_limit(CaseInsensitiveDict({}), boundary="ending") is SpecialOffset.LATEST

    def test_offsets_only(self):
        spec = parse_range_limit(CaseInsensitiveDict({"startingOffsets": "earliest"}), boundary="starting")
        assert spec is SpecialOffset.EARLIEST

    def test_global_timestamp_beats_everything(self):
        # Spark checks the global timestamp first and never looks at the rest,
        # so the lower-priority values are not even parsed.
        spec = parse_range_limit(
            CaseInsensitiveDict(
                {
                    "startingTimestamp": "1700000000000",
                    "startingOffsetsByTimestamp": "not-even-json",
                    "startingOffsets": "not-even-json",
                }
            ),
            boundary="starting",
        )
        assert spec == GlobalTimestamp(_TS_MS)

    def test_per_partition_timestamp_beats_offsets(self):
        spec = parse_range_limit(
            CaseInsensitiveDict(
                {
                    "endingOffsetsByTimestamp": '{"t": {"0": 1700000000000}}',
                    "endingOffsets": "not-even-json",
                }
            ),
            boundary="ending",
        )
        assert spec == PartitionTimestamps({_tp(0): _TS_MS})

    def test_combination_is_not_rejected(self):
        # Spark applies precedence rather than failing, so a job that supplies
        # a fallback keeps working.
        resolved = _resolved(startingOffsets="earliest", startingTimestamp="1700000000000")
        assert resolved.starting == GlobalTimestamp(_TS_MS)

    def test_blank_lower_priority_option_is_ignored(self):
        resolved = _resolved(endingOffsets="", endingTimestamp="1700000000000")
        assert resolved.ending == GlobalTimestamp(_TS_MS)

    def test_blank_winning_option_raises(self):
        with pytest.raises(json.JSONDecodeError):
            _resolved(startingOffsets="")


# ---------------------------------------------------------------------------
# startingOffsetsByTimestampStrategy
# ---------------------------------------------------------------------------


class TestTimestampStrategy:
    def test_default_is_error(self):
        assert _resolved().starting_ts_strategy is TimestampStrategy.ERROR

    @pytest.mark.parametrize("raw", ["latest", "LATEST", "Latest"])
    def test_latest_accepted(self, raw):
        assert _resolved(startingOffsetsByTimestampStrategy=raw).starting_ts_strategy is TimestampStrategy.LATEST

    @pytest.mark.parametrize("raw", ["earliest", " latest", "latest ", " error ", "\tLATEST\n"])
    def test_invalid_raises(self, raw):
        with pytest.raises(ValueError, match="startingOffsetsByTimestampStrategy"):
            _resolved(startingOffsetsByTimestampStrategy=raw)


# ---------------------------------------------------------------------------
# Protected consumer properties
# ---------------------------------------------------------------------------


class TestProtectedKafkaConfigs:
    @pytest.mark.parametrize("name", ["auto.offset.reset", "enable.auto.commit", "enable.partition.eof"])
    def test_rejected(self, name):
        with pytest.raises(ValueError, match=f"kafka.{name}' is not supported".replace(".", r"\.")):
            _resolved(**{f"kafka.{name}": "whatever"})

    def test_auto_offset_reset_names_the_alternative(self):
        with pytest.raises(ValueError, match="startingOffsets"):
            _resolved(**{"kafka.auto.offset.reset": "earliest"})

    def test_group_id_still_allowed(self):
        assert _resolved(**{"kafka.group.id": "mine"}).client_config["group.id"] == "mine"


# ---------------------------------------------------------------------------
# _build_consumer_config
# ---------------------------------------------------------------------------


class TestBuildConsumerConfig:
    def test_defaults_applied(self):
        config = build_consumer_config({"bootstrap.servers": "localhost:9092"}, _READ_UNCOMMITTED)
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == "sail-kafka-reader"
        assert config["enable.auto.commit"] is False
        assert config["auto.offset.reset"] == "error"
        assert config["enable.partition.eof"] is True

    def test_user_group_id_overrides_default(self):
        settings = {"bootstrap.servers": "localhost:9092", "group.id": "my-group"}
        config = build_consumer_config(settings, _READ_UNCOMMITTED)
        assert config["group.id"] == "my-group"

    def test_source_owned_settings_win(self):
        # Option validation rejects these before we get here; the merge order is
        # the second line of defence, and Spark likewise keeps its own consumer
        # settings authoritative.
        config = build_consumer_config(
            {"enable.auto.commit": "true", "auto.offset.reset": "earliest", "enable.partition.eof": "false"},
            _READ_UNCOMMITTED,
        )
        assert config["enable.auto.commit"] is False
        assert config["auto.offset.reset"] == "error"
        assert config["enable.partition.eof"] is True

    @pytest.mark.parametrize(
        ("level", "expected"),
        [(IsolationLevel.READ_UNCOMMITTED, "read_uncommitted"), (IsolationLevel.READ_COMMITTED, "read_committed")],
    )
    def test_isolation_level_uses_the_librdkafka_spelling(self, level, expected):
        # librdkafka only accepts the lowercase values.
        assert build_consumer_config({}, level)["isolation.level"] == expected


class TestIsolationLevel:
    def test_defaults_to_read_uncommitted(self):
        # librdkafka defaults to `read_committed`; Spark never sets the property
        # and so inherits the Java client's `read_uncommitted`. Left to the
        # librdkafka default, a transactional topic would silently return a
        # different row set here than under Spark.
        assert _resolved().isolation_level == IsolationLevel.READ_UNCOMMITTED

    def test_follows_the_user_setting_in_any_case(self):
        # Spark derives `ListOffsetsOptions` from the driver Kafka params, so a
        # user asking for `read_committed` gets last-stable-offset semantics
        # when a "latest" boundary or a timestamp is resolved.
        assert _resolved(**{"kafka.isolation.level": "READ_COMMITTED"}).isolation_level == (
            IsolationLevel.READ_COMMITTED
        )

    def test_is_not_left_in_the_client_config(self):
        # Planning and the consumers both take it from `isolation_level`, so
        # a raw spelling cannot reach librdkafka.
        assert "isolation.level" not in _resolved(**{"kafka.isolation.level": "read_committed"}).client_config

    @pytest.mark.parametrize("raw", ["read_everything", " READ_COMMITTED ", "read_uncommitted ", "\tread_committed\n"])
    def test_invalid_value_fails_option_validation(self, raw):
        with pytest.raises(ValueError, match="must be 'read_uncommitted' or 'read_committed'"):
            _resolved(**{"kafka.isolation.level": raw})


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
        config = _resolved(**{f"KAFKA.{key.upper()}": value for key, value in settings.items()}).client_config
        assert build_admin_config(config) == settings
        assert all(build_consumer_config(config, _READ_UNCOMMITTED)[key] == value for key, value in settings.items())

    @pytest.mark.parametrize(
        ("client", "build_config"),
        [
            (AdminClient, build_admin_config),
            (Consumer, lambda config: build_consumer_config(config, _READ_UNCOMMITTED)),
        ],
    )
    def test_invalid_security_protocol_fails_without_plaintext_fallback(self, client, build_config):
        config = _resolved(**{"kafka.security.protocol": "not-a-protocol"}).client_config
        # Real librdkafka validation fails before it can open a connection.
        with pytest.raises(KafkaException, match=r"security\.protocol"):
            client(build_config(config))

    def test_consumer_only_properties_are_stripped(self):
        # librdkafka builds the admin handle on a producer and logs a CONFWARN
        # for every consumer property it finds there, so a user-supplied
        # `kafka.group.id` would warn on every planning call.
        config = build_admin_config({"bootstrap.servers": "localhost:9092", "group.id": "my-group"})
        assert config == {"bootstrap.servers": "localhost:9092"}


# ---------------------------------------------------------------------------
# Subscription strategies — assign / subscribe / subscribePattern
# ---------------------------------------------------------------------------


class TestSubscription:
    def test_subscribe_topic_list(self):
        assert parse_subscription(CaseInsensitiveDict({"subscribe": "a, b ,c"})) == SubscribeTopics(["a", "b", "c"])

    def test_subscribe_dedupes(self):
        # A repeated topic would otherwise be enumerated once per occurrence,
        # producing duplicate input partitions and emitting every row twice.
        assert parse_subscription(CaseInsensitiveDict({"subscribe": "t,t, t"})) == SubscribeTopics(["t"])

    def test_subscribe_topics_are_sorted(self):
        assert parse_subscription(CaseInsensitiveDict({"subscribe": "b,a,b,c"})) == SubscribeTopics(["a", "b", "c"])

    def test_subscribe_empty_raises(self):
        with pytest.raises(ValueError, match="at least one topic"):
            parse_subscription(CaseInsensitiveDict({"subscribe": " , "}))

    @pytest.mark.parametrize("raw", ["orders-.*", "  orders-.*  "])
    def test_subscribe_pattern(self, raw):
        sub = parse_subscription(CaseInsensitiveDict({"subscribePattern": raw}))
        assert isinstance(sub, SubscribePattern)
        assert sub.pattern.fullmatch("orders-eu")
        assert not sub.pattern.fullmatch("shipments")

    def test_subscribe_pattern_invalid_regex_raises(self):
        with pytest.raises(ValueError, match="not a valid regular expression"):
            parse_subscription(CaseInsensitiveDict({"subscribePattern": "orders-["}))

    def test_assign(self):
        sub = parse_subscription(CaseInsensitiveDict({"assign": '{"t": [0, 2]}'}))
        assert sub == AssignPartitions(frozenset({_tp(0), _tp(2)}))

    def test_assign_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            parse_subscription(CaseInsensitiveDict({"assign": "not-json"}))

    def test_assign_empty_object(self):
        # `JsonUtils.partitions` accepts an empty map; the assignment simply
        # selects nothing.
        assert parse_subscription(CaseInsensitiveDict({"assign": "{}"})) == AssignPartitions(frozenset())

    def test_assign_empty_partition_list(self):
        assert parse_subscription(CaseInsensitiveDict({"assign": '{"t": []}'})) == AssignPartitions(frozenset())

    def test_assign_negative_partition(self):
        # Spark deserializes `Seq[Int]` and filters against the broker's
        # partitions later, so an id that cannot exist is skipped, not rejected.
        assert parse_subscription(CaseInsensitiveDict({"assign": '{"t": [-1]}'})) == AssignPartitions(
            frozenset({_tp(-1)})
        )

    def test_assign_bool_partition_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            parse_subscription(CaseInsensitiveDict({"assign": '{"t": [true]}'}))

    def test_none_specified_raises(self):
        with pytest.raises(ValueError, match="must be specified"):
            parse_subscription(CaseInsensitiveDict({"kafka.bootstrap.servers": "localhost:9092"}))

    def test_more_than_one_raises(self):
        with pytest.raises(ValueError, match="Only one of the following options"):
            parse_subscription(CaseInsensitiveDict({"subscribe": "t", "assign": '{"t": [0]}'}))


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
        resolved = source.reader(None)
        assert resolved.client_config["bootstrap.servers"] == "localhost:9092"
        assert resolved.subscription == SubscribeTopics(["t"])
        assert resolved.starting is SpecialOffset.EARLIEST
        assert resolved.include_headers is True

    def test_lower_case_option_keys(self):
        resolved = _resolved(startingoffsets="earliest", maxbatchrows="99")
        assert resolved.starting is SpecialOffset.EARLIEST
        assert resolved.max_batch_rows == 99  # noqa: PLR2004

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
        resolved = source.reader(None)
        assert resolved.starting is SpecialOffset.EARLIEST
        assert resolved.max_batch_rows == 77  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Numeric options
# ---------------------------------------------------------------------------


class TestBooleanOptions:
    def test_defaults(self):
        resolved = _resolved()
        assert resolved.include_headers is False
        assert resolved.fail_on_data_loss is True

    @pytest.mark.parametrize(("raw", "expected"), [("true", True), ("TRUE", True), ("False", False)])
    def test_case_insensitive_values(self, raw, expected):
        assert _resolved(includeHeaders=raw).include_headers is expected
        assert _resolved(failOnDataLoss=raw).fail_on_data_loss is expected

    @pytest.mark.parametrize("option", ["includeHeaders", "failOnDataLoss"])
    @pytest.mark.parametrize("raw", ["", "yes", "0", "1", "maybe", " true", "false ", "\tTRUE\n"])
    def test_invalid_values_rejected(self, option, raw):
        with pytest.raises(ValueError, match=rf"{option}.*must be 'true' or 'false'"):
            _resolved(**{option: raw})


class TestNumericOptions:
    @pytest.mark.parametrize("option", ["maxBatchRows", "maxBatchBytes"])
    @pytest.mark.parametrize("value", ["", "0", "-1", "nan", "inf", "1_000", " 7 "])
    def test_batch_resource_limits_reject_invalid_values(self, option, value):
        with pytest.raises(ValueError, match=rf"{option}"):
            _resolved(**{option: value})

    @pytest.mark.parametrize("raw", ["1_000", " 100 "])
    def test_poll_timeout_rejects_underscores_and_whitespace(self, raw):
        with pytest.raises(ValueError, match=r"pollTimeoutMs"):
            _resolved(pollTimeoutMs=raw)

    def test_poll_timeout_default_matches_spark(self):
        # Spark's batch default for kafkaConsumer.pollTimeoutMs is
        # spark.network.timeout, i.e. 120s.
        assert _resolved().poll_timeout_s == 120.0  # noqa: PLR2004

    def test_spark_poll_timeout_option_name(self):
        assert _resolved(**{"kafkaConsumer.pollTimeoutMs": str(_POLL_TIMEOUT_MS)}).poll_timeout_s == 0.25  # noqa: PLR2004

    def test_short_poll_timeout_alias(self):
        assert _resolved(pollTimeoutMs=str(_POLL_TIMEOUT_MS)).poll_timeout_s == 0.25  # noqa: PLR2004

    def test_spark_name_wins_over_alias(self):
        resolved = _resolved(**{"kafkaConsumer.pollTimeoutMs": "500", "pollTimeoutMs": "invalid"})
        assert resolved.poll_timeout_s == 0.5  # noqa: PLR2004

    @pytest.mark.parametrize("raw", ["250.5", "2.5e2", "1e3", "Infinity", "inf", "nan"])
    def test_poll_timeout_must_be_whole_milliseconds(self, raw):
        # Spark reads kafkaConsumer.pollTimeoutMs with `toLong`, which rejects
        # fractions, exponents, and non-finite values.
        with pytest.raises(ValueError, match=r"pollTimeoutMs.*must be an integer"):
            _resolved(pollTimeoutMs=raw)

    @pytest.mark.parametrize("raw", ["0", "-1"])
    def test_non_positive_poll_timeout_rejected(self, raw):
        with pytest.raises(ValueError, match=r"pollTimeoutMs.*positive"):
            _resolved(pollTimeoutMs=raw)

    def test_min_partitions_absent(self):
        assert _resolved().min_partitions is None
        assert _resolved().max_records_per_partition is None

    def test_min_partitions_parsed(self):
        assert _resolved(minPartitions="4").min_partitions == 4  # noqa: PLR2004
        assert _resolved(maxRecordsPerPartition="100").max_records_per_partition == 100  # noqa: PLR2004

    def test_min_partitions_must_be_positive(self):
        with pytest.raises(ValueError, match="must be positive"):
            _resolved(minPartitions="0")

    def test_min_partitions_is_bounded_by_int32(self):
        # Spark parses this with `String.toInt`, which rejects anything wider.
        # Python integers are unbounded, so without the bound the value goes
        # straight into the splitter as a chunk count.
        assert _resolved(minPartitions=str(2**31 - 1)).min_partitions == 2**31 - 1
        with pytest.raises(ValueError, match="must be at most 2147483647"):
            _resolved(minPartitions=str(2**31))

    def test_max_records_per_partition_is_bounded_by_int64(self):
        # Spark parses this one with `String.toLong`.
        assert _resolved(maxRecordsPerPartition=str(2**63 - 1)).max_records_per_partition == 2**63 - 1
        with pytest.raises(ValueError, match="must be at most 9223372036854775807"):
            _resolved(maxRecordsPerPartition=str(2**63))


# ---------------------------------------------------------------------------
# validate_offsets — Spark batch-mode sentinel rules
# ---------------------------------------------------------------------------


class TestValidateOffsets:
    def test_allowed_special_offsets(self):
        validate_offsets(SpecialOffset.EARLIEST, boundary="starting")
        validate_offsets(SpecialOffset.LATEST, boundary="ending")

    def test_latest_rejected_for_starting(self):
        with pytest.raises(ValueError, match="'startingOffsets' does not support 'latest'"):
            validate_offsets(SpecialOffset.LATEST, boundary="starting")

    def test_earliest_rejected_for_ending(self):
        with pytest.raises(ValueError, match="'endingOffsets' does not support 'earliest'"):
            validate_offsets(SpecialOffset.EARLIEST, boundary="ending")

    def test_allowed_json_sentinels(self):
        validate_offsets({_tp(0): -2}, boundary="starting")
        validate_offsets({_tp(0): -1}, boundary="ending")

    def test_json_latest_rejected_for_starting(self):
        with pytest.raises(ValueError, match="'startingOffsets' does not support offset -1"):
            validate_offsets({_tp(0): -1}, boundary="starting")

    def test_json_earliest_rejected_for_ending(self):
        with pytest.raises(ValueError, match="'endingOffsets' does not support offset -2"):
            validate_offsets({_tp(0): -2}, boundary="ending")

    def test_concrete_offsets_allowed(self):
        validate_offsets({_tp(0): 0, _tp(1): 12345}, boundary="starting")
        validate_offsets({_tp(0): 0, _tp(1): 12345}, boundary="ending")

    @pytest.mark.parametrize("boundary", ["starting", "ending"])
    def test_offset_below_negative_two_rejected(self, boundary):
        with pytest.raises(ValueError, match=rf"'{boundary}Offsets'.*must be >= 0"):
            validate_offsets({_tp(0): -3}, boundary=boundary)

    @pytest.mark.parametrize("offset", [-1000, -1001])
    def test_librdkafka_sentinels_rejected(self, offset):
        # -1000 is OFFSET_STORED and -1001 is OFFSET_INVALID. Passed through,
        # OFFSET_STORED would silently read from committed group offsets.
        with pytest.raises(ValueError, match=r"'startingOffsets'.*must be >= 0"):
            validate_offsets({_tp(0): offset}, boundary="starting")

    def test_error_names_the_offending_partition(self):
        with pytest.raises(ValueError, match=r"topic 't' partition 3"):
            validate_offsets({_tp(0): 5, _tp(3): -7}, boundary="starting")


# ---------------------------------------------------------------------------
# minPartitions / maxRecordsPerPartition splitting
# ---------------------------------------------------------------------------


class TestDivideRange:
    def test_single_part(self):
        assert divide_range(OffsetRange(_tp(0), 0, 10), 1) == [OffsetRange(_tp(0), 0, 10)]

    def test_even_split(self):
        assert divide_range(OffsetRange(_tp(0), 0, 10), 2) == [OffsetRange(_tp(0), 0, 5), OffsetRange(_tp(0), 5, 10)]

    def test_remainder_goes_to_latest_chunks(self):
        # Spark sizes each chunk against what is left (`remaining / (parts - i)`),
        # so ten records over three chunks are 3/3/4 rather than 4/3/3.
        assert divide_range(OffsetRange(_tp(0), 0, 10), 3) == [
            OffsetRange(_tp(0), 0, 3),
            OffsetRange(_tp(0), 3, 6),
            OffsetRange(_tp(0), 6, 10),
        ]
        assert divide_range(OffsetRange(_tp(0), 0, 10), 4) == [
            OffsetRange(_tp(0), 0, 2),
            OffsetRange(_tp(0), 2, 4),
            OffsetRange(_tp(0), 4, 7),
            OffsetRange(_tp(0), 7, 10),
        ]

    def test_more_parts_than_records(self):
        assert divide_range(OffsetRange(_tp(0), 0, 2), 5) == [OffsetRange(_tp(0), 0, 1), OffsetRange(_tp(0), 1, 2)]

    def test_large_partition_hint_skips_empty_iterations(self, monkeypatch):
        def bounded_range(parts):
            assert parts == 1, "the splitter must not iterate billions of empty chunks"
            return range(parts)

        monkeypatch.setattr("pysail.spark.datasource.kafka.planning.range", bounded_range, raising=False)
        assert divide_range(OffsetRange(_tp(0), 9, 10), 2**31 - 1) == [OffsetRange(_tp(0), 9, 10)]

    def test_chunks_are_contiguous_and_complete(self):
        chunks = divide_range(OffsetRange(_tp(0), 100, 137), 4)
        assert chunks[0].start == 100  # noqa: PLR2004
        assert chunks[-1].end == 137  # noqa: PLR2004
        assert all(left.end == right.start for left, right in itertools.pairwise(chunks))


class TestSplitOffsetRanges:
    def test_no_options_is_identity(self):
        ranges = [OffsetRange(_tp(0), 0, 100)]
        assert split_offset_ranges(ranges, min_partitions=None, max_records_per_partition=None) == ranges

    def test_max_records_caps_each_range(self):
        out = split_offset_ranges([OffsetRange(_tp(0), 0, 10)], min_partitions=None, max_records_per_partition=4)
        assert out == [OffsetRange(_tp(0), 0, 3), OffsetRange(_tp(0), 3, 6), OffsetRange(_tp(0), 6, 10)]
        assert all(r.size <= 4 for r in out)  # noqa: PLR2004

    def test_min_partitions_below_count_is_identity(self):
        ranges = [OffsetRange(_tp(0), 0, 10), OffsetRange(_tp(1), 0, 10)]
        assert split_offset_ranges(ranges, min_partitions=2, max_records_per_partition=None) == ranges

    def test_min_partitions_splits_proportionally(self):
        # One partition, 100 records, minPartitions=4 -> four equal chunks.
        out = split_offset_ranges([OffsetRange(_tp(0), 0, 100)], min_partitions=4, max_records_per_partition=None)
        assert out == [
            OffsetRange(_tp(0), 0, 25),
            OffsetRange(_tp(0), 25, 50),
            OffsetRange(_tp(0), 50, 75),
            OffsetRange(_tp(0), 75, 100),
        ]

    def test_min_partitions_weights_by_size(self):
        # 90 vs 10 records with minPartitions=10 -> 9 chunks and 1 chunk.
        out = split_offset_ranges(
            [OffsetRange(_tp(0), 0, 90), OffsetRange(_tp(1), 0, 10)], min_partitions=10, max_records_per_partition=None
        )
        assert len([r for r in out if r.topic_partition.partition == 0]) == 9  # noqa: PLR2004
        assert len([r for r in out if r.topic_partition.partition == 1]) == 1

    def test_both_options_apply_in_order(self):
        out = split_offset_ranges([OffsetRange(_tp(0), 0, 100)], min_partitions=8, max_records_per_partition=50)
        # maxRecords gives 2 ranges, then minPartitions splits those up to 8.
        assert len(out) == 8  # noqa: PLR2004
        assert out[0].start == 0
        assert out[-1].end == 100  # noqa: PLR2004

    def test_records_are_never_lost(self):
        out = split_offset_ranges([OffsetRange(_tp(0), 5, 47)], min_partitions=7, max_records_per_partition=9)
        assert sum(r.size for r in out) == 42  # noqa: PLR2004
        assert out[0].start == 5  # noqa: PLR2004
        assert out[-1].end == 47  # noqa: PLR2004

    def test_small_ranges_do_not_inflate_the_partition_count(self):
        # Spark sets aside ranges too small to split before dividing the
        # `minPartitions` budget, so the big range takes only the budget the
        # small ones left behind. A single proportional pass would give the big
        # range 4 chunks and still add one per small range, overshooting to 6.
        out = split_offset_ranges(
            [OffsetRange(_tp(0), 0, 100), OffsetRange(_tp(1), 0, 1), OffsetRange(_tp(2), 0, 1)],
            min_partitions=4,
            max_records_per_partition=None,
        )
        assert out == [
            OffsetRange(_tp(0), 0, 50),
            OffsetRange(_tp(0), 50, 100),
            OffsetRange(_tp(1), 0, 1),
            OffsetRange(_tp(2), 0, 1),
        ]

    def test_skewed_split_matches_spark_budget(self):
        out = split_offset_ranges(
            [OffsetRange(_tp(0), 0, 1000), OffsetRange(_tp(1), 0, 5)], min_partitions=8, max_records_per_partition=None
        )
        assert len(out) == 8  # noqa: PLR2004
        assert [r for r in out if r.topic_partition.partition == 1] == [OffsetRange(_tp(1), 0, 5)]
        assert out[0].start == 0
        assert out[-2].end == 1000  # noqa: PLR2004


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


def test_missing_bootstrap_servers_fail_before_client_creation():
    with pytest.raises(ValueError, match=r"bootstrap\.servers"):
        KafkaDataSource(options={"subscribe": "t"}).reader(None)


@pytest.mark.parametrize("servers", ["", " ", "\t\n"])
def test_blank_bootstrap_servers_fail_before_client_creation(servers):
    with pytest.raises(ValueError, match="must not be blank"):
        _resolved(**{"kafka.bootstrap.servers": servers})


def test_admin_timeout_respects_client_limits():
    # librdkafka admin request timeouts accept 1 ms to 1 hour.
    with pytest.raises(ValueError, match=r"adminTimeoutMs.*must be at most 3600000"):
        _resolved(adminTimeoutMs="3600001")


@pytest.mark.parametrize("timeout", ["0.5", "1e3"])
def test_admin_timeout_must_be_whole_milliseconds(timeout):
    with pytest.raises(ValueError, match=r"adminTimeoutMs.*must be an integer"):
        _resolved(adminTimeoutMs=timeout)


def test_admin_timeout_limit_does_not_restrict_poll_timeout():
    assert _resolved(adminTimeoutMs="3600000").admin_timeout_s == pytest.approx(3600)
    assert _resolved(pollTimeoutMs="3600001").poll_timeout_s == pytest.approx(3600.001)


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
        admin = AdminClient(build_admin_config(settings), logger=logging.getLogger(__name__))
        admin.poll(0)
    assert not any("consumer property" in record.message for record in caplog.records)
    config = build_consumer_config(settings, _READ_UNCOMMITTED)
    assert all(config[key] == value for key, value in settings.items())
