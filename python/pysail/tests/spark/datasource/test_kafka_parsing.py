"""Unit tests for pure option-parsing helpers in the Kafka data source.

These don't touch Kafka or Docker — they cover the parsing/validation surface
that would otherwise only be exercised through the integration tests.
"""

from __future__ import annotations

import itertools

import pytest

from pysail.testing.spark.utils.common import pyspark_version

if pyspark_version() < (4, 1):
    pytest.skip("Python data source requires Spark 4.1+", allow_module_level=True)

pytest.importorskip("confluent_kafka")

from pyspark.sql.datasource import CaseInsensitiveDict

from pysail.spark.datasource.kafka import (
    KafkaDataSource,
    _build_admin_config,
    _build_consumer_config,
    _build_schema,
    _divide_range,
    _extract_kafka_config,
    _parse_bool,
    _parse_global_timestamp,
    _parse_offset_spec,
    _parse_subscription,
    _parse_timestamp_json,
    _resolve_range_option,
    _split_offset_ranges,
    _validate_kafka_config,
    _validate_offset_spec,
    _validate_spec_partitions,
)

_TS_MS = 1700000000000
_POLL_TIMEOUT_MS = 250


class _TP:
    """Stand-in for confluent_kafka.TopicPartition (constructor needs a client)."""

    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition


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


# ---------------------------------------------------------------------------
# Timestamp option parsing
# ---------------------------------------------------------------------------


class TestParseTimestamps:
    def test_global_int(self):
        assert _parse_global_timestamp("1700000000000", option_name="startingTimestamp") == _TS_MS

    def test_global_int_stripped(self):
        assert _parse_global_timestamp("  1700000000000  ", option_name="endingTimestamp") == _TS_MS

    def test_invalid_global_raises(self):
        with pytest.raises(ValueError, match="must be an integer"):
            _parse_global_timestamp("not-a-number", option_name="startingTimestamp")

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
# _parse_bool
# ---------------------------------------------------------------------------


class TestParseBool:
    def test_absent_uses_default(self):
        assert _parse_bool(CaseInsensitiveDict({}), "includeHeaders", default=False) is False
        assert _parse_bool(CaseInsensitiveDict({}), "failOnDataLoss", default=True) is True

    @pytest.mark.parametrize(("raw", "want"), [("true", True), ("TRUE", True), ("false", False), (" False ", False)])
    def test_case_insensitive(self, raw, want):
        assert _parse_bool(CaseInsensitiveDict({"includeHeaders": raw}), "includeHeaders", default=False) is want

    @pytest.mark.parametrize("raw", ["yes", "1", "0", "", "no"])
    def test_invalid_raises(self, raw):
        # Spark calls Scala's `toBoolean`, which throws on anything else;
        # treating them as false would silently drop the headers column.
        with pytest.raises(ValueError, match="must be 'true' or 'false'"):
            _parse_bool(CaseInsensitiveDict({"includeHeaders": raw}), "includeHeaders", default=False)


# ---------------------------------------------------------------------------
# _extract_kafka_config / protected consumer properties
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

    def test_fail_on_data_loss_false_skips_ahead(self):
        # Spark's failOnDataLoss=false skips to what is still in the log rather
        # than failing on an aged-out start offset.
        config = _build_consumer_config({}, fail_on_data_loss=False)
        assert config["auto.offset.reset"] == "earliest"

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


class TestBuildAdminConfig:
    def test_connection_settings_pass_through(self):
        config = _build_admin_config({"bootstrap.servers": "localhost:9092", "security.protocol": "SASL_SSL"})
        assert config == {"bootstrap.servers": "localhost:9092", "security.protocol": "SASL_SSL"}

    def test_consumer_only_properties_are_stripped(self):
        # librdkafka builds the admin handle on a producer and logs a CONFWARN
        # for every consumer property it finds there, so a user-supplied
        # `kafka.group.id` would warn on every planning call.
        config = _build_admin_config({"bootstrap.servers": "localhost:9092", "group.id": "my-group"})
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

    def test_assign_empty_partition_list_raises(self):
        with pytest.raises(ValueError, match="non-empty array"):
            _parse_subscription(CaseInsensitiveDict({"assign": '{"t": []}'}))

    def test_assign_negative_partition_raises(self):
        with pytest.raises(ValueError, match="non-negative integer"):
            _parse_subscription(CaseInsensitiveDict({"assign": '{"t": [-1]}'}))

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


# ---------------------------------------------------------------------------
# failOnDataLoss
# ---------------------------------------------------------------------------


class TestFailOnDataLoss:
    def test_default_true(self):
        assert _resolved()["fail_on_data_loss"] is True

    def test_explicit_false(self):
        assert _resolved(failOnDataLoss="false")["fail_on_data_loss"] is False

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="must be 'true' or 'false'"):
            _resolved(failOnDataLoss="maybe")


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


# ---------------------------------------------------------------------------
# _validate_spec_partitions — Spark's exact (topic, partition) set check
# ---------------------------------------------------------------------------


_TPS = [_TP("t", 0), _TP("t", 1)]


class TestValidateSpecPartitions:
    def test_string_spec_is_noop(self):
        _validate_spec_partitions("earliest", _TPS, option_name="startingOffsets")

    def test_global_timestamp_is_noop(self):
        _validate_spec_partitions(_TS_MS, _TPS, option_name="startingTimestamp")

    def test_exact_match_ok(self):
        _validate_spec_partitions({"t": {"0": 1, "1": 2}}, _TPS, option_name="startingOffsets")

    def test_missing_partition_raises(self):
        # Spark requires every assigned partition; without this an omitted
        # partition silently falls back to a watermark.
        with pytest.raises(ValueError, match=r"missing: \[\('t', 1\)\]"):
            _validate_spec_partitions({"t": {"0": 1}}, _TPS, option_name="startingOffsets")

    def test_extra_partition_raises(self):
        with pytest.raises(ValueError, match=r"not assigned: \[\('t', 7\)\]"):
            _validate_spec_partitions({"t": {"0": 1, "1": 2, "7": 3}}, _TPS, option_name="startingOffsets")

    def test_unknown_topic_raises(self):
        with pytest.raises(ValueError, match="not assigned"):
            _validate_spec_partitions({"other": {"0": 1}}, _TPS, option_name="startingOffsets")


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


class TestSchema:
    def test_all_fields_nullable(self):
        # Spark builds these with plain StructFields, which default to
        # nullable=True. Nullability is user-visible in printSchema and in
        # write-compatibility checks.
        schema = _build_schema(include_headers=True)
        assert all(field.nullable for field in schema)

    def test_header_struct_fields_nullable(self):
        headers = _build_schema(include_headers=True).field("headers")
        assert all(field.nullable for field in headers.type.value_type)

    def test_headers_omitted_by_default(self):
        assert "headers" not in _build_schema(include_headers=False).names

    def test_reader_rejects_user_specified_schema(self):
        import pyarrow as pa

        source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
        custom = pa.schema([pa.field("value", pa.string(), nullable=True)])
        with pytest.raises(ValueError, match="fixed schema"):
            source.reader(custom)

    def test_reader_rejects_a_wrong_column_type(self):
        import pyarrow as pa

        source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
        wrong = pa.schema([pa.field(f.name, pa.string() if f.name == "value" else f.type) for f in source.schema()])
        with pytest.raises(ValueError, match="fixed schema"):
            source.reader(wrong)

    def test_reader_accepts_the_fixed_schema(self):
        source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
        assert source.reader(source.schema()) is not None

    def test_reader_tolerates_a_nullability_only_difference(self):
        # The schema Sail passes back has round-tripped through Arrow's C data
        # interface; nullability cannot change what `read()` produces, so it
        # must not be grounds for rejecting our own schema.
        import pyarrow as pa

        source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
        tightened = pa.schema([pa.field(f.name, f.type, nullable=False) for f in source.schema()])
        assert source.reader(tightened) is not None
