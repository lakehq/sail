# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Spark option parsing and validation in the Kafka data source."""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.spark(version=">=4.0", reason="Uses the Python DataSource API")

TS_MS = 1700000000000


@pytest.mark.parametrize("option", ["startingOffsets", "endingOffsetsByTimestamp"])
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("null", 0),
        ("1.9", 1),
        ("-1.9", -1),
        ("-0.9", 0),
        ("1e30", 2**63 - 1),
        ("-1e30", -(2**63)),
        ("1e400", 2**63 - 1),
        ("-1e400", -(2**63)),
        (str(2**63), -(2**63)),
        (str(-(2**63) - 1), 2**63 - 1),
        (str(2**64), 0),
        ("9007199254740993.0", 9007199254740992),
    ],
)
def test_partition_values_use_spark_json_numeric_coercion(option, raw, expected, kafka_options, partition_of_t):
    # Expected values come from Spark 4.1.1 JsonUtils on the JVM, including
    # the different overflow rules for integer and floating-point literals.
    parsed = kafka_options.parse_topic_partition_mapping('{"t": {"0": ' + raw + "}}", option)
    assert parsed == {partition_of_t(0): expected}


@pytest.mark.parametrize("option", ["startingOffsets", "startingOffsetsByTimestamp"])
@pytest.mark.parametrize("partition", [-(2**31) - 1, 2**31])
def test_partition_limit_id_width(option, partition, parse_read_options):
    with pytest.raises(ValueError, match="signed 32-bit"):
        parse_read_options(**{option: json.dumps({"t": {str(partition): 0}})})


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("null", 0),
        ("1.9", 1),
        ("-1.9", -1),
        (str(2**31), -(2**31)),
        (str(-(2**31) - 1), 2**31 - 1),
        ("1e30", 2**31 - 1),
        ("-1e30", -(2**31)),
    ],
)
def test_assignment_uses_spark_json_numeric_coercion(raw, expected, kafka_options, partition_of_t):
    assert kafka_options.parse_assignment('{"t": [' + raw + "]}") == frozenset({partition_of_t(expected)})


@pytest.mark.parametrize(
    "option", ["startingOffsets", "endingOffsets", "startingOffsetsByTimestamp", "endingOffsetsByTimestamp"]
)
@pytest.mark.parametrize(("raw", "expected"), [("null", 0), ("1.9", 1)])
def test_coerced_values_reach_the_read_options(option, raw, expected, parse_read_options, partition_of_t):
    options = parse_read_options(**{option: '{"t": {"0": ' + raw + "}}"})
    limit = options.starting if option.startswith("starting") else options.ending
    values = limit.timestamps if option.endswith("ByTimestamp") else limit.offsets
    assert values == {partition_of_t(0): expected}


def test_coerced_offsets_still_obey_batch_sentinel_rules(parse_read_options):
    with pytest.raises(ValueError, match="does not support offset -1"):
        parse_read_options(startingOffsets='{"t": {"0": -1.9}}')


@pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity"])
def test_nonstandard_json_numbers_are_rejected(raw, kafka_options):
    with pytest.raises(ValueError, match="Invalid JSON numeric constant"):
        kafka_options.parse_assignment('{"t": [' + raw + "]}")
    with pytest.raises(ValueError, match="Invalid JSON numeric constant"):
        kafka_options.parse_topic_partition_mapping('{"t": {"0": ' + raw + "}}", "startingOffsets")


@pytest.mark.parametrize(
    ("option", "raw", "expected"),
    [
        ("startingOffsets", "earliest", -2),
        ("endingOffsets", "latest", -1),
        ("startingOffsets", "  earliest  ", -2),
        ("startingOffsets", "EARLIEST", -2),
        ("startingOffsets", "Earliest", -2),
        ("startingOffsets", "eArLiEsT", -2),
    ],
)
def test_offset_sentinel_spelling(option, raw, expected, parse_read_options):
    resolved = parse_read_options(**{option: raw})
    assert getattr(resolved, "starting" if option == "startingOffsets" else "ending") == expected


@pytest.mark.parametrize("option", ["startingOffsets", "endingOffsetsByTimestamp"])
@pytest.mark.parametrize("raw", ["", "not-json"])
def test_invalid_range_json(option, raw, parse_read_options):
    # A present blank value is invalid; only absence selects the default.
    with pytest.raises(json.JSONDecodeError):
        parse_read_options(**{option: raw})


@pytest.mark.parametrize("key", ["0", "00", "+0"])
@pytest.mark.parametrize("option", ["startingOffsets", "startingOffsetsByTimestamp"])
def test_partition_map_normalizes_integer_keys(option, key, kafka_models, parse_read_options, partition_of_t):
    limit_type = kafka_models.PartitionOffsets if option == "startingOffsets" else kafka_models.PartitionTimestamps
    resolved = parse_read_options(**{option: json.dumps({"t": {key: 5}})})
    assert resolved.starting == limit_type({partition_of_t(0): 5}, option)


@pytest.mark.parametrize("option", ["startingOffsets", "endingOffsetsByTimestamp"])
@pytest.mark.parametrize(
    ("value", "match"),
    [
        ([1, 2, 3], "must be an object"),
        ({"t": 5}, "topic 't' must be an object"),
        ({"t": {"0": "x"}}, "partition '0' must be a number or null"),
        ({"t": {"0": True}}, "must be a number or null"),
        ({"t": {"zero": 1}}, "must be an integer"),
        ({"t": {"1_0": 5}}, "must be an integer"),
        ({"t": {" 0": 5}}, "must be an integer"),
        ({"t": {"0 ": 5}}, "must be an integer"),
    ],
)
def test_invalid_partition_map(option, value, match, parse_read_options):
    with pytest.raises(ValueError, match=match):
        parse_read_options(**{option: json.dumps(value)})


def test_offsets_duplicate_partition_keys_collapse(kafka_models, parse_read_options, partition_of_t):
    # Two spellings of one id are one entry in the Scala `Map` too.
    limit = parse_read_options(startingOffsets='{"t": {"0": 5, "00": 9}}').starting
    assert limit == kafka_models.PartitionOffsets({partition_of_t(0): 9}, "startingOffsets")


@pytest.mark.parametrize("boundary", ["starting", "ending"])
@pytest.mark.parametrize("raw", ["1700000000000", "  1700000000000  ", "\t1700000000000\n"])
def test_timestamps_global_int(boundary, raw, kafka_models, parse_read_options):
    assert getattr(parse_read_options(**{f"{boundary}Timestamp": raw}), boundary) == kafka_models.GlobalTimestamp(TS_MS)


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_timestamps_global_timestamp_width(value, parse_read_options):
    with pytest.raises(ValueError, match="signed 64-bit"):
        parse_read_options(startingTimestamp=str(value))


@pytest.mark.parametrize("value", [-(2**63), 2**63 - 1])
def test_timestamps_global_timestamp_width_boundaries(value, kafka_models, parse_read_options):
    assert parse_read_options(startingTimestamp=str(value)).starting == kafka_models.GlobalTimestamp(value)


@pytest.mark.parametrize("raw", ["1_700_000_000_000", "not-a-number"])
def test_invalid_global_timestamp(raw, parse_read_options):
    with pytest.raises(ValueError, match="must be an integer"):
        parse_read_options(startingTimestamp=raw)


def test_range_precedence_absent_returns_default_sentinel(kafka_options, kafka_models, case_insensitive):
    assert (
        kafka_options.parse_range_limit(case_insensitive({}), boundary="starting")
        is kafka_models.SpecialOffset.EARLIEST
    )
    assert kafka_options.parse_range_limit(case_insensitive({}), boundary="ending") is kafka_models.SpecialOffset.LATEST


def test_range_precedence_offsets_only(kafka_options, kafka_models, case_insensitive):
    spec = kafka_options.parse_range_limit(case_insensitive({"startingOffsets": "earliest"}), boundary="starting")
    assert spec is kafka_models.SpecialOffset.EARLIEST


def test_range_precedence_global_timestamp_beats_everything(kafka_options, kafka_models, case_insensitive):
    # Spark checks the global timestamp first and never looks at the rest,
    # so the lower-priority values are not even parsed.
    spec = kafka_options.parse_range_limit(
        case_insensitive(
            {
                "startingTimestamp": "1700000000000",
                "startingOffsetsByTimestamp": "not-even-json",
                "startingOffsets": "not-even-json",
            }
        ),
        boundary="starting",
    )
    assert spec == kafka_models.GlobalTimestamp(TS_MS)


def test_range_precedence_per_partition_timestamp_beats_offsets(
    kafka_options, kafka_models, case_insensitive, partition_of_t
):
    spec = kafka_options.parse_range_limit(
        case_insensitive(
            {
                "endingOffsetsByTimestamp": '{"t": {"0": 1700000000000}}',
                "endingOffsets": "not-even-json",
            }
        ),
        boundary="ending",
    )
    assert spec == kafka_models.PartitionTimestamps({partition_of_t(0): TS_MS}, "endingOffsetsByTimestamp")


def test_range_precedence_combination_is_not_rejected(kafka_models, parse_read_options):
    # Spark applies precedence rather than failing, so a job that supplies
    # a fallback keeps working.
    resolved = parse_read_options(startingOffsets="earliest", startingTimestamp="1700000000000")
    assert resolved.starting == kafka_models.GlobalTimestamp(TS_MS)


def test_range_precedence_blank_lower_priority_option_is_ignored(kafka_models, parse_read_options):
    resolved = parse_read_options(endingOffsets="", endingTimestamp="1700000000000")
    assert resolved.ending == kafka_models.GlobalTimestamp(TS_MS)


def test_timestamp_strategy_default_is_error(kafka_models, parse_read_options):
    assert parse_read_options().starting_ts_strategy is kafka_models.TimestampStrategy.ERROR


@pytest.mark.parametrize("raw", ["latest", "LATEST", "Latest"])
def test_timestamp_strategy_latest_accepted(raw, kafka_models, parse_read_options):
    assert parse_read_options(startingOffsetsByTimestampStrategy=raw).starting_ts_strategy is (
        kafka_models.TimestampStrategy.LATEST
    )


@pytest.mark.parametrize("raw", ["earliest", " latest", "latest ", " error ", "\tLATEST\n"])
def test_timestamp_strategy_invalid_raises(raw, parse_read_options):
    with pytest.raises(ValueError, match="startingOffsetsByTimestampStrategy"):
        parse_read_options(startingOffsetsByTimestampStrategy=raw)


@pytest.mark.parametrize("name", ["auto.offset.reset", "enable.auto.commit", "enable.partition.eof"])
def test_protected_configs_rejected(name, parse_read_options):
    with pytest.raises(ValueError, match=f"kafka.{name}' is not supported".replace(".", r"\.")):
        parse_read_options(**{f"kafka.{name}": "whatever"})


def test_protected_configs_auto_offset_reset_names_the_alternative(parse_read_options):
    with pytest.raises(ValueError, match="startingOffsets"):
        parse_read_options(**{"kafka.auto.offset.reset": "earliest"})


def test_protected_configs_group_id_still_allowed(parse_read_options):
    assert parse_read_options(**{"kafka.group.id": "mine"}).client_config["group.id"] == "mine"


def test_isolation_defaults_to_read_uncommitted(parse_read_options):
    from confluent_kafka import IsolationLevel

    # librdkafka defaults to `read_committed`; Spark never sets the property
    # and so inherits the Java client's `read_uncommitted`. Left to the
    # librdkafka default, a transactional topic would silently return a
    # different row set here than under Spark.
    assert parse_read_options().isolation_level == IsolationLevel.READ_UNCOMMITTED


@pytest.mark.parametrize(
    ("raw", "level_name"),
    [
        ("read_committed", "READ_COMMITTED"),
        ("read_uncommitted", "READ_UNCOMMITTED"),
        ("READ_COMMITTED", "READ_COMMITTED"),
        ("Read_Committed", "READ_COMMITTED"),
    ],
)
def test_isolation_planning_accepts_any_letter_case(raw, level_name, parse_read_options):
    from confluent_kafka import IsolationLevel

    # Spark's KafkaOffsetReaderAdmin upper-cases the value before parsing it, so
    # planning (`ListOffsetsOptions` for "latest" and timestamp lookups) accepts
    # any case. The consumers' stricter check is in `build_consumer_config`.
    assert parse_read_options(**{"kafka.isolation.level": raw}).isolation_level == IsolationLevel[level_name]


def test_isolation_raw_value_is_kept_for_the_consumers(parse_read_options):
    # As in Spark, consumers receive the property as the user wrote it.
    config = parse_read_options(**{"kafka.isolation.level": "READ_COMMITTED"}).client_config
    assert config["isolation.level"] == "READ_COMMITTED"


@pytest.mark.parametrize("raw", ["read_everything", " READ_COMMITTED ", "read_uncommitted ", "\tread_committed\n"])
def test_isolation_invalid_value_fails_option_validation(raw, parse_read_options):
    with pytest.raises(ValueError, match="must be 'read_uncommitted' or 'read_committed'"):
        parse_read_options(**{"kafka.isolation.level": raw})


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("a, b ,c", ("a", "b", "c")), ("t,t, t", ("t",)), ("b,a,b,c", ("a", "b", "c"))],
)
def test_subscribe_normalizes_topics(raw, expected, kafka_models, parse_read_options):
    assert parse_read_options(subscribe=raw).subscription == kafka_models.SubscribeTopics(expected)


def test_subscription_subscribe_empty_raises(kafka_options, case_insensitive):
    with pytest.raises(ValueError, match="at least one topic"):
        kafka_options.parse_subscription(case_insensitive({"subscribe": " , "}))


@pytest.mark.parametrize("raw", ["orders-.*", "  orders-.*  "])
def test_subscription_subscribe_pattern(raw, kafka_options, kafka_models, case_insensitive):
    sub = kafka_options.parse_subscription(case_insensitive({"subscribePattern": raw}))
    assert isinstance(sub, kafka_models.SubscribePattern)
    assert sub.pattern.search("orders-eu")
    assert sub.pattern.search("prefix-orders-eu")
    assert not sub.pattern.search("shipments")


def test_subscription_subscribe_pattern_invalid_regex_raises(kafka_options, case_insensitive):
    with pytest.raises(ValueError, match="not a valid regular expression"):
        kafka_options.parse_subscription(case_insensitive({"subscribePattern": "orders-["}))


@pytest.mark.parametrize(
    ("assignment", "partitions"), [({}, []), ({"t": []}, []), ({"t": [-1]}, [-1]), ({"t": [0, 2]}, [0, 2])]
)
def test_assignment_accepts_empty_and_nonexistent_ids(
    assignment, partitions, kafka_options, kafka_models, case_insensitive, partition_of_t
):
    # Spark filters these IDs against broker metadata during planning.
    actual = kafka_options.parse_subscription(case_insensitive({"assign": json.dumps(assignment)}))
    assert actual == kafka_models.AssignPartitions(frozenset(partition_of_t(p) for p in partitions))


def test_subscription_assign_invalid_json_raises(kafka_options, case_insensitive):
    with pytest.raises(json.JSONDecodeError):
        kafka_options.parse_subscription(case_insensitive({"assign": "not-json"}))


def test_subscription_assign_bool_partition_raises(kafka_options, case_insensitive):
    with pytest.raises(ValueError, match="must be a number or null"):
        kafka_options.parse_subscription(case_insensitive({"assign": '{"t": [true]}'}))


def test_subscription_none_specified_raises(kafka_options, case_insensitive):
    with pytest.raises(ValueError, match="must be specified"):
        kafka_options.parse_subscription(case_insensitive({"kafka.bootstrap.servers": "localhost:9092"}))


def test_subscription_more_than_one_raises(kafka_options, case_insensitive):
    with pytest.raises(ValueError, match="Only one of the following options"):
        kafka_options.parse_subscription(case_insensitive({"subscribe": "t", "assign": '{"t": [0]}'}))


def test_option_case_upper_case_option_keys(kafka_models):
    from pysail.spark.datasource.kafka import KafkaDataSource

    source = KafkaDataSource(
        options={
            "KAFKA.BOOTSTRAP.SERVERS": "localhost:9092",
            "SUBSCRIBE": "t",
            "STARTINGOFFSETS": "earliest",
            "INCLUDEHEADERS": "true",
        }
    )
    resolved = source.read_options
    assert resolved.client_config["bootstrap.servers"] == "localhost:9092"
    assert resolved.subscription == kafka_models.SubscribeTopics(("t",))
    assert resolved.starting is kafka_models.SpecialOffset.EARLIEST
    assert resolved.include_headers is True


def test_option_case_lower_case_option_keys(kafka_models, parse_read_options):
    resolved = parse_read_options(startingoffsets="earliest", maxbatchrows="99")
    assert resolved.starting is kafka_models.SpecialOffset.EARLIEST
    assert resolved.max_batch_rows == 99


def test_option_case_insensitive_dict_input_is_not_flattened(kafka_models, case_insensitive):
    from pysail.spark.datasource.kafka import KafkaDataSource

    # This is what PySpark and Sail actually pass to the constructor.
    source = KafkaDataSource(
        options=case_insensitive(
            {
                "kafka.bootstrap.servers": "localhost:9092",
                "subscribe": "t",
                "startingOffsets": "earliest",
                "maxBatchRows": "77",
            }
        )
    )
    resolved = source.read_options
    assert resolved.starting is kafka_models.SpecialOffset.EARLIEST
    assert resolved.max_batch_rows == 77


def test_boolean_defaults(parse_read_options):
    resolved = parse_read_options()
    assert resolved.include_headers is False
    assert resolved.fail_on_data_loss is True


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("TRUE", True), ("False", False)])
def test_boolean_case_insensitive_values(raw, expected, parse_read_options):
    assert parse_read_options(includeHeaders=raw).include_headers is expected
    assert parse_read_options(failOnDataLoss=raw).fail_on_data_loss is expected


@pytest.mark.parametrize("option", ["includeHeaders", "failOnDataLoss"])
@pytest.mark.parametrize("raw", ["", "yes", "0", "1", "maybe", " true", "false ", "\tTRUE\n"])
def test_boolean_invalid_values_rejected(option, raw, parse_read_options):
    with pytest.raises(ValueError, match=rf"{option}.*must be 'true' or 'false'"):
        parse_read_options(**{option: raw})


@pytest.mark.parametrize("option", ["maxBatchRows", "maxBatchBytes"])
@pytest.mark.parametrize("value", ["", "0", "-1", "nan", "inf", "1_000", " 7 "])
def test_numeric_batch_resource_limits_reject_invalid_values(option, value, parse_read_options):
    with pytest.raises(ValueError, match=rf"{option}"):
        parse_read_options(**{option: value})


@pytest.mark.parametrize("raw", ["1_000", " 100 "])
def test_numeric_poll_timeout_rejects_underscores_and_whitespace(raw, parse_read_options):
    with pytest.raises(ValueError, match=r"pollTimeoutMs"):
        parse_read_options(pollTimeoutMs=raw)


@pytest.mark.parametrize(
    ("options", "seconds"),
    [
        ({}, 120.0),
        ({"kafkaConsumer.pollTimeoutMs": "250"}, 0.25),
        ({"pollTimeoutMs": "250"}, 0.25),
        ({"kafkaConsumer.pollTimeoutMs": "500", "pollTimeoutMs": "invalid"}, 0.5),
        ({"kafkaConsumer.pollTimeoutMs": "500", "pollTimeoutMs": str(2**31)}, 0.5),
        ({"pollTimeoutMs": "3600001"}, 3600.001),
        ({"kafkaConsumer.pollTimeoutMs": str(2**31 - 1)}, (2**31 - 1) / 1000),
        ({"pollTimeoutMs": str(2**31 - 1)}, (2**31 - 1) / 1000),
    ],
)
def test_poll_timeout_units_bounds_and_precedence(options, seconds, parse_read_options):
    assert parse_read_options(**options).poll_timeout_s == seconds


@pytest.mark.parametrize(
    "options",
    [
        {"pollTimeoutMs": str(2**31)},
        {"kafkaConsumer.pollTimeoutMs": str(2**31)},
        {"kafkaConsumer.pollTimeoutMs": str(2**63 - 1)},
        {"kafkaConsumer.pollTimeoutMs": str(2**31), "pollTimeoutMs": "1000"},
    ],
)
def test_poll_timeout_rejects_c_int_overflow(options, parse_read_options):
    with pytest.raises(ValueError, match="must be at most 2147483647"):
        parse_read_options(**options)


@pytest.mark.parametrize("raw", ["250.5", "2.5e2", "1e3", "Infinity", "inf", "nan"])
def test_numeric_poll_timeout_must_be_whole_milliseconds(raw, parse_read_options):
    # Spark reads kafkaConsumer.pollTimeoutMs with `toLong`, which rejects
    # fractions, exponents, and non-finite values.
    with pytest.raises(ValueError, match=r"pollTimeoutMs.*must be an integer"):
        parse_read_options(pollTimeoutMs=raw)


@pytest.mark.parametrize("raw", ["0", "-1"])
def test_numeric_non_positive_poll_timeout_rejected(raw, parse_read_options):
    with pytest.raises(ValueError, match=r"pollTimeoutMs.*positive"):
        parse_read_options(pollTimeoutMs=raw)


def test_numeric_min_partitions_absent(parse_read_options):
    assert parse_read_options().min_partitions is None
    assert parse_read_options().max_records_per_partition is None


def test_numeric_min_partitions_parsed(parse_read_options):
    assert parse_read_options(minPartitions="4").min_partitions == 4
    assert parse_read_options(maxRecordsPerPartition="100").max_records_per_partition == 100


def test_numeric_min_partitions_must_be_positive(parse_read_options):
    with pytest.raises(ValueError, match="must be positive"):
        parse_read_options(minPartitions="0")


def test_numeric_min_partitions_is_bounded_by_int32(parse_read_options):
    # Spark parses this with `String.toInt`, which rejects anything wider.
    # Python integers are unbounded, so without the bound the value goes
    # straight into the splitter as a chunk count.
    assert parse_read_options(minPartitions=str(2**31 - 1)).min_partitions == 2**31 - 1
    with pytest.raises(ValueError, match="must be at most 2147483647"):
        parse_read_options(minPartitions=str(2**31))


def test_numeric_max_records_per_partition_is_bounded_by_int64(parse_read_options):
    # Spark parses this one with `String.toLong`.
    assert parse_read_options(maxRecordsPerPartition=str(2**63 - 1)).max_records_per_partition == 2**63 - 1
    with pytest.raises(ValueError, match="must be at most 9223372036854775807"):
        parse_read_options(maxRecordsPerPartition=str(2**63))


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("startingOffsets", {"t": {"0": -2}}),
        ("endingOffsets", {"t": {"0": -1}}),
        ("startingOffsets", {"t": {"0": 0, "1": 12345}}),
        ("endingOffsets", {"t": {"0": 0, "1": 12345}}),
    ],
)
def test_valid_batch_offset_maps(option, value, parse_read_options):
    parse_read_options(**{option: json.dumps(value)})


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("startingOffsets", "latest"),
        ("endingOffsets", "earliest"),
        ("startingOffsets", '{"t":{"0":-1}}'),
        ("endingOffsets", '{"t":{"0":-2}}'),
    ],
)
def test_forbidden_batch_sentinels(option, value, parse_read_options):
    with pytest.raises(ValueError, match=f"'{option}' does not support"):
        parse_read_options(**{option: value})


@pytest.mark.parametrize("boundary", ["starting", "ending"])
@pytest.mark.parametrize("offset", [-3, -1000, -1001])
def test_other_negative_offsets_are_rejected(boundary, offset, parse_read_options):
    # Never forward OFFSET_STORED / OFFSET_INVALID to the Kafka client.
    with pytest.raises(ValueError, match=rf"'{boundary}Offsets'.*must be >= 0"):
        parse_read_options(**{f"{boundary}Offsets": json.dumps({"t": {"0": offset}})})


def test_offset_error_names_the_offending_partition(parse_read_options):
    with pytest.raises(ValueError, match="topic 't' partition 3"):
        parse_read_options(startingOffsets='{"t":{"0":5,"3":-7}}')


def range_validation_forbid_admin(monkeypatch):
    from pysail.spark.datasource.kafka import client as kafka_client

    def fail_on_create(_config):
        pytest.fail("AdminClient should not be instantiated before range-limit validation")

    monkeypatch.setattr(kafka_client, "AdminClient", fail_on_create)


@pytest.mark.parametrize(
    ("start_opt", "end_opt", "start_val", "end_val", "match"),
    [
        ("startingOffsets", "endingOffsets", 8, 3, "Inverted offset range"),
        ("startingOffsetsByTimestamp", "endingOffsetsByTimestamp", 200, 100, "Inverted timestamp range"),
        ("startingOffsetsByTimestamp", "endingOffsetsByTimestamp", -100, -200, "Inverted timestamp range"),
        ("startingOffsetsByTimestamp", "endingOffsetsByTimestamp", 100, -5, "Inverted timestamp range"),
    ],
)
def test_range_validation_inverted_paired_maps_rejected_before_broker_access(
    monkeypatch, start_opt, end_opt, start_val, end_val, match, parse_read_options
):
    range_validation_forbid_admin(monkeypatch)
    with pytest.raises(ValueError, match=match):
        parse_read_options(
            **{
                start_opt: json.dumps({"t": {"0": start_val}}),
                end_opt: json.dumps({"t": {"0": end_val}}),
            }
        )


@pytest.mark.parametrize(
    ("start_opt", "end_opt"),
    [
        ("startingOffsets", "endingOffsets"),
        ("startingOffsetsByTimestamp", "endingOffsetsByTimestamp"),
    ],
)
def test_range_validation_mismatched_paired_map_keys_rejected_before_broker_access(
    monkeypatch, start_opt, end_opt, parse_read_options
):
    range_validation_forbid_admin(monkeypatch)
    with pytest.raises(ValueError, match="must specify the same TopicPartitions"):
        parse_read_options(
            **{
                start_opt: json.dumps({"t": {"0": 1}}),
                end_opt: json.dumps({"t": {"0": 1, "1": 2}}),
            }
        )


@pytest.mark.parametrize(
    ("start_opt", "end_opt", "val"),
    [
        ("startingOffsets", "endingOffsets", 5),
        ("startingOffsetsByTimestamp", "endingOffsetsByTimestamp", 100),
    ],
)
def test_range_validation_equal_paired_endpoints_accepted(start_opt, end_opt, val, parse_read_options):
    resolved = parse_read_options(
        **{
            start_opt: json.dumps({"t": {"0": val}}),
            end_opt: json.dumps({"t": {"0": val}}),
        }
    )
    assert resolved.starting is not None
    assert resolved.ending is not None


def test_range_validation_paired_offset_sentinels_accepted(kafka_models, parse_read_options, partition_of_t):
    resolved = parse_read_options(
        startingOffsets='{"t": {"0": -2, "1": 5}}',
        endingOffsets='{"t": {"0": 0, "1": -1}}',
    )
    assert resolved.starting == kafka_models.PartitionOffsets(
        {partition_of_t(0): -2, partition_of_t(1): 5}, "startingOffsets"
    )
    assert resolved.ending == kafka_models.PartitionOffsets(
        {partition_of_t(0): 0, partition_of_t(1): -1}, "endingOffsets"
    )


@pytest.mark.parametrize(
    "opts",
    [
        {"startingOffsets": '{"t": {"0": 8}}', "endingOffsets": "latest"},
        {"startingOffsets": '{"t": {"0": 8}}', "endingOffsetsByTimestamp": '{"t": {"0": 100}}'},
        {"startingTimestamp": "200", "endingTimestamp": "100"},
        {"startingTimestamp": "200", "startingOffsets": '{"t": {"0": 8}}', "endingOffsets": '{"t": {"0": 3}}'},
    ],
)
def test_range_validation_mixed_global_and_shadowed_limits_not_rejected_pairwise(opts, parse_read_options):
    resolved = parse_read_options(**opts)
    assert resolved.starting is not None
    assert resolved.ending is not None


def test_missing_bootstrap_servers_fail_before_client_creation():
    from pysail.spark.datasource.kafka import KafkaDataSource

    with pytest.raises(ValueError, match=r"bootstrap\.servers"):
        KafkaDataSource(options={"subscribe": "t"}).reader(None)


@pytest.mark.parametrize("servers", ["", " ", "\t\n"])
def test_blank_bootstrap_servers_fail_before_client_creation(servers, parse_read_options):
    with pytest.raises(ValueError, match="must not be blank"):
        parse_read_options(**{"kafka.bootstrap.servers": servers})


def test_admin_timeout_respects_client_limits(parse_read_options):
    # librdkafka admin request timeouts accept 1 ms to 1 hour.
    with pytest.raises(ValueError, match=r"adminTimeoutMs.*must be at most 3600000"):
        parse_read_options(adminTimeoutMs="3600001")


@pytest.mark.parametrize("timeout", ["0.5", "1e3"])
def test_admin_timeout_must_be_whole_milliseconds(timeout, parse_read_options):
    with pytest.raises(ValueError, match=r"adminTimeoutMs.*must be an integer"):
        parse_read_options(adminTimeoutMs=timeout)


def test_consumer_config_defaults_applied(kafka_client):
    config = kafka_client.build_consumer_config({"bootstrap.servers": "localhost:9092"})
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["group.id"] == "sail-kafka-reader"
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "error"
    assert config["enable.partition.eof"] is True


def test_consumer_config_user_group_id_overrides_default(kafka_client):
    settings = {"bootstrap.servers": "localhost:9092", "group.id": "my-group"}
    config = kafka_client.build_consumer_config(settings)
    assert config["group.id"] == "my-group"


def test_consumer_config_source_owned_settings_win(kafka_client):
    # Option validation rejects these before we get here; the merge order is
    # the second line of defence, and Spark likewise keeps its own consumer
    # settings authoritative.
    config = kafka_client.build_consumer_config(
        {"enable.auto.commit": "true", "auto.offset.reset": "earliest", "enable.partition.eof": "false"}
    )
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "error"
    assert config["enable.partition.eof"] is True


def test_consumer_config_isolation_level_defaults_to_read_uncommitted(kafka_client):
    # The Java client's default, which Spark's executor consumers inherit.
    assert kafka_client.build_consumer_config({})["isolation.level"] == "read_uncommitted"


@pytest.mark.parametrize("raw", ["read_committed", "read_uncommitted"])
def test_consumer_config_isolation_level_passes_through(raw, kafka_client):
    assert kafka_client.build_consumer_config({"isolation.level": raw})["isolation.level"] == raw


@pytest.mark.parametrize("raw", ["READ_COMMITTED", "Read_Committed", "READ_UNCOMMITTED"])
def test_consumer_config_isolation_level_is_case_sensitive(raw, kafka_client):
    # Spark's executor consumers validate the raw value with the Java client's
    # exact-match check; librdkafka would accept any case, so Sail checks it.
    with pytest.raises(ValueError, match=r"Invalid value .* for configuration isolation\.level"):
        kafka_client.build_consumer_config({"isolation.level": raw})


def test_admin_config_strips_isolation_level(kafka_client):
    # Planning passes the level to list_offsets; the admin handle must not warn about it.
    assert kafka_client.build_admin_config({"isolation.level": "read_committed"}) == {}


def test_admin_config_connection_settings_pass_through(kafka_client, parse_read_options):
    settings = {
        "bootstrap.servers": "localhost:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "test-user",
        "sasl.password": "test-only-secret",
        "ssl.ca.location": "/test/ca.pem",
        "enable.ssl.certificate.verification": "true",
    }
    config = parse_read_options(**{f"KAFKA.{key.upper()}": value for key, value in settings.items()}).client_config
    assert kafka_client.build_admin_config(config) == settings
    consumer_config = kafka_client.build_consumer_config(config)
    assert all(consumer_config[key] == value for key, value in settings.items())


@pytest.mark.parametrize("kind", ["admin", "consumer"])
def test_admin_config_invalid_security_protocol_fails_without_plaintext_fallback(
    kind, kafka_client, parse_read_options
):
    from confluent_kafka import Consumer, KafkaException
    from confluent_kafka.admin import AdminClient

    config = parse_read_options(**{"kafka.security.protocol": "not-a-protocol"}).client_config
    client, client_config = (
        (AdminClient, kafka_client.build_admin_config(config))
        if kind == "admin"
        else (Consumer, kafka_client.build_consumer_config(config))
    )
    # Real librdkafka validation fails before it can open a connection.
    with pytest.raises(KafkaException, match=r"security\.protocol"):
        client(client_config)


def test_consumer_tuning_is_stripped_before_creating_admin_handle(caplog, kafka_client):
    import logging

    from confluent_kafka.admin import AdminClient

    settings = dict.fromkeys(kafka_client.CONSUMER_ONLY_CONFIGS, "consumer-only-value")
    config = kafka_client.build_admin_config(settings)
    assert config == {}
    with caplog.at_level(logging.WARNING):
        admin = AdminClient(config, logger=logging.getLogger(__name__))
        admin.poll(0)
    assert not any("consumer property" in record.message for record in caplog.records)


def test_schema_reader_rejects_a_wrong_column_type():
    import pyarrow as pa

    from pysail.spark.datasource.kafka import KafkaDataSource

    source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
    wrong = pa.schema([pa.field(f.name, pa.string() if f.name == "value" else f.type) for f in source.schema()])
    with pytest.raises(ValueError, match="fixed schema"):
        source.reader(wrong)


def test_schema_reader_rejects_a_narrowed_nullability():
    # Spark checks a user-supplied schema with
    # `equalsIgnoreCompatibleNullability`, under which narrowing a nullable
    # column to non-nullable is incompatible. It matters most for `key` and
    # `value`, which Kafka really does emit as null.
    import pyarrow as pa

    from pysail.spark.datasource.kafka import KafkaDataSource

    source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
    tightened = pa.schema([pa.field(f.name, f.type, nullable=False) for f in source.schema()])
    with pytest.raises(ValueError, match="fixed schema"):
        source.reader(tightened)


def test_schema_reader_ignores_schema_metadata():
    from pysail.spark.datasource.kafka import KafkaDataSource

    # Metadata round-trips through Sail and cannot change what `read()`
    # produces, so it must not be grounds for rejecting our own schema.
    source = KafkaDataSource(options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t"})
    with_metadata = source.schema().with_metadata({b"sail": b"1"})
    assert source.reader(with_metadata) is not None


def test_schema_headers_nested_nullability():
    import pyarrow as pa

    from pysail.spark.datasource.kafka import KafkaDataSource

    source = KafkaDataSource(
        options={"kafka.bootstrap.servers": "localhost:9092", "subscribe": "t", "includeHeaders": "true"}
    )
    schema = source.schema()
    assert all(field.nullable for field in schema)
    headers_field = schema.field("headers")
    assert pa.types.is_list(headers_field.type)
    assert headers_field.type.value_field.nullable
    struct_type = headers_field.type.value_type
    assert pa.types.is_struct(struct_type)
    assert [(field.name, field.type, field.nullable) for field in struct_type] == [
        ("key", pa.string(), True),
        ("value", pa.binary(), True),
    ]
