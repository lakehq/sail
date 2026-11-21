# SPDX-License-Identifier: Apache-2.0

import os
from datetime import date, time
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row


@pytest.mark.parametrize("file_format", ["parquet", "json", "csv"])
def test_base_path_partition_discovery(spark, tmp_path, file_format):
    root = tmp_path / "table"
    day = date(2026, 1, 1)
    data = spark.createDataFrame(
        [("first", day, "CN"), ("second", day, "US"), ("third", date(2026, 1, 2), "CN")],
        "id STRING, delivery_date DATE, country STRING",
    )
    data.write.format(file_format).option("header", True).partitionBy("delivery_date", "country").save(str(root))
    selected = root / "delivery_date=2026-01-01"
    reader = spark.read.format(file_format).option("header", True)
    assert "delivery_date" not in reader.load(str(selected)).columns

    reader = reader.option("bAsEpAtH", str(root))
    df = reader.load(str(selected))
    assert df.schema["delivery_date"].dataType.simpleString() == "date"
    assert all("2026-01-01" in path for path in df.inputFiles())
    assert df.orderBy("id").collect() == [
        Row(id="first", delivery_date=day, country="CN"),
        Row(id="second", delivery_date=day, country="US"),
    ]
    assert df.where("delivery_date = DATE '2026-01-02'").count() == 0
    assert df.where("country = 'CN'").select("id").collect() == [Row(id="first")]

    leaf_file = next((selected / "country=CN").glob(f"*.{file_format}"))
    explicit = reader.schema("id STRING, delivery_date STRING, country STRING").load(str(leaf_file))
    assert explicit.collect() == [Row(id="first", delivery_date="2026-01-01", country="CN")]


def test_base_path_multiple_inputs(spark, tmp_path):
    for number in (1, 2, 3):
        part = tmp_path / f"part={number}"
        part.mkdir()
        (part / "data.json").write_text('{"value":"row"}')
    paths = [str(tmp_path / "part=1"), str(tmp_path / "part=3")]
    df = spark.read.option("basePath", tmp_path.as_uri()).schema("value STRING").json(paths)
    assert df.orderBy("part").collect() == [Row(value="row", part=1), Row(value="row", part=3)]


@pytest.mark.parametrize("invalid_base", ["file", "sibling", "missing"])
def test_invalid_base_path(spark, tmp_path, invalid_base):
    root = tmp_path / "table-data"
    root.mkdir()
    data = root / "row.json"
    data.write_text('{"value":1}')
    sibling = tmp_path / "other"
    sibling.mkdir()
    base = {"file": data, "sibling": sibling, "missing": tmp_path / "missing"}[invalid_base]
    with pytest.raises(Exception, match="basePath"):
        spark.read.option("basePath", str(base)).json(str(data)).collect()


def test_base_path_partition_type_inference_disabled(spark, tmp_path):
    part = tmp_path / "part=001"
    part.mkdir()
    (part / "data.json").write_text('{"value":1}')
    key = "spark.sql.sources.partitionColumnTypeInference.enabled"
    try:
        spark.conf.set(key, "false")
        df = spark.read.option("basePath", str(tmp_path)).json(str(part))
        assert df.select("part").collect() == [Row(part="001")]
    finally:
        spark.conf.unset(key)


def test_base_path_mixed_partition_values(spark, tmp_path):
    selected = tmp_path / "country=CN"
    values = [str(i) for i in range(12)] + ["unknown", "__HIVE_DEFAULT_PARTITION__"]
    for value in values:
        directory = selected / f"code={value}"
        directory.mkdir(parents=True)
        (directory / "row.json").write_text('{"id":1}')

    df = spark.read.option("basePath", str(tmp_path)).json(str(selected))
    assert df.schema["code"].dataType.simpleString() == "string"
    assert {row.code for row in df.select("code").collect()} == {*values[:-1], None}


def write_json_partition(root, path):
    directory = root / path
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "row.json").write_text('{"id":1}')


def test_base_path_null_partition_filter(spark, tmp_path):
    for path in ["p=2", "p=__HIVE_DEFAULT_PARTITION__"]:
        write_json_partition(tmp_path, path)
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.where("p < 1").collect() == []
    assert df.where("p IS NULL").collect() == [Row(id=1, p=None)]


def test_base_path_decimal_partitions(spark, tmp_path):
    values = [2**63 + 1, 2**63 + 2]
    for value in values:
        write_json_partition(tmp_path, f"p={value}")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.schema["p"].dataType.simpleString() == "decimal(19,0)"
    assert df.select("p").distinct().orderBy("p").collect() == [Row(p=Decimal(value)) for value in values]


@pytest.mark.parametrize("infer_schema", [True, False])
def test_base_path_overlapping_csv_columns(spark, tmp_path, infer_schema):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "rows.csv").write_text("p,data,other\nignored,42,abc\n")
    df = spark.read.options(basePath=str(tmp_path), header=True, inferSchema=infer_schema).csv(str(part))
    data = 42 if infer_schema else "42"
    assert df.columns == ["p", "data", "other"]
    assert df.collect() == [Row(p=1, data=data, other="abc")]
    assert df.select("other", "p").collect() == [Row(other="abc", p=1)]
    assert df.where("p = 0").collect() == []


@pytest.mark.parametrize("case_sensitive", [False, True])
def test_base_path_mixed_case_keys(spark, tmp_path, case_sensitive):
    for path in ["P=2", "p=1"]:
        write_json_partition(tmp_path, path)
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(case_sensitive).lower())
        reader = spark.read.option("basePath", str(tmp_path))
        if case_sensitive:
            with pytest.raises(Exception, match=r"(?i)(mixed partition columns|conflicting partition column)"):
                reader.json(str(tmp_path)).collect()
        else:
            assert [r[0] for r in reader.json(str(tmp_path)).select("p").orderBy("p").collect()] == [1, 2]
    finally:
        spark.conf.set(key, previous)


def test_base_path_partition_directory_boundary(spark, tmp_path):
    part = "outside=1/data/first%2Flast=ok"
    write_json_partition(tmp_path, part)
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.columns == ["id", "first/last"]
    assert df.select("first/last").collect() == [Row("ok")]


def test_base_path_loose_file(spark, tmp_path):
    write_json_partition(tmp_path, "p=1")
    (tmp_path / "loose.json").write_text('{"id":99}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize("timestamp_type", ["TIMESTAMP_LTZ", "TIMESTAMP_NTZ"])
@pytest.mark.parametrize("session_timezone", ["America/New_York"], indirect=True)
@pytest.mark.usefixtures("session_timezone")
def test_base_path_timestamp_partition(spark, tmp_path, timestamp_type):
    write_json_partition(tmp_path, "p=2025-01-01 12%3A01%3A02")
    key = "spark.sql.timestampType"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, timestamp_type)
        df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
        expected_type = "timestamp" if timestamp_type == "TIMESTAMP_LTZ" else "timestamp_ntz"
        assert df.schema["p"].dataType.simpleString() == expected_type
        assert df.selectExpr("cast(p as string) AS s").collect() == [Row(s="2025-01-01 12:01:02")]
        if timestamp_type == "TIMESTAMP_LTZ":
            assert df.selectExpr("unix_seconds(p) AS seconds").collect() == [Row(seconds=1735750862)]
    finally:
        spark.conf.set(key, previous)


@pytest.mark.parametrize("validate", [False, True])
def test_base_path_invalid_partition_cast(spark, tmp_path, validate):
    write_json_partition(tmp_path, "p=oops")
    key = "spark.sql.sources.validatePartitionColumns"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(validate).lower())
        reader = spark.read.schema("id INT, p INT").option("basePath", str(tmp_path))
        if validate:
            with pytest.raises(Exception, match="oops"):
                reader.json(str(tmp_path)).collect()
        else:
            assert reader.json(str(tmp_path)).collect() == [Row(id=1, p=None)]
    finally:
        spark.conf.set(key, previous)


def test_base_path_empty_partition_value(spark, tmp_path):
    write_json_partition(tmp_path, "p=")
    with pytest.raises(Exception, match=r"(?i)empty.*partition column"):
        spark.read.option("basePath", str(tmp_path)).json(str(tmp_path)).collect()


def test_base_path_recursive_file_lookup(spark, tmp_path):
    write_json_partition(tmp_path, "p=1")
    (tmp_path / "loose.json").write_text('{"id":2}')
    df = spark.read.options(basePath=str(tmp_path), recursiveFileLookup=True).json(str(tmp_path))
    assert df.columns == ["id"]
    assert df.orderBy("id").collect() == [Row(id=1), Row(id=2)]


@pytest.mark.parametrize("pattern", ["p=1*/row.json", "p=1*", "p=1*/"])
def test_base_path_glob_matches_partition_directories(spark, tmp_path, pattern):
    for path in ["p=1", "p=11", "p=2"]:
        write_json_partition(tmp_path, path)
    df = spark.read.option("basePath", str(tmp_path)).json(f"{tmp_path}/{pattern}")
    assert df.orderBy("p").collect() == [Row(id=1, p=1), Row(id=1, p=11)]
    assert len(df.inputFiles()) == 2


@pytest.mark.parametrize("schema", [None, "id INT, p TIMESTAMP"])
def test_base_path_reader_timezone(spark, tmp_path, schema):
    write_json_partition(tmp_path, "p=2025-01-01 12%3A01%3A02")
    reader = spark.read.options(basePath=str(tmp_path), timeZone="America/New_York")
    if schema:
        reader = reader.schema(schema)
    df = reader.json(str(tmp_path))
    assert df.selectExpr("unix_seconds(p) AS seconds").collect() == [Row(seconds=1735750862)]


def test_base_path_explicit_csv_positions(spark, tmp_path):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "rows.csv").write_text("p,data\nignored,42\n")
    df = spark.read.options(basePath=str(tmp_path), header=True).schema("data STRING, p INT").csv(str(part))
    assert df.collect() == [Row(data="ignored", p=1)]


def test_base_path_schema_sample_excludes_loose_files(spark, tmp_path):
    for i in range(11):
        (tmp_path / f"{i}.json").write_text('{"loose":42}')
    write_json_partition(tmp_path, "p=1")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.columns == ["id", "p"]
    assert df.collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize("excluded_file", ["empty.json", "nonmatching.txt"])
def test_base_path_discovers_partitions_before_file_filtering(spark, tmp_path, excluded_file):
    write_json_partition(tmp_path, "p=1")
    part = tmp_path / "p=text"
    part.mkdir()
    (part / excluded_file).write_text("" if excluded_file == "empty.json" else '{"id":2}')
    df = spark.read.options(basePath=str(tmp_path), pathGlobFilter="*.json").json(str(tmp_path))
    assert df.schema["p"].dataType.simpleString() == "string"
    assert df.collect() == [Row(id=1, p="1")]


def test_base_path_overlapping_text_value(spark, tmp_path):
    part = tmp_path / "value=hello"
    part.mkdir()
    (part / "rows.txt").write_text("one\ntwo\n")
    df = spark.read.option("basePath", str(tmp_path)).text(str(part))
    assert df.collect() == [Row(value="hello"), Row(value="hello")]


@pytest.mark.parametrize(
    ("token", "expected_type", "value"),
    [
        ("9007199254740993E2", "decimal(18,0)", Decimal(900719925474099300)),
        ("__HIVE_DEFAULT_PARTITION__", "void", None),
        ("%31", "string", "1"),
        ("\uff11\uff12", "int", 12),
        ("-\u0661\u0662", "int", -12),
        ("\uff13000000000", "bigint", 3000000000),
        ("\uff112345678901234567890", "decimal(20,0)", Decimal(12345678901234567890)),
        ("\U0001d7d9\U0001d7da", "string", "\U0001d7d9\U0001d7da"),
        ("2025-1-2", "string", "2025-1-2"),
        ("2025-01-01 10%3A10%3A10.123456", "string", "2025-01-01 10:10:10.123456"),
        ("inf", "string", "inf"),
        ("0x1.8p1", "double", 3.0),
        ("-0X1.8P+1F", "double", -3.0),
        ("0x1p-1074", "double", float.fromhex("0x1p-1074")),
        ("0x1.00000000000008p0", "double", 1.0),
        ("1.25d", "double", 1.25),
        ("%C3%A9", "string", "Ã©"),
        ("雪%FF%2F%xx", "string", "雪ÿ/%xx"),
    ],
)
def test_base_path_partition_literal(spark, tmp_path, token, expected_type, value):
    write_json_partition(tmp_path, f"p={token}")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.schema["p"].dataType.simpleString() == expected_type
    assert df.collect() == [Row(id=1, p=value)]


@pytest.mark.spark(version=">=4.0", reason="Uses ignoreInvalidPartitionPaths")
@pytest.mark.parametrize("per_read", [True, False])
def test_base_path_ignore_invalid_partition_paths(spark, tmp_path, per_read):
    write_json_partition(tmp_path, "p=1")
    write_json_partition(tmp_path, "loose")
    reader = spark.read.option("basePath", str(tmp_path))
    key = "spark.sql.files.ignoreInvalidPartitionPaths"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(not per_read).lower())
        if per_read:
            reader = reader.option("ignoreInvalidPartitionPaths", True)
        assert reader.json(str(tmp_path)).collect() == [Row(id=1, p=1)]
    finally:
        spark.conf.set(key, previous)


def test_base_path_explicit_partition_field_case(spark, tmp_path):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "row.json").write_text('{"id":1,"p":99}')
    df = spark.read.option("basePath", str(tmp_path)).schema("id INT, P INT").json(str(part))
    assert df.columns == ["id", "P"]
    assert df.collect() == [Row(id=1, P=1)]


def test_base_path_unpartitioned_root(spark, tmp_path):
    write_json_partition(tmp_path, "nested")
    (tmp_path / "row.json").write_text('{"id":2}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.collect() == [Row(id=2)]
    assert len(df.inputFiles()) == 1


@pytest.mark.parametrize("overlap", ["p=1", "p=1/row.json"])
def test_base_path_overlapping_inputs(spark, tmp_path, overlap):
    for partition in ["p=1", "p=2"]:
        write_json_partition(tmp_path, partition)
    paths = [str(tmp_path / "p=*"), str(tmp_path / overlap)]
    df = spark.read.option("basePath", str(tmp_path)).json(paths)
    assert df.orderBy("p").collect() == [Row(id=1, p=1), Row(id=1, p=2)]
    assert len(df.inputFiles()) == 2


@pytest.mark.parametrize("missing", ["missing*", "missing"])
def test_base_path_missing_input(spark, tmp_path, missing):
    write_json_partition(tmp_path, "p=1")
    paths = [str(tmp_path / "p=1"), str(tmp_path / missing)]
    with pytest.raises(Exception, match=r"Path does not exist.*missing"):
        spark.read.option("basePath", str(tmp_path)).schema("id INT").json(paths).collect()


@pytest.mark.parametrize("second", ["row.json", "."])
@pytest.mark.parametrize("mode", ["default", "basePath", "recursiveFileLookup"])
def test_unpartitioned_overlapping_inputs(spark, tmp_path, second, mode):
    (tmp_path / "row.json").write_text('{"id":1}')
    reader = spark.read.schema("id INT")
    if mode == "basePath":
        reader = reader.option(mode, str(tmp_path))
    elif mode == "recursiveFileLookup":
        reader = reader.option(mode, True)
    df = reader.json([str(tmp_path), str(tmp_path / second)])
    assert df.collect() == [Row(id=1)] * (1 if mode == "recursiveFileLookup" else 2)


@pytest.mark.parametrize("path", ["empty", "emp*/"])
def test_base_path_empty_directory(spark, tmp_path, path):
    (tmp_path / "empty").mkdir()
    df = spark.read.option("basePath", str(tmp_path)).schema("id INT").json(f"{tmp_path}/{path}")
    assert df.collect() == []


def test_base_path_partial_csv_schema(spark, tmp_path):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "row.csv").write_text("value,other\n42,foo\n")
    df = spark.read.options(basePath=str(tmp_path), header=True).schema("value STRING, p INT").csv(str(part))
    assert df.collect() == [Row(value="42", p=1)]


def test_base_path_timestamp_dst_transitions(spark, tmp_path):
    for token in ["2025-03-09 02%3A30%3A00", "2025-11-02 01%3A30%3A00"]:
        write_json_partition(tmp_path, f"p={token}")
    df = spark.read.options(basePath=str(tmp_path), timeZone="America/New_York").json(str(tmp_path))
    # New York shifts 02:30 forward to 03:30 in spring and uses the first 01:30 in autumn.
    assert df.selectExpr("unix_seconds(p) AS seconds").orderBy("seconds").collect() == [
        Row(seconds=1741505400),
        Row(seconds=1762061400),
    ]


def test_base_path_validates_filtered_partition(spark, tmp_path):
    write_json_partition(tmp_path, "p=1")
    part = tmp_path / "p=bogus"
    part.mkdir()
    (part / "row.txt").write_text('{"id":2}')
    reader = spark.read.options(basePath=str(tmp_path), pathGlobFilter="*.json").schema("id INT, p INT")
    with pytest.raises(Exception, match="bogus"):
        reader.json(str(tmp_path)).collect()


def test_base_path_hive_escaped_partition_name(spark, tmp_path):
    write_json_partition(tmp_path, "caf%E9=1")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.select("café").collect() == [Row(1)]


@pytest.mark.parametrize(("pattern", "expected"), [(".snap*/p=*/row.json", [13]), ("*/p=*/row.json", [1, 13])])
@pytest.mark.spark(version=">=4.0", reason="Uses ignoreInvalidPartitionPaths")
def test_base_path_glob_through_hidden_directory(spark, tmp_path, pattern, expected):
    write_json_partition(tmp_path, ".snapshot/p=13")
    write_json_partition(tmp_path, "visible/p=1")
    reader = spark.read.options(basePath=str(tmp_path), ignoreInvalidPartitionPaths=True)
    assert [r.p for r in reader.json(str(tmp_path / pattern)).orderBy("p").collect()] == expected


def test_base_path_glob_matching_only_hidden_file(spark, tmp_path):
    (tmp_path / ".row.json").write_text('{"id":1}')
    df = spark.read.option("basePath", str(tmp_path)).schema("id INT").json(str(tmp_path / ".row*"))
    assert df.collect() == []


@pytest.mark.parametrize("summary", ["_metadata", "_common_metadata"])
def test_base_path_summary_files(spark, tmp_path, summary):
    write_json_partition(tmp_path, "p=1/data")
    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / summary).touch()
    (tmp_path / "p=1" / "data" / summary).write_text('{"id":99}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.collect() == [Row(id=1, p=1)]


def test_base_path_ntz_partition_with_offset(spark, tmp_path):
    write_json_partition(tmp_path, "p=2026-03-01 00%3A30%3A00+02%3A00")
    df = spark.read.option("basePath", str(tmp_path)).schema("id INT, p TIMESTAMP_NTZ").json(str(tmp_path))
    rows = df.where("CAST(p AS DATE) = DATE '2026-03-01'").selectExpr("CAST(p AS STRING) AS wall_clock").collect()
    assert rows == [Row(wall_clock="2026-03-01 00:30:00")]


@pytest.mark.parametrize("invalid", ["yes", "1"])
def test_base_path_boolean_partition_validation(spark, tmp_path, invalid):
    for value in ["TrUe", "false", invalid]:
        write_json_partition(tmp_path, f"p={value}")
    reader = spark.read.option("basePath", str(tmp_path)).schema("id INT, p BOOLEAN")
    key = "spark.sql.sources.validatePartitionColumns"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "true")
        with pytest.raises(Exception, match=invalid):
            reader.json(str(tmp_path)).collect()
        spark.conf.set(key, "false")
        assert [r.p for r in reader.json(str(tmp_path)).orderBy("p").collect()] == [None, False, True]
    finally:
        spark.conf.set(key, previous)


def test_base_path_partial_date_partitions(spark, tmp_path):
    for token in ["2025", "2025-2"]:
        write_json_partition(tmp_path, f"p={token}")
    df = spark.read.option("basePath", str(tmp_path)).schema("id INT, p DATE").json(str(tmp_path))
    assert [r.p for r in df.orderBy("p").collect()] == [date(2025, 1, 1), date(2025, 2, 1)]


@pytest.mark.spark(version=">=4.1", reason="TIME partition inference requires Spark 4.1")
def test_base_path_time_partitions(spark, tmp_path):
    for token in ["12%3A30%3A00", "12%3A30%3A00.123456"]:
        write_json_partition(tmp_path, f"p={token}")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.schema["p"].dataType.simpleString() == "time(6)"
    # Spark Connect requires TIME to be enabled to return it in an Arrow batch.
    key = "spark.sql.timeType.enabled"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "true")
        assert [r.p for r in df.orderBy("p").collect()] == [time(12, 30), time(12, 30, 0, 123456)]
        assert df.where("p > TIME '12:30:00'").count() == 1
    finally:
        spark.conf.set(key, previous)


@pytest.mark.spark(version="<4.1", reason="Older Spark clients read time-of-day partitions as strings")
def test_base_path_time_partition_before_spark_41(spark, tmp_path):
    write_json_partition(tmp_path, "p=12%3A30%3A00")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.collect() == [Row(id=1, p="12:30:00")]


@pytest.mark.parametrize("schema", [None, "id INT, `Café` STRING"])
def test_base_path_unicode_case_insensitive_keys(spark, tmp_path, schema):
    for name, value in [("CAFÉ", 1), ("café", 2)]:
        write_json_partition(tmp_path, f"{name}={value}")
    reader = spark.read.option("basePath", str(tmp_path))
    if schema:
        reader = reader.schema(schema)
    df = reader.json(str(tmp_path))
    assert len(df.columns) == 2
    if schema:
        assert df.columns == ["id", "Café"]
    assert [row[1] for row in df.orderBy(df.columns[1]).collect()] == (["1", "2"] if schema else [1, 2])


@pytest.mark.parametrize("pattern", ["*", "p=*/id=part.json"])
def test_base_path_equals_in_filename(spark, tmp_path, pattern):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "id=part.json").write_text('{"id":7}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path / pattern))
    assert df.collect() == [Row(id=7, p=1)]
    assert len(df.inputFiles()) == 1


@pytest.mark.parametrize("ignore_invalid", [False, True])
def test_base_path_staging_partition(spark, tmp_path, ignore_invalid):
    for path in ["p=1", "_temporary/p=2"]:
        write_json_partition(tmp_path, path)
    reader = spark.read.options(basePath=str(tmp_path), ignoreInvalidPartitionPaths=ignore_invalid)
    paths = [str(tmp_path / "p=1"), str(tmp_path / "_temporary/p=2")]
    assert reader.json(paths).collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize("summary", ["_metadata", "_common_metadata"])
def test_base_path_recursive_summary(spark, tmp_path, summary):
    write_json_partition(tmp_path, "nested")
    (tmp_path / "nested" / summary).write_text('{"unexpected":2}')
    df = spark.read.options(basePath=str(tmp_path), recursiveFileLookup=True).json(str(tmp_path))
    assert df.columns == ["id"]
    assert df.collect() == [Row(id=1)]


@pytest.mark.parametrize("read_file", [False, True])
def test_base_path_parquet_summary_schema(spark, tmp_path, read_file):
    summary = tmp_path / "_common_metadata"
    pq.write_metadata(pa.schema([("id", pa.int32())]), summary)
    df = spark.read.option("basePath", str(tmp_path)).parquet(str(summary if read_file else tmp_path))
    assert df.schema.simpleString() == "struct<id:int>"
    assert df.collect() == []


def test_base_path_parquet_metadata_prefix_schema(spark, tmp_path):
    pq.write_table(pa.table({"old": [1]}), tmp_path / "_metadata.old")
    reader = spark.read.option("basePath", str(tmp_path))
    df = reader.parquet(str(tmp_path))
    assert df.columns == ["old"]
    assert df.collect() == []

    pq.write_table(pa.table({"new": [2]}), tmp_path / "part.parquet")
    df = reader.option("mergeSchema", True).parquet(str(tmp_path))
    assert df.select("new", "old").collect() == [Row(new=2, old=None)]


def test_base_path_local_glob_percent_escape(spark, tmp_path):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "foo%3Abar.json").write_text('{"id":1}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path / "p=*/foo%3Abar.json"))
    assert df.collect() == [Row(id=1, p=1)]


def test_base_path_literal_directory(spark, tmp_path):
    root = tmp_path / "bracket[old]"
    write_json_partition(root, "p=1")
    df = spark.read.option("basePath", str(root)).json(str(tmp_path / "bracket*/p=1"))
    assert df.collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize("ty", ["TINYINT", "SMALLINT", "INT", "BIGINT"])
def test_base_path_integer_whitespace(spark, tmp_path, ty):
    write_json_partition(tmp_path, "p= 123")
    reader = spark.read.option("basePath", str(tmp_path)).schema(f"id INT, p {ty}")
    key = "spark.sql.sources.validatePartitionColumns"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, "true")
        with pytest.raises(Exception, match="123"):
            reader.json(str(tmp_path)).collect()
        spark.conf.set(key, "false")
        assert reader.json(str(tmp_path)).collect() == [Row(id=1, p=None)]
    finally:
        spark.conf.set(key, previous)


def test_base_path_void_partition(spark, tmp_path):
    write_json_partition(tmp_path, "p=text")
    df = spark.read.option("basePath", str(tmp_path)).schema("id INT, p VOID").json(str(tmp_path))
    assert df.collect() == [Row(id=1, p=None)]


def test_base_path_file_glob_schema_sample(spark, tmp_path):
    for i in range(10):
        part = tmp_path / f"p={i}"
        part.mkdir()
        pq.write_table(pa.table({"id": [i]}), part / "row.parquet")
    # This partition contributes to type discovery but is outside the footer sample
    # and is pruned before scanning.
    last = tmp_path / "p=zzz"
    last.mkdir()
    (last / "row.parquet").write_bytes(b"invalid footer")
    df = spark.read.option("basePath", str(tmp_path)).parquet(str(tmp_path / "p=*/row.parquet"))
    assert df.schema.simpleString() == "struct<id:bigint,p:string>"
    assert df.where("p <> 'zzz'").count() == 10


@pytest.mark.parametrize("prefix", ["file:", "file://"])
def test_base_path_hadoop_file_uri(spark, tmp_path, prefix):
    base = tmp_path / "bracket[old]#%3A"
    part = base / "p=1"
    part.mkdir(parents=True)
    (part / "foo%3Abar.json").write_text('{"id":1}')
    reader = spark.read.option("basePath", f"{prefix}{base}")
    df = reader.json(f"{prefix}{tmp_path}/bracket*#%3A/p=*/foo%3Abar.json")
    assert df.collect() == [Row(id=1, p=1)]


def test_base_path_temporary_directory_as_root(spark, tmp_path):
    base = tmp_path / "_temporary"
    write_json_partition(base, "p=1")
    df = spark.read.option("basePath", str(base)).schema("id INT").json(str(base))
    assert df.columns == ["id"]
    assert df.collect() == []


def test_base_path_summary_prefix_in_data_filename(spark, tmp_path):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "_metadata=bar").write_text('{"id":1}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.collect() == [Row(id=1, p=1)]


def test_base_path_glob_streaming_metadata(spark, tmp_path):
    write_json_partition(tmp_path, "data")
    log = tmp_path / "_spark_metadata"
    log.mkdir()
    (log / "0").write_text('{"id":3}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path / "*"))
    assert df.collect() == [Row(id=1)]
    assert len(df.inputFiles()) == 1


def test_listing_brace_alternative_schema_sample(spark, tmp_path):
    a = tmp_path / "a"
    a.mkdir()
    for i in range(12):
        (a / f"{i}.json").write_text('{"id":1}')
    b = tmp_path / "b"
    b.mkdir()
    (b / "row.json").write_text('{"extra":"b"}')
    df = spark.read.json(str(tmp_path / "{a,b}"))
    assert df.where("extra IS NOT NULL").select("extra").collect() == [Row(extra="b")]


@pytest.mark.parametrize(
    ("ty", "token", "expected"), [("TINYINT", "128", -128), ("SMALLINT", "65535", -1), ("DECIMAL(3,0)", "1000", None)]
)
def test_base_path_numeric_partition_overflow(spark, tmp_path, ty, token, expected):
    write_json_partition(tmp_path, f"p={token}")
    df = spark.read.option("basePath", str(tmp_path)).schema(f"id INT, p {ty}").json(str(tmp_path))
    assert df.collect() == [Row(id=1, p=expected)]


@pytest.mark.parametrize(
    ("ty", "token", "expected"),
    [
        ("YEAR TO MONTH", "1-2", "1-2"),
        ("YEAR TO MONTH", "INTERVAL '1-2' YEAR TO MONTH", "1-2"),
        ("DAY TO SECOND", "1 02%3A03%3A04", "1 02:03:04"),
        ("HOUR TO MINUTE", "12%3A34", "12:34"),
    ],
)
def test_base_path_interval_partition(spark, tmp_path, ty, token, expected):
    write_json_partition(tmp_path, f"p={token}")
    df = spark.read.option("basePath", str(tmp_path)).schema(f"id INT, p INTERVAL {ty}").json(str(tmp_path))
    assert df.selectExpr("CAST(p AS STRING) AS value").collect() == [Row(value=f"INTERVAL '{expected}' {ty}")]


def test_base_path_leap_second_inference(spark, tmp_path):
    write_json_partition(tmp_path, "p=2025-01-01 10%3A20%3A60")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path))
    assert df.collect() == [Row(id=1, p="2025-01-01 10:20:60")]


@pytest.mark.parametrize(
    ("token", "expected"),
    [("1.2345E+2", Decimal("123.45")), ("1E-3", Decimal("0.00")), ("-0.005", Decimal("-0.01"))],
)
def test_base_path_explicit_decimal_rescaling(spark, tmp_path, token, expected):
    write_json_partition(tmp_path, f"p={token}")
    df = spark.read.option("basePath", str(tmp_path)).schema("id INT, p DECIMAL(8,2)").json(str(tmp_path))
    assert df.collect() == [Row(id=1, p=expected)]


def test_base_path_decimal_literal_precision(spark, tmp_path):
    value = "1" * 39
    write_json_partition(tmp_path, f"p={value}")
    reader = spark.read.option("basePath", str(tmp_path)).schema("id INT, p DECIMAL(3,0)")
    with pytest.raises(Exception, match=value):
        reader.json(str(tmp_path)).collect()


@pytest.mark.parametrize("ty", ["DATE", "TIMESTAMP", "TIMESTAMP_NTZ"])
@pytest.mark.parametrize("ansi", [True, False])
def test_base_path_invalid_datetime_partition(spark, tmp_path, ty, ansi):
    write_json_partition(tmp_path, "p=broken")
    reader = spark.read.option("basePath", str(tmp_path)).schema(f"id INT, p {ty}")
    key = "spark.sql.ansi.enabled"
    previous = spark.conf.get(key)
    try:
        spark.conf.set(key, str(ansi).lower())
        if ansi:
            with pytest.raises(Exception, match="broken"):
                reader.json(str(tmp_path)).collect()
        else:
            assert reader.json(str(tmp_path)).collect() == [Row(id=1, p=None)]
    finally:
        spark.conf.set(key, previous)


@pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
@pytest.mark.parametrize("excluded", [False, True])
def test_base_path_no_files_for_schema_inference(spark, tmp_path, file_format, excluded):
    if excluded:
        write_json_partition(tmp_path, "p=1")
    reader = spark.read.format(file_format).options(basePath=str(tmp_path), pathGlobFilter="*.missing")
    with pytest.raises(Exception, match="UNABLE_TO_INFER_SCHEMA"):
        reader.load(str(tmp_path)).collect()


@pytest.mark.parametrize("file_format", ["json", "csv", "parquet"])
def test_base_path_zero_byte_file_for_inference(spark, tmp_path, file_format):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "data").touch()
    reader = spark.read.format(file_format).option("basePath", str(tmp_path))
    if file_format == "parquet":
        # Older Spark releases try to read the empty footer during schema inference.
        with pytest.raises(Exception, match=r"UNABLE_TO_INFER_SCHEMA|CANNOT_READ_FILE_FOOTER"):
            reader.load(str(tmp_path)).collect()
    else:
        df = reader.load(str(tmp_path))
        assert df.schema.simpleString() == "struct<p:int>"
        assert df.collect() == []


@pytest.mark.skipif(os.name == "nt", reason="Backslash is a path separator on Windows")
@pytest.mark.parametrize(
    ("name", "pattern"), [("x*.json", r"x\*.json"), ("x[0].json", r"x\[0\].json"), ("x{a}.json", r"x\{a\}.json")]
)
def test_base_path_quoted_glob(spark, tmp_path, name, pattern):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / name).write_text('{"id":1}')
    df = spark.read.option("basePath", str(tmp_path)).json(str(part / pattern))
    assert df.collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize(("pattern", "expected"), [(r"a[a\-z].json", "-"), (r"a[a\]z].json", "]")])
def test_base_path_quoted_character_class(spark, tmp_path, pattern, expected):
    for char in ["-", "]", "b"]:
        (tmp_path / f"a{char}.json").write_text(f'{{"value":"{char}"}}')
    df = spark.read.options(basePath=str(tmp_path), pathGlobFilter=pattern).json(str(tmp_path))
    assert df.collect() == [Row(value=expected)]


def test_base_path_reader_timezone_validation(spark, tmp_path):
    write_json_partition(tmp_path, "p=1")
    reader = spark.read.options(basePath=str(tmp_path), timeZone="Mars/X")
    with pytest.raises(Exception, match="Mars/X"):
        reader.json(str(tmp_path)).collect()


@pytest.mark.parametrize("file_format", ["json", "csv"])
@pytest.mark.parametrize("recursive", [False, True])
def test_base_path_summary_only_text_format(spark, tmp_path, file_format, recursive):
    (tmp_path / "_metadata").write_text('{"only_in_summary":1}')
    reader = spark.read.format(file_format).options(basePath=str(tmp_path), recursiveFileLookup=recursive)
    df = reader.load(str(tmp_path))
    assert df.columns == []
    assert df.collect() == []
    with pytest.raises(Exception, match="UNABLE_TO_INFER_SCHEMA"):
        reader.option("pathGlobFilter", "*.missing").load(str(tmp_path)).collect()


def test_base_path_literal_percent_in_filename_filter(spark, tmp_path):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "row%20a.json").write_text('{"id":1}')
    (part / "row a.json").write_text('{"id":2}')
    df = spark.read.options(basePath=str(tmp_path), pathGlobFilter="row%20*.json").json(str(tmp_path))
    assert df.collect() == [Row(id=1, p=1)]
    assert len(df.inputFiles()) == 1


@pytest.mark.parametrize("per_read", [True, False])
def test_base_path_merge_schema_with_summary(spark, tmp_path, per_read):
    part = tmp_path / "p=1"
    part.mkdir()
    for i in range(10):
        pq.write_table(pa.table({"a": [i]}), part / f"{i}.parquet")
    pq.write_table(pa.table({"a": [10], "b": [42]}), part / "last.parquet")
    pq.write_metadata(pa.schema([("a", pa.int64()), ("c", pa.string())]), tmp_path / "_common_metadata")
    key = "spark.sql.parquet.mergeSchema"
    previous = spark.conf.get(key, None)
    try:
        spark.conf.set(key, str(not per_read).lower())
        reader = spark.read.option("basePath", str(tmp_path))
        if per_read:
            reader = reader.option("mergeSchema", True)
        df = reader.parquet(str(tmp_path))
        assert df.columns == ["a", "b", "c", "p"]
        assert df.where("a = 10").collect() == [Row(a=10, b=42, c=None, p=1)]
        assert reader.option("mergeSchema", False).parquet(str(tmp_path)).columns == ["a", "c", "p"]
    finally:
        if previous is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, previous)


def test_base_path_native_multicomponent_glob(spark, tmp_path):
    write_json_partition(tmp_path, "p=1")
    write_json_partition(tmp_path, "p=2")
    # pathlib supplies the native separator between the wildcard and the filename.
    pattern = tmp_path / "p=*" / "row.json"
    df = spark.read.option("basePath", str(tmp_path)).json(str(pattern))
    assert df.orderBy("p").collect() == [Row(id=1, p=1), Row(id=1, p=2)]


def test_base_path_relative_first_component_glob(spark):
    with TemporaryDirectory(prefix="basepath-", dir=".") as directory:
        root = Path(directory).resolve()
        write_json_partition(root, "p=1")
        name = root.name
        pattern = f"{name[:-1]}[{name[-1]}]/p=1"
        df = spark.read.option("basePath", str(root)).json(pattern)
        assert df.collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize("pattern", ["p={New York,New Jersey}", "p=**w*"])
def test_base_path_hadoop_partition_glob(spark, tmp_path, pattern):
    for name in ["New York", "New Jersey", "California"]:
        write_json_partition(tmp_path, f"p={name}")
    df = spark.read.option("basePath", str(tmp_path)).json(str(tmp_path / pattern))
    assert [r.p for r in df.orderBy("p").collect()] == ["New Jersey", "New York"]


@pytest.mark.parametrize("name", ["row.json", "*.json"])
@pytest.mark.parametrize("uri", [False, True])
def test_base_path_file_with_trailing_slash(spark, tmp_path, name, uri):
    write_json_partition(tmp_path, "p=1")
    root = tmp_path.as_uri() if uri else tmp_path
    path = f"{root}/p=1/{name}/"
    df = spark.read.option("basePath", str(tmp_path)).json(path)
    assert df.collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize("base_path", [False, True])
@pytest.mark.parametrize("with_summary", [False, True])
def test_parquet_merge_disabled_selects_one_footer(spark, tmp_path, base_path, with_summary):
    part = tmp_path / "p=1"
    part.mkdir()
    pq.write_table(pa.table({"b": [2]}), part / "b.parquet")
    pq.write_table(pa.table({"a": [1]}), part / "a.parquet")
    if with_summary:
        pq.write_metadata(pa.schema([("c", pa.int64())]), tmp_path / "_common_metadata")
    reader = spark.read.option("mergeSchema", False)
    if base_path:
        reader = reader.option("basePath", str(tmp_path))
    df = reader.parquet(str(tmp_path))
    assert df.columns == ["c" if with_summary else "a", "p"]


@pytest.mark.parametrize("pattern", ["row{,2}.json", "row{2,}.json", "row{}.json"])
@pytest.mark.parametrize("filename_filter", [True, False])
def test_base_path_empty_brace_alternative(spark, tmp_path, pattern, filename_filter):
    write_json_partition(tmp_path, "p=1")
    part = tmp_path / "p=1"
    (part / "row2.json").write_text('{"id":2}')
    reader = spark.read.option("basePath", str(tmp_path))
    if filename_filter:
        df = reader.option("pathGlobFilter", pattern).json(str(part))
    else:
        df = reader.json(str(part / pattern))
    assert [row.id for row in df.orderBy("id").collect()] == ([1] if "{}" in pattern else [1, 2])


@pytest.mark.parametrize("explicit_false", [False, True])
def test_base_path_single_footer_field_case(spark, tmp_path, explicit_false):
    for filename, name, value in [("a", "id", 1), ("b", "ID", 2)]:
        pq.write_table(pa.table({name: [value]}), tmp_path / f"{filename}.parquet")
    reader = spark.read.option("basePath", str(tmp_path))
    if explicit_false:
        reader = reader.option("mergeSchema", False)
    df = reader.parquet(str(tmp_path))
    assert df.columns == ["id"]
    assert df.orderBy("id").collect() == [Row(id=1), Row(id=2)]
    assert df.where("id = 2").collect() == [Row(id=2)]


@pytest.mark.parametrize("filename_filter", [False, True])
@pytest.mark.parametrize("negation", ["!", "^"])
def test_base_path_negated_character_class(spark, tmp_path, filename_filter, negation):
    write_json_partition(tmp_path, "p=1")
    part = tmp_path / "p=1"
    (part / "raw.json").write_text('{"id":2}')
    pattern = f"r[{negation}a]w.json"
    reader = spark.read.option("basePath", str(tmp_path))
    if filename_filter:
        df = reader.option("pathGlobFilter", pattern).json(str(part))
    else:
        df = reader.json(str(part / pattern))
    assert df.collect() == [Row(id=1, p=1)]


@pytest.mark.parametrize("merge_schema", [False, True])
def test_parquet_merge_schema_unpartitioned_directory(spark, tmp_path, merge_schema):
    nested = tmp_path / "archive"
    nested.mkdir()
    pq.write_table(pa.table({"archived": [2]}), nested / "old.parquet")
    current = tmp_path / "current.parquet"
    pq.write_table(pa.table({"id": [1]}), current)

    df = spark.read.option("mergeSchema", merge_schema).parquet(str(tmp_path))
    assert df.columns == ["id"]
    assert df.collect() == [Row(id=1)]
    assert df.inputFiles() == [current.as_uri()]


def test_parquet_merge_schema_partition_boundaries(spark, tmp_path):
    selected = tmp_path / "p=1"
    partition = selected / "day=2"
    partition.mkdir(parents=True)
    pq.write_table(pa.table({"id": [1]}), selected / "loose.parquet")
    pq.write_table(pa.table({"id": [2]}), partition / "data.parquet")

    reader = spark.read.option("mergeSchema", True)
    assert reader.parquet(str(selected)).collect() == [Row(id=2, day=2)]
    assert reader.parquet(str(partition / "data.parquet")).collect() == [Row(id=2)]
