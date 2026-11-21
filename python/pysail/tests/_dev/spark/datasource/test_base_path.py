# SPDX-License-Identifier: Apache-2.0

from datetime import date, time
from decimal import Decimal

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


def test_base_path_local_glob_percent_escape(spark, tmp_path):
    part = tmp_path / "p=1"
    part.mkdir()
    (part / "foo%3Abar.json").write_text('{"id":1}')
    (part / "foo:bar.json").write_text('{"id":2}')
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
