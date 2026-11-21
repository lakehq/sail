# SPDX-License-Identifier: Apache-2.0

from datetime import date
from decimal import Decimal

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
