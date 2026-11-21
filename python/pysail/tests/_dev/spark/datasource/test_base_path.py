# SPDX-License-Identifier: Apache-2.0

from datetime import date

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
