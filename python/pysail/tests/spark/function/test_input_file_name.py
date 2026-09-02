from pathlib import Path
from urllib.parse import unquote, urlparse

from pyspark.sql import functions as F  # noqa: N812


def _file_name(location: str) -> str:
    parsed = urlparse(location)
    path = unquote(parsed.path) if parsed.scheme else location
    return Path(path).name


def test_input_file_name_without_file_source(spark):
    dataframe = spark.range(2).select(
        F.input_file_name().alias("file_name"),
        F.input_file_block_start().alias("block_start"),
        F.input_file_block_length().alias("block_length"),
    )
    filtered = spark.range(2).where(F.input_file_name() == "").orderBy("id").collect()
    block_filtered = spark.range(2).where(F.input_file_block_start() == -1).orderBy("id").collect()
    grouped = spark.range(2).groupBy(F.input_file_name().alias("file_name")).count().collect()
    block_grouped = spark.range(2).groupBy(F.input_file_block_length().alias("block_length")).count().collect()

    assert [(row.file_name, row.block_start, row.block_length) for row in dataframe.collect()] == [
        ("", -1, -1),
        ("", -1, -1),
    ]
    assert dataframe.schema["file_name"].dataType.simpleString() == "string"
    assert not dataframe.schema["file_name"].nullable
    assert [row.id for row in filtered] == [0, 1]
    assert [row.id for row in block_filtered] == [0, 1]
    assert [(row.file_name, row["count"]) for row in grouped] == [("", 2)]
    assert [(row.block_length, row["count"]) for row in block_grouped] == [(-1, 2)]


def test_input_file_name_from_parquet_scan(spark, tmp_path):
    location = tmp_path / "input-file-name"
    spark.createDataFrame([(1,), (2,)], ["id"]).coalesce(1).write.mode("overwrite").parquet(str(location))
    [data_file] = location.glob("*.parquet")

    dataframe = spark.read.parquet(str(location)).select(
        "id",
        F.input_file_name().alias("file_name"),
        F.input_file_block_start().alias("block_start"),
        F.input_file_block_length().alias("block_length"),
    )
    rows = dataframe.orderBy("id").collect()

    assert [(row.id, _file_name(row.file_name), row.block_start, row.block_length) for row in rows] == [
        (1, data_file.name, 0, data_file.stat().st_size),
        (2, data_file.name, 0, data_file.stat().st_size),
    ]
    parsed = urlparse(rows[0].file_name)
    assert parsed.scheme == "file"
    assert Path(unquote(parsed.path)).resolve() == data_file.resolve()
    assert spark.read.parquet(str(location)).where(F.input_file_block_length() > 0).count() == len(rows)
    assert not dataframe.schema["file_name"].nullable
    assert not dataframe.schema["block_start"].nullable
    assert not dataframe.schema["block_length"].nullable


def test_input_file_metadata_from_delta_scan(spark, tmp_path):
    location = tmp_path / "input-file-name-delta"
    spark.createDataFrame([(1,), (2,)], ["id"]).coalesce(1).write.format("delta").mode("overwrite").save(str(location))
    [data_file] = location.rglob("*.parquet")

    rows = (
        spark.read.format("delta")
        .load(str(location))
        .select(
            "id",
            F.input_file_name().alias("file_name"),
            F.input_file_block_start().alias("block_start"),
            F.input_file_block_length().alias("block_length"),
        )
        .orderBy("id")
        .collect()
    )

    assert [(row.id, _file_name(row.file_name), row.block_start, row.block_length) for row in rows] == [
        (1, data_file.name, 0, data_file.stat().st_size),
        (2, data_file.name, 0, data_file.stat().st_size),
    ]
    parsed = urlparse(rows[0].file_name)
    assert parsed.scheme == "file"
    assert Path(unquote(parsed.path)).resolve() == data_file.resolve()
