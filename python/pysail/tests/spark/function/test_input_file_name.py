from pathlib import Path
from urllib.parse import unquote, urlparse

from pyspark.sql import functions as F  # noqa: N812


def _file_name(location: str) -> str:
    parsed = urlparse(location)
    path = unquote(parsed.path) if parsed.scheme else location
    return Path(path).name


def test_input_file_name_without_file_source(spark):
    dataframe = spark.range(2).select(F.input_file_name().alias("file_name"))
    filtered = spark.range(2).where(F.input_file_name() == "").orderBy("id").collect()
    grouped = spark.range(2).groupBy(F.input_file_name().alias("file_name")).count().collect()

    assert [row.file_name for row in dataframe.collect()] == ["", ""]
    assert dataframe.schema["file_name"].dataType.simpleString() == "string"
    assert not dataframe.schema["file_name"].nullable
    assert [row.id for row in filtered] == [0, 1]
    assert [(row.file_name, row["count"]) for row in grouped] == [("", 2)]


def test_input_file_name_from_parquet_scan(spark, tmp_path):
    location = tmp_path / "input-file-name"
    spark.createDataFrame([(1,), (2,)], ["id"]).coalesce(1).write.mode("overwrite").parquet(str(location))
    [data_file] = location.glob("*.parquet")

    dataframe = spark.read.parquet(str(location)).select("id", F.input_file_name().alias("file_name"))
    rows = dataframe.orderBy("id").collect()

    assert [(row.id, _file_name(row.file_name)) for row in rows] == [
        (1, data_file.name),
        (2, data_file.name),
    ]
    assert not dataframe.schema["file_name"].nullable
