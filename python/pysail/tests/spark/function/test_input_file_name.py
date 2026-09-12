from itertools import pairwise
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F  # noqa: N812

PARQUET_SPLIT_ROW_COUNT = 16_384
PARQUET_SPLIT_ROW_GROUP_SIZE = 512
PARQUET_SPLIT_PAYLOAD_SIZE = 512
DATAFUSION_REPARTITION_FILE_MIN_SIZE = 1024 * 1024


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


def test_input_file_block_metadata_from_parquet_splits(spark, tmp_path):
    data_file = tmp_path / "split-input-file.parquet"
    payloads = [f"{index:08d}-{'x' * PARQUET_SPLIT_PAYLOAD_SIZE}" for index in range(PARQUET_SPLIT_ROW_COUNT)]
    pq.write_table(
        pa.table({"id": range(PARQUET_SPLIT_ROW_COUNT), "payload": payloads}),
        data_file,
        row_group_size=PARQUET_SPLIT_ROW_GROUP_SIZE,
        compression="NONE",
        use_dictionary=False,
    )
    file_size = data_file.stat().st_size
    assert file_size > DATAFUSION_REPARTITION_FILE_MIN_SIZE

    metadata_rows = (
        spark.read.parquet(str(data_file))
        .select(
            F.input_file_name().alias("file_name"),
            F.input_file_block_start().alias("block_start"),
            F.input_file_block_length().alias("block_length"),
        )
        .groupBy("file_name", "block_start", "block_length")
        .count()
        .orderBy("block_start")
        .collect()
    )

    assert len(metadata_rows) > 1
    assert sum(row["count"] for row in metadata_rows) == PARQUET_SPLIT_ROW_COUNT
    assert all(Path(unquote(urlparse(row.file_name).path)).resolve() == data_file.resolve() for row in metadata_rows)

    ranges = [(row.block_start, row.block_length) for row in metadata_rows]
    assert ranges[0][0] == 0
    assert any(block_start > 0 for block_start, _ in ranges)
    assert all(block_length > 0 for _, block_length in ranges)
    assert all(
        block_start + block_length == next_start for (block_start, block_length), (next_start, _) in pairwise(ranges)
    )
    assert sum(block_length for _, block_length in ranges) == file_size


def test_input_file_metadata_from_delta_scan(spark, tmp_path):
    location = tmp_path / "input-file-name-delta"
    first_ids = [1, 2]
    second_ids = [3, 4, 5]
    spark.createDataFrame([(value,) for value in first_ids], ["id"]).coalesce(1).write.format("delta").mode(
        "overwrite"
    ).save(str(location))
    spark.createDataFrame([(value,) for value in second_ids], ["id"]).coalesce(1).write.format("delta").mode(
        "append"
    ).save(str(location))
    data_files = {path.resolve() for path in location.glob("*.parquet")}

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

    assert [row.id for row in rows] == [*first_ids, *second_ids]
    row_files = {}
    for row in rows:
        parsed = urlparse(row.file_name)
        data_file = Path(unquote(parsed.path)).resolve()
        assert parsed.scheme == "file"
        assert data_file in data_files
        assert row.block_start == 0
        assert row.block_length == data_file.stat().st_size
        row_files[row.id] = data_file

    first_file = {row_files[value] for value in first_ids}
    second_file = {row_files[value] for value in second_ids}
    assert len(first_file) == 1
    assert len(second_file) == 1
    assert first_file.isdisjoint(second_file)
    assert first_file | second_file == data_files
