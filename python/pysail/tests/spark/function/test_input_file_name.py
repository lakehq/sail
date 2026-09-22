from collections import Counter
from itertools import pairwise
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F  # noqa: N812

PARQUET_SPLIT_ROW_COUNT = 16_384
PARQUET_SPLIT_ROW_GROUP_SIZE = 512
PARQUET_SPLIT_PAYLOAD_SIZE = 512
DATAFUSION_REPARTITION_FILE_MIN_SIZE = 1024 * 1024


@pytest.fixture
def parquet_metadata(spark, tmp_path):
    files = {}
    for name, ids in [("a.parquet", [1, 2]), ("b.parquet", [3, 4])]:
        path = tmp_path / name
        pq.write_table(pa.table({"id": ids, "arr": [[value, value + 1] for value in ids]}), path)
        files.update({value: (path.as_uri(), 0, path.stat().st_size) for value in ids})
    return spark.read.parquet(str(tmp_path)), files


@pytest.mark.parametrize("boundary", ["lambda", "coalesce", "union", "union_coalesce"])
def test_input_file_metadata_survives_projection_boundaries(spark, parquet_metadata, boundary):
    dataframe, expected = parquet_metadata
    if boundary == "lambda":
        dataframe = dataframe.selectExpr("id", "transform(arr, x -> x + id) AS arr")
    elif boundary == "coalesce":
        dataframe = dataframe.coalesce(1)
    else:
        dataframe = dataframe.select("id").union(spark.range(5, 7))
        expected.update({5: ("", -1, -1), 6: ("", -1, -1)})
        if boundary == "union_coalesce":
            dataframe = dataframe.coalesce(1)

    rows = dataframe.select(
        "*",
        F.input_file_name().alias("file_name"),
        F.input_file_block_start().alias("block_start"),
        F.input_file_block_length().alias("block_length"),
    ).collect()

    assert {row.id: (row.file_name, row.block_start, row.block_length) for row in rows} == expected
    if boundary == "lambda":
        assert {row.id: row.arr for row in rows} == {value: [2 * value, 2 * value + 1] for value in expected}


def test_input_file_metadata_inside_lambda(parquet_metadata):
    dataframe, expected = parquet_metadata
    rows = dataframe.selectExpr(
        "id",
        "transform(arr, x -> concat(input_file_name(), cast(x AS string))) AS names",
        "transform(arr, x -> input_file_block_start() + x) AS starts",
        "transform(arr, x -> input_file_block_length() + x) AS lengths",
        "transform(arr, x -> transform(array(x, x + 1), "
        "y -> concat(input_file_name(), cast(x + y + id AS string)))) AS nested",
    ).collect()

    for row in rows:
        name, start, length = expected[row.id]
        assert row.names == [f"{name}{row.id}", f"{name}{row.id + 1}"]
        assert row.starts == [start + row.id, start + row.id + 1]
        assert row.lengths == [length + row.id, length + row.id + 1]
        assert row.nested == [
            [f"{name}{2 * value + row.id}", f"{name}{2 * value + row.id + 1}"] for value in [row.id, row.id + 1]
        ]


@pytest.mark.parametrize(
    "generator", ["explode", "explode_outer", "posexplode", "posexplode_outer", "inline", "inline_outer"]
)
@pytest.mark.parametrize("select_after", [False, True])
def test_input_file_metadata_survives_generators(parquet_metadata, generator, select_after):
    dataframe, files = parquet_metadata
    dataframe = dataframe.selectExpr("id", "CASE id WHEN 2 THEN array() WHEN 4 THEN NULL ELSE arr END AS arr")
    if generator.startswith("inline"):
        expression = f"{generator}(transform(arr, x -> named_struct('item', cast(x AS BIGINT)))) AS item"
    elif generator.startswith("posexplode"):
        expression = f"{generator}(arr) AS (pos, item)"
    else:
        expression = f"{generator}(arr) AS item"
    metadata = [
        "input_file_name() AS file_name",
        "input_file_block_start() AS block_start",
        "input_file_block_length() AS block_length",
    ]
    if select_after:
        dataframe = dataframe.selectExpr("id", expression).selectExpr("*", *metadata)
    else:
        dataframe = dataframe.selectExpr("id", expression, *metadata)
    rows = dataframe.collect()

    expected = [
        (key, item, *file)
        for key, file in files.items()
        for item in ([key, key + 1] if key % 2 else ([None] if generator.endswith("outer") else []))
    ]
    assert sorted((row.id, row.item, row.file_name, row.block_start, row.block_length) for row in rows) == expected
    if generator.startswith("posexplode"):
        assert sorted((row.id, row.pos) for row in rows) == [
            (key, position)
            for key in files
            for position in ([0, 1] if key % 2 else ([None] if generator.endswith("outer") else []))
        ]


@pytest.mark.parametrize("function", ["input_file_name", "input_file_block_start", "input_file_block_length"])
@pytest.mark.parametrize("grouping", ["expression", "alias", "ordinal"])
def test_input_file_metadata_sql_grouping(spark, parquet_metadata, function, grouping):
    dataframe, files = parquet_metadata
    index = ["input_file_name", "input_file_block_start", "input_file_block_length"].index(function)
    expected = Counter(metadata[index] for metadata in files.values())
    group = {"expression": f"{function}()", "alias": "metadata", "ordinal": "1"}[grouping]
    dataframe.createOrReplaceTempView("input_file_grouping")
    try:
        rows = spark.sql(
            f"SELECT {function}() AS metadata, count(*) AS n, max({function}()) AS maximum "  # noqa: S608
            f"FROM input_file_grouping GROUP BY {group}"
        ).collect()
        assert sorted(tuple(row) for row in rows) == sorted((value, count, value) for value, count in expected.items())
    finally:
        spark.catalog.dropTempView("input_file_grouping")


@pytest.mark.parametrize("function", ["input_file_name", "input_file_block_start", "input_file_block_length"])
@pytest.mark.parametrize("window", ["partition", "order", "argument"])
def test_input_file_metadata_in_window(parquet_metadata, function, window):
    dataframe, files = parquet_metadata
    index = ["input_file_name", "input_file_block_start", "input_file_block_length"].index(function)
    metadata = {key: value[index] for key, value in files.items()}
    if window == "partition":
        expression = f"row_number() OVER (PARTITION BY {function}() ORDER BY id)"
        expected = {
            key: sum(value == metadata[previous] for previous in files if previous <= key)
            for key, value in metadata.items()
        }
    elif window == "order":
        expression = f"row_number() OVER (ORDER BY {function}(), id)"
        expected = {
            key: position for position, key in enumerate(sorted(files, key=lambda key: (metadata[key], key)), 1)
        }
    else:
        expression = f"lag({function}()) OVER (ORDER BY id)"
        expected = {key: metadata.get(key - 1) for key in files}

    rows = dataframe.selectExpr("id", f"{expression} AS value").collect()
    assert {row.id: row.value for row in rows} == expected


@pytest.mark.parametrize("boundary", ["repartition", "sort", "aggregate"])
def test_input_file_metadata_after_shuffle_uses_defaults(parquet_metadata, boundary):
    dataframe, expected = parquet_metadata
    if boundary == "repartition":
        dataframe = dataframe.repartition(2)
    elif boundary == "sort":
        dataframe = dataframe.orderBy("id")
    else:
        dataframe = dataframe.groupBy("id").count()
    rows = dataframe.select(
        "id", F.input_file_name(), F.input_file_block_start(), F.input_file_block_length()
    ).collect()
    assert sorted(tuple(row) for row in rows) == [(key, "", -1, -1) for key in expected]


def test_input_file_metadata_materialized_before_shuffle(parquet_metadata):
    dataframe, expected = parquet_metadata
    rows = (
        dataframe.select("id", F.input_file_name(), F.input_file_block_start(), F.input_file_block_length())
        .repartition(2)
        .collect()
    )
    assert sorted(tuple(row) for row in rows) == [(key, *value) for key, value in expected.items()]


def test_input_file_name_escapes_uri_path(spark, tmp_path):
    directory = tmp_path / "space #hash %percent"
    directory.mkdir()
    path = directory / "file #100%.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    rows = spark.read.parquet(str(path)).select(F.input_file_name()).collect()
    assert [row[0] for row in rows] == [path.as_uri()]


@pytest.mark.parametrize("function", ["input_file_name", "input_file_block_start", "input_file_block_length"])
@pytest.mark.parametrize("nested_union", [False, True])
def test_input_file_metadata_rejects_multiple_file_sources(spark, parquet_metadata, function, nested_union):
    dataframe, _ = parquet_metadata
    joined = dataframe.alias("left").join(dataframe.alias("right"), "id").select("id")
    if nested_union:
        joined = joined.union(spark.range(5, 7))
    with pytest.raises(Exception, match="MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION"):
        joined.selectExpr(f"{function}()").collect()


def test_input_file_metadata_can_be_materialized_before_join(parquet_metadata):
    dataframe, expected = parquet_metadata
    left = dataframe.select("id", F.input_file_name().alias("left_file"))
    right = dataframe.select("id", F.input_file_name().alias("right_file"))
    rows = left.join(right, "id").collect()
    assert {row.id: (row.left_file, row.right_file) for row in rows} == {
        key: (value[0], value[0]) for key, value in expected.items()
    }


def test_input_file_metadata_from_union_of_files(parquet_metadata):
    dataframe, expected = parquet_metadata
    rows = (
        dataframe.where("id <= 2")
        .union(dataframe.where("id > 2"))
        .select("id", F.input_file_name(), F.input_file_block_start(), F.input_file_block_length())
        .collect()
    )
    assert sorted(tuple(row) for row in rows) == [(key, *value) for key, value in expected.items()]


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
