from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row
from pyspark.sql import functions as sf

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.parametrize("partitions", [1, 4])
def test_parquet_writer_preserves_execution_partitions_and_order(spark, tmp_path, partitions):
    path = tmp_path / "output.parquet"
    rows = 40000
    frame = spark.range(0, rows, 1, partitions).sortWithinPartitions("id", ascending=False)
    frame.write.parquet(str(path))

    files = sorted(path.glob("*.parquet"))
    assert len(files) == partitions
    actual = []
    for file in files:
        parquet = pq.ParquetFile(file)
        values = parquet.read().column("id").to_pylist()
        assert values == sorted(values, reverse=True)
        assert len(values) == rows // partitions
        actual.extend(values)
    assert sorted(actual) == list(range(rows))
    assert spark.read.parquet(str(path)).count() == rows


def test_parquet_writer_append_keeps_each_tasks_files(spark, tmp_path):
    path = tmp_path / "append"
    frame = spark.range(0, 12000, 1, 4).repartition(3)
    frame.write.parquet(str(path))
    first_files = set(path.glob("*.parquet"))
    assert len(first_files) == 3  # noqa: PLR2004
    frame.write.mode("append").parquet(str(path))
    files = set(path.glob("*.parquet"))
    assert first_files < files
    assert len(files) == 6  # noqa: PLR2004
    assert spark.read.parquet(str(path)).count() == 24000  # noqa: PLR2004


def test_parquet_writer_partition_directories_and_file_schema(spark, tmp_path):
    path = tmp_path / "partitioned"
    values = [None, "", "a/b", "a=b", "a%b", "中文"] + [f"p{i}" for i in range(64)]
    rows = [(i, values[i % len(values)], str(i % 2)) for i in range(1400)]
    frame = spark.createDataFrame(rows, "id BIGINT, part STRING, sub STRING").coalesce(1)
    frame.write.partitionBy("part", "sub").parquet(str(path))

    actual = []
    for file in path.rglob("*.parquet"):
        parquet = pq.ParquetFile(file)
        assert parquet.schema_arrow.names == ["id"]
        segments = file.relative_to(path).parts[:-1]
        partitions = dict(unquote(segment).split("=", 1) for segment in segments)
        part = partitions["part"]
        if part == "__HIVE_DEFAULT_PARTITION__":
            part = None
        actual.extend((row["id"], part, partitions["sub"]) for row in parquet.read().to_pylist())
    expected = [(i, part or None, sub) for i, part, sub in rows]
    assert sorted(actual) == expected
    assert (path / "part=a%2Fb").is_dir()
    assert (path / "part=a%3Db").is_dir()
    assert (path / "part=a%25b").is_dir()
    read_back = spark.read.parquet(str(path)).selectExpr("id", "part", "CAST(sub AS STRING) AS sub")
    assert read_back.orderBy("id").collect() == [Row(id=i, part=part, sub=sub) for i, part, sub in expected]


@pytest.mark.parametrize("part", ["a=b", "a:b", "a'b", "a~b", "a>b", "a<b", "a|b", "a`b", "a}b"])
def test_parquet_writer_partition_equality_keeps_matching_rows(spark, tmp_path, part):
    path = str(tmp_path / "partitioned")
    expected = [Row(id=1, p=part)]
    spark.createDataFrame(expected).write.partitionBy("p").parquet(path)

    frame = spark.read.parquet(path)
    assert frame.collect() == expected
    assert frame.where(sf.col("p") == sf.lit(part)).collect() == expected


@pytest.mark.parametrize(("part", "escaped"), [("a=b", "a%3Db"), ("a~b", "a%7Eb")])
def test_parquet_partition_filter_reads_both_directory_spellings(spark, tmp_path, part, escaped):
    for i, name in enumerate([part, escaped]):
        directory = tmp_path / "region=west" / f"p={name}"
        directory.mkdir(parents=True)
        pq.write_table(pa.table({"id": [i]}), directory / "data.parquet")
    frame = spark.read.parquet(str(tmp_path))
    actual = frame.where((sf.col("region") == "west") & (sf.col("p") == part)).orderBy("id").collect()
    assert actual == [Row(id=i, region="west", p=part) for i in range(2)]


def test_parquet_writer_binary_partition_preserves_bytes(spark, tmp_path):
    path = str(tmp_path / "binary_partition")
    frame = spark.createDataFrame([(1, b"abc")], "id BIGINT, p BINARY")
    frame.write.partitionBy("p").parquet(path)
    assert spark.read.schema(frame.schema).parquet(path).collect() == frame.collect()


@pytest.mark.parametrize("expression", ["array(1, 2)", "map('a', 1)", "named_struct('a', 1)", "parse_json('{}')"])
def test_parquet_writer_rejects_complex_partition_types(spark, tmp_path, expression):
    frame = spark.sql(f"SELECT 1 AS id, {expression} AS p")
    with pytest.raises(AnalysisException):
        frame.write.partitionBy("p").parquet(str(tmp_path / "output"))
    assert list(tmp_path.rglob("*.parquet")) == []


@pytest.mark.skipif(is_jvm_spark(), reason="Sail does not support interval partition decoding")
@pytest.mark.parametrize("expression", ["INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '1 02:03:04.123456' DAY TO SECOND"])
def test_parquet_writer_rejects_unreadable_interval_partitions(spark, tmp_path, expression):
    frame = spark.sql(f"SELECT 1 AS id, {expression} AS p")
    with pytest.raises(AnalysisException, match="Unsupported Parquet partition type"):
        frame.write.partitionBy("p").parquet(str(tmp_path / "output"))
    assert list(tmp_path.rglob("*.parquet")) == []


def test_parquet_writer_variant_roundtrip(spark, tmp_path):
    path = str(tmp_path / "variant")
    frame = spark.sql("SELECT id, parse_json('{\"a\":[1,null,true]}') AS v FROM range(0, 8, 1, 4)")
    frame.write.parquet(path)
    expected = frame.selectExpr("id", "to_json(v) AS json").orderBy("id").collect()
    actual = spark.read.parquet(path).selectExpr("id", "to_json(v) AS json").orderBy("id").collect()
    assert actual == expected
    if not is_jvm_spark():
        for file in (tmp_path / "variant").glob("*.parquet"):
            metadata = pq.ParquetFile(file).schema_arrow.field("v").metadata
            assert metadata[b"ARROW:extension:name"] == b"arrow.parquet.variant"


def test_parquet_writer_nullable_numeric_partitions(spark, tmp_path):
    path = tmp_path / "numeric_partitions"
    rows = [(1, None, None), (2, 0, 0.0), (3, 5, 3.0)]
    spark.createDataFrame(rows, "id BIGINT, p INT, q DOUBLE").coalesce(1).write.partitionBy("p", "q").parquet(str(path))
    directories = {file.parent.relative_to(path).as_posix() for file in path.rglob("*.parquet")}
    assert directories == {
        "p=__HIVE_DEFAULT_PARTITION__/q=__HIVE_DEFAULT_PARTITION__",
        "p=0/q=0.0",
        "p=5/q=3.0",
    }
    actual = spark.read.parquet(str(path)).selectExpr("id", "CAST(p AS INT) AS p", "CAST(q AS DOUBLE) AS q")
    assert actual.orderBy("id").collect() == [Row(id=i, p=p, q=q) for i, p, q in rows]
    typed = spark.read.schema("id BIGINT, p INT, q DOUBLE").parquet(str(path))
    assert typed.where("p IS NULL").collect() == [Row(id=1, p=None, q=None)]
    assert typed.where("p != 5").collect() == [Row(id=2, p=0, q=0.0)]
    assert typed.where("q = 3.0").collect() == [Row(id=3, p=5, q=3.0)]


def test_parquet_writer_escapes_partition_column_names(spark, tmp_path):
    path = tmp_path / "escaped_column"
    frame = spark.createDataFrame([(1, "a=b"), (2, "plain")], ["id", "part=key/"])
    frame.write.partitionBy("part=key/").parquet(str(path))
    assert (path / "part%3Dkey%2F=a%3Db").is_dir()
    read_back = spark.read.parquet(str(path))
    assert read_back.columns == ["id", "part=key/"]
    assert read_back.orderBy("id").collect() == frame.orderBy("id").collect()
    assert read_back.where("`part=key/` = 'plain'").collect() == [(2, "plain")]


@pytest.mark.parametrize("partitioned", [False, True])
def test_parquet_writer_empty_input(spark, tmp_path, partitioned):
    path = tmp_path / "empty"
    frame = spark.range(0, 0, 1, 4).selectExpr("id", "CAST(NULL AS STRING) AS part")
    writer = frame.write
    if partitioned:
        writer = writer.partitionBy("part")
    writer.parquet(str(path))

    files = list(path.rglob("*.parquet"))
    if partitioned:
        assert files == []
    else:
        assert len(files) == 1
        parquet = pq.ParquetFile(files[0])
        assert parquet.metadata.num_rows == 0
        assert parquet.schema_arrow.names == ["id", "part"]
        assert spark.read.parquet(str(path)).collect() == []


@pytest.mark.skipif(is_jvm_spark(), reason="Sail Parquet writer options")
def test_parquet_writer_keeps_encoding_options_and_projected_arrow_schema(spark, tmp_path):
    path = tmp_path / "options"
    frame = spark.range(0, 75, 1, 1).selectExpr("id", "'p' AS part")
    (
        frame.write.partitionBy("part")
        .option("max_row_group_size", "7")
        .option("compression", "zstd(3)")
        .option("dictionary_enabled", "false")
        .parquet(str(path))
    )
    files = list(path.rglob("*.parquet"))
    assert len(files) == 1
    parquet = pq.ParquetFile(files[0])
    assert parquet.schema_arrow.names == ["id"]
    assert parquet.metadata.num_row_groups == 11  # noqa: PLR2004
    for index in range(parquet.metadata.num_row_groups):
        column = parquet.metadata.row_group(index).column(0)
        assert column.compression == "ZSTD"
        assert "RLE_DICTIONARY" not in column.encodings
        assert column.statistics is not None
    assert spark.read.parquet(str(path)).orderBy("id").collect() == [Row(id=i, part="p") for i in range(75)]

    path = tmp_path / "no_arrow_metadata"
    frame.write.option("skip_arrow_metadata", "true").parquet(str(path))
    for file in path.glob("*.parquet"):
        assert b"ARROW:schema" not in (pq.ParquetFile(file).metadata.metadata or {})
