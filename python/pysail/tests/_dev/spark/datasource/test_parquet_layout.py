# SPDX-License-Identifier: Apache-2.0

import re

import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark

_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_PART_FILE_RE = re.compile(
    rf"^part-(?P<partition>\d{{5,}})-(?P<uuid>{_UUID})-c(?P<counter>\d{{3}})(?:\.[a-z0-9]+)?\.parquet$"
)


def data_files(path):
    """Returns the data files under the path, relative to the path."""
    return sorted(p.relative_to(path) for p in path.rglob("*") if p.is_file() and not p.name.startswith(("_", ".")))


def write_ids(path):
    ids = set()
    for file in data_files(path):
        m = _PART_FILE_RE.match(file.name)
        assert m is not None, file
        ids.add(m.group("uuid"))
    return ids


def assert_success_marker(path):
    marker = path / "_SUCCESS"
    assert marker.is_file()
    assert marker.stat().st_size == 0
    assert [p for p in path.rglob("_SUCCESS") if p != marker] == []


def test_parquet_write_file_names(spark, tmp_path):
    path = tmp_path / "names"
    spark.range(0, 100, 1, 4).write.parquet(str(path))
    files = data_files(path)
    partitions = sorted(_PART_FILE_RE.match(f.name).group("partition") for f in files)
    assert partitions == ["00000", "00001", "00002", "00003"]
    assert {_PART_FILE_RE.match(f.name).group("counter") for f in files} == {"000"}
    assert len(write_ids(path)) == 1
    assert_success_marker(path)
    assert spark.read.parquet(str(path)).count() == 100


def test_parquet_write_single_partition(spark, tmp_path):
    path = tmp_path / "single"
    spark.range(0, 100, 1, 4).coalesce(1).write.parquet(str(path))
    assert len(data_files(path)) == 1
    assert_success_marker(path)


def test_parquet_append_adds_files_of_another_write(spark, tmp_path):
    path = tmp_path / "append"
    spark.range(0, 10, 1, 2).write.parquet(str(path))
    before = {f: (path / f).read_bytes() for f in data_files(path)}
    (first,) = write_ids(path)

    spark.range(10, 20, 1, 2).write.mode("append").parquet(str(path))
    after = data_files(path)
    assert len(after) == 4
    assert len(write_ids(path)) == 2
    assert first in write_ids(path)
    for file, content in before.items():
        assert (path / file).read_bytes() == content
    assert_success_marker(path)
    assert sorted(r.id for r in spark.read.parquet(str(path)).collect()) == list(range(20))


def test_parquet_overwrite_leaves_only_the_new_write(spark, tmp_path):
    path = tmp_path / "overwrite"
    spark.range(0, 10, 1, 2).write.parquet(str(path))
    (first,) = write_ids(path)
    spark.range(0, 3).write.mode("append").json(str(path))
    (path / "stray.txt").write_text("stray")
    (path / ".hidden").write_text("hidden")
    (path / "nested").mkdir()
    (path / "nested" / "old.parquet").write_bytes(b"old")

    spark.range(10, 20, 1, 2).write.mode("overwrite").parquet(str(path))
    files = [p.relative_to(path) for p in path.rglob("*") if p.is_file()]
    assert sorted(f for f in files if f.name != "_SUCCESS") == data_files(path)
    assert len(data_files(path)) == 2
    (second,) = write_ids(path)
    assert second != first
    assert_success_marker(path)
    assert sorted(r.id for r in spark.read.parquet(str(path)).collect()) == list(range(10, 20))


def test_parquet_max_records_per_file(spark, tmp_path):
    path = tmp_path / "records"
    spark.range(0, 5, 1, 1).write.option("maxRecordsPerFile", 2).parquet(str(path))
    files = data_files(path)
    assert [pq.read_metadata(path / f).num_rows for f in files] == [2, 2, 1]
    assert [_PART_FILE_RE.match(f.name).group("counter") for f in files] == ["000", "001", "002"]
    assert len(write_ids(path)) == 1
    assert_success_marker(path)
    assert sorted(r.id for r in spark.read.parquet(str(path)).collect()) == list(range(5))


def test_parquet_write_without_success_file(spark, tmp_path):
    path = tmp_path / "no_marker"
    df = spark.range(0, 5, 1, 1)
    df.write.option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").parquet(str(path))
    assert len(data_files(path)) == 1
    assert not (path / "_SUCCESS").exists()
    assert sorted(r.id for r in spark.read.parquet(str(path)).collect()) == list(range(5))


def test_parquet_partitioned_write_layout(spark, tmp_path):
    path = tmp_path / "partitioned"
    df = spark.range(0, 20, 1, 2).withColumn("k", F.col("id") % 2)
    df.write.partitionBy("k").parquet(str(path))
    files = data_files(path)
    assert {str(f.parent) for f in files} == {"k=0", "k=1"}
    assert len(write_ids(path)) == 1
    assert_success_marker(path)
    # The schema gives the type of the partition column, which is otherwise read as a string.
    actual = sorted((r.id, r.k) for r in spark.read.schema("id LONG, k LONG").parquet(str(path)).collect())
    assert actual == [(i, i % 2) for i in range(20)]


def test_parquet_empty_write(spark, tmp_path):
    path = tmp_path / "empty"
    spark.createDataFrame([], schema="id LONG, value STRING").write.parquet(str(path))
    assert_success_marker(path)
    df = spark.read.parquet(str(path))
    assert df.schema.simpleString() == "struct<id:bigint,value:string>"
    assert df.collect() == []


def test_parquet_empty_partitioned_write(spark, tmp_path):
    path = tmp_path / "empty_partitioned"
    spark.createDataFrame([], schema="id LONG, k INT").write.partitionBy("k").parquet(str(path))
    assert_success_marker(path)
    assert data_files(path) == []


@pytest.mark.parametrize(
    ("compression", "suffix", "codec"),
    [
        ("zstd", ".zstd.parquet", "ZSTD"),
        ("gzip", ".gz.parquet", "GZIP"),
        ("snappy", ".snappy.parquet", "SNAPPY"),
        # PyArrow reports the LZ4_RAW codec as LZ4.
        ("lz4_raw", ".lz4raw.parquet", "LZ4"),
    ],
)
def test_parquet_write_compression_in_file_name(spark, tmp_path, compression, suffix, codec):
    path = tmp_path / "compression"
    spark.range(0, 10, 1, 1).write.option("compression", compression).parquet(str(path))
    (file,) = data_files(path)
    assert _PART_FILE_RE.match(file.name) is not None
    assert file.name.endswith(f"-c000{suffix}")
    assert pq.ParquetFile(path / file).metadata.row_group(0).column(0).compression == codec


def test_parquet_write_uncompressed_file_name(spark, tmp_path):
    path = tmp_path / "uncompressed"
    spark.range(0, 10, 1, 1).write.option("compression", "uncompressed").parquet(str(path))
    (file,) = data_files(path)
    assert re.fullmatch(rf"part-00000-{_UUID}-c000\.parquet", file.name) is not None


def test_parquet_sorted_write_reads_back_in_file_name_order(spark, tmp_path):
    path = tmp_path / "sorted"
    spark.range(0, 1000, 1, 4).orderBy(F.col("id").desc()).write.parquet(str(path))
    values = []
    for file in data_files(path):
        values.extend(pq.read_table(path / file).column("id").to_pylist())
    assert values == list(range(999, -1, -1))
    assert_success_marker(path)


def test_parquet_failed_write_has_no_success_marker(spark, tmp_path):
    path = tmp_path / "failed"
    df = spark.range(0, 10, 1, 2).select(F.raise_error(F.lit("write failure")).alias("x"))
    with pytest.raises(Exception, match="write failure"):
        df.write.parquet(str(path))
    assert not (path / "_SUCCESS").exists()


@pytest.mark.skipif(is_jvm_spark(), reason="Sail writes its own version to the file metadata")
def test_parquet_write_file_metadata(spark, tmp_path):
    path = tmp_path / "metadata"
    spark.createDataFrame([Row(id=1, value="a")]).coalesce(1).write.parquet(str(path))
    (file,) = data_files(path)
    metadata = pq.ParquetFile(path / file).metadata
    assert metadata.created_by.startswith("sail version")
    assert b"ARROW:schema" in metadata.metadata
    assert metadata.row_group(0).column(0).compression == "ZSTD"
