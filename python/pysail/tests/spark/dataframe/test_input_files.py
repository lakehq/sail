"""Tests for the DataFrame ``inputFiles`` snapshot of constituent files."""

import json
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow.parquet as pq
import pytest
from pyspark.sql.utils import AnalysisException


def test_input_files_single_file(spark, tmp_path):
    """Return the single file backing a one-partition dataset."""
    spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}]).repartition(1).write.json(
        str(tmp_path), mode="overwrite"
    )
    df = spark.read.format("json").load(str(tmp_path))

    files = df.inputFiles()
    assert len(files) == 1
    assert all(isinstance(f, str) for f in files)
    assert files[0].endswith(".json")


@pytest.mark.parametrize("partitions", [1, 3, 10])
@pytest.mark.parametrize("file_format", ["json", "parquet"])
def test_input_files_multiple_files(spark, tmp_path, partitions, file_format):
    """Preserve explicit partitions even above the default write parallelism."""
    spark.range(1, 100, 1, partitions).write.format(file_format).save(str(tmp_path), mode="overwrite")
    df = spark.read.format(file_format).load(str(tmp_path))

    assert len(df.inputFiles()) == partitions
    assert sorted(row.id for row in df.collect()) == list(range(1, 100))


def _physical_file_rows(path, file_format):
    if file_format == "parquet":
        return pq.read_table(path).column("id").to_pylist()
    return [json.loads(line)["id"] for line in path.read_text().splitlines()]


@pytest.mark.parametrize("file_format", ["json", "parquet"])
@pytest.mark.parametrize("partitioning", ["range", "coalesce", "repartition", "multi_batch"])
def test_input_files_preserves_write_partitions(spark, tmp_path, file_format, partitioning):
    """File contents retain partition identity, rather than merely matching the file count."""
    if partitioning == "multi_batch":
        count = 100_000
        dataframe = spark.range(0, count, 1, 1)
        expected = [list(range(count))]
    else:
        count = 20
        dataframe = spark.range(0, count, 1, 10)
        if partitioning == "coalesce":
            dataframe = dataframe.coalesce(2)
            expected = [list(range(10)), list(range(10, count))]
        elif partitioning == "repartition":
            dataframe = dataframe.selectExpr("id", "cast(id / 2 as int) AS key").repartition(10, "key")
            # Record the input partition membership independently of the writer.
            groups = defaultdict(list)
            for row in dataframe.selectExpr("id", "spark_partition_id() AS partition_id").collect():
                groups[row.partition_id].append(row.id)
            expected = sorted(sorted(values) for values in groups.values())
        else:
            expected = [list(range(start, start + 2)) for start in range(0, count, 2)]

    dataframe.write.format(file_format).save(str(tmp_path), mode="overwrite")
    result = spark.read.format(file_format).load(str(tmp_path))
    paths = {Path(unquote(urlparse(uri).path)) for uri in result.inputFiles()}
    physical = {path for path in tmp_path.iterdir() if not path.name.startswith(("_", "."))}
    assert paths == physical
    parts = sorted(sorted(_physical_file_rows(path, file_format)) for path in paths)
    assert sorted(value for part in parts for value in part) == list(range(count))
    # Spark may also write an empty file for an empty first partition.
    assert [part for part in parts if part] == expected


@pytest.mark.parametrize("file_format", ["json", "parquet"])
@pytest.mark.parametrize("partition_by", [False, True])
def test_input_files_append_and_overwrite(spark, tmp_path, file_format, partition_by):
    """An overwrite deletes old files once; append retains all prior partition files."""

    def write(start, end, mode):
        dataframe = spark.range(start, end, 1, 3).selectExpr("id", "id % 2 AS key")
        writer = dataframe.write.format(file_format).mode(mode)
        if partition_by:
            writer = writer.partitionBy("key")
        writer.save(str(tmp_path))

    def read():
        dataframe = spark.read.schema("id long, key long").format(file_format).load(str(tmp_path))
        files = set(dataframe.inputFiles())
        physical = {path for path in tmp_path.rglob("*") if path.is_file() and not path.name.startswith(("_", "."))}
        assert {Path(unquote(urlparse(uri).path)) for uri in files} == physical
        rows = dataframe.collect()
        assert all(row.key == row.id % 2 for row in rows)
        return files, sorted(row.id for row in rows)

    write(0, 12, "overwrite")
    original, ids = read()
    assert ids == list(range(12))
    write(12, 24, "append")
    appended, ids = read()
    assert original < appended
    assert ids == list(range(24))
    write(24, 36, "overwrite")
    replaced, ids = read()
    assert replaced.isdisjoint(appended)
    assert ids == list(range(24, 36))
    assert len(replaced) == (6 if partition_by else 3)


def test_input_files_union_is_deduplicated(spark, tmp_path):
    """Files shared by both sides of a union are reported only once."""
    partitions = 3
    spark.range(0, 10, 1, partitions).write.json(str(tmp_path), mode="overwrite")
    df = spark.read.format("json").load(str(tmp_path))

    # The union references the same source twice; the files must not be double-counted.
    assert len(df.union(df).inputFiles()) == partitions


def test_input_files_excludes_hidden_markers(spark, tmp_path):
    """Hidden files such as ``_SUCCESS`` and ``.crc`` are not part of the dataset."""
    spark.range(3).repartition(1).write.json(str(tmp_path), mode="overwrite")
    (tmp_path / "_SUCCESS").touch()
    (tmp_path / ".crc").touch()
    df = spark.read.format("json").load(str(tmp_path))

    files = df.inputFiles()
    assert len(files) == 1
    assert all("_SUCCESS" not in f and not f.endswith("/.crc") for f in files)


def test_input_files_are_percent_encoded(spark, tmp_path):
    """Returned URIs percent-encode reserved characters such as spaces."""
    path = str(tmp_path / "a b")
    spark.range(1).repartition(1).write.json(path, mode="overwrite")
    df = spark.read.format("json").load(path)

    files = df.inputFiles()
    assert len(files) == 1
    assert "%20" in files[0]
    assert " " not in files[0]


def test_input_files_ignores_eliminated_scans(spark, tmp_path):
    """A scan removed by optimization (``WHERE false``) contributes no files."""
    spark.range(5).repartition(1).write.json(str(tmp_path), mode="overwrite")
    df = spark.read.format("json").load(str(tmp_path))

    assert df.filter("1 = 0").inputFiles() == []
    assert len(df.inputFiles()) == 1


def test_input_files_excludes_nested_hidden_directories(spark, tmp_path):
    """A hidden directory nested below a visible one is excluded from reads and inputFiles."""
    rows = 3
    visible = tmp_path / "visible"
    visible.mkdir()
    (visible / "good.json").write_text("".join(f'{{"id": {i}}}\n' for i in range(rows)))
    (visible / "_hidden").mkdir()
    (visible / "_hidden" / "bad.json").write_text('{"id": 99}\n')
    df = spark.read.format("json").load(str(tmp_path))

    # The nested hidden file must be excluded from both the scan and inputFiles.
    assert df.count() == rows
    files = df.inputFiles()
    assert len(files) == 1
    assert all("_hidden" not in f for f in files)


def test_input_files_excludes_explicitly_targeted_hidden_file(spark, tmp_path):
    """An explicitly targeted hidden file is ignored, as in Spark."""
    hidden = tmp_path / "_data.json"
    hidden.write_text('{"id": 1}\n')
    df = spark.read.schema("id long").json(str(hidden))

    assert df.count() == 0
    assert df.inputFiles() == []


def test_input_files_honors_path_glob_filter(spark, tmp_path):
    """A listing-format ``pathGlobFilter`` restricts the files that compose the dataset."""
    (tmp_path / "keep.json").write_text('{"id": 1}\n')
    (tmp_path / "drop.json").write_text('{"id": 2}\n')
    df = spark.read.format("json").option("pathGlobFilter", "keep.*").load(str(tmp_path))

    files = df.inputFiles()
    assert len(files) == 1
    assert files[0].endswith("keep.json")


def test_input_files_matches_scan_under_glob_alternation(spark, tmp_path):
    """``inputFiles`` and scans both support ``{a,b}`` alternation."""
    (tmp_path / "a.png").write_bytes(b"\x89PNG")
    (tmp_path / "b.jpg").write_bytes(b"\xff\xd8\xff")
    (tmp_path / "c.pdf").write_bytes(b"%PDF")
    df = spark.read.format("binaryFile").option("pathGlobFilter", "*.{png,jpg}").load(str(tmp_path))

    assert df.count() == len(df.inputFiles())
    assert {file.rsplit("/", 1)[-1] for file in df.inputFiles()} == {"a.png", "b.jpg"}


def test_input_files_rejects_invalid_path_glob_filter(spark, tmp_path):
    """A malformed ``pathGlobFilter`` surfaces as an error rather than being silently ignored."""
    (tmp_path / "a.png").write_bytes(b"\x89PNG")
    df = spark.read.format("binaryFile").option("pathGlobFilter", "[").load(str(tmp_path))

    with pytest.raises(AnalysisException, match="path glob filter"):
        df.inputFiles()
